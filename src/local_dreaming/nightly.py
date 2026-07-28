from __future__ import annotations

import fcntl
import os
import stat
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from local_dreaming.config import ModelSettings, NightlyBudget
from local_dreaming.doctor import run_doctor, verify_certification_stamp
from local_dreaming.pipeline import (
    BudgetTracker,
    BudgetUsage,
    PipelineJob,
    PipelineOutcome,
    PipelinePhase,
    PipelineStatus,
)
from local_dreaming.worker import WorkerRuntime


class NightlyStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    BUDGET_EXHAUSTED = "budget_exhausted"
    CERTIFICATION_REQUIRED = "certification_required"
    ALREADY_RUNNING = "already_running"


@dataclass(frozen=True, slots=True)
class NightlyReport:
    run_id: str | None
    status: NightlyStatus
    lease_owner: str
    leased_jobs: int
    outcomes: tuple[PipelineOutcome, ...]
    usage: BudgetUsage
    error_codes: tuple[str, ...] = ()


class CertificationGate(Protocol):
    def verify(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class DoctorCertificationGate:
    runtime: WorkerRuntime
    models: ModelSettings
    stamp_path: Path

    def verify(self) -> bool:
        offline = run_doctor(self.runtime, models=self.models, live_probe=False)
        return offline.offline_ready and verify_certification_stamp(
            self.stamp_path,
            runtime=self.runtime,
            models=self.models,
        )


class NightlyOperations(Protocol):
    def start_nightly_run(self, *, run_id: str | None = None) -> str: ...

    def add_nightly_usage(
        self,
        run_id: str,
        *,
        scan_bytes: int = 0,
        episode_count: int = 0,
        model_calls: int = 0,
        input_tokens: int = 0,
        output_tokens: int = 0,
    ) -> None: ...

    def finish_nightly_run(
        self,
        run_id: str,
        *,
        status: str,
        error_summary: str | None = None,
    ) -> None: ...

    def lease_job(
        self,
        *,
        owner: str,
        lease_seconds: int = 300,
        job_types: Sequence[str] | None = None,
        job_ids: Sequence[str] | None = None,
    ) -> dict[str, Any] | None: ...

    def fail_job(
        self,
        job_id: str,
        *,
        owner: str,
        error: str,
        retry_at: str | None = None,
    ) -> bool: ...


class NightlyPipeline(Protocol):
    def run_leased_job(
        self,
        job: PipelineJob,
        *,
        lease_owner: str,
        budget: BudgetTracker,
    ) -> PipelineOutcome: ...


class JobLoader(Protocol):
    """Loads bounded input from canonical storage using only opaque job payload references."""

    def load(self, leased_record: Mapping[str, Any]) -> PipelineJob: ...


@contextmanager
def try_file_lock(path: Path) -> Iterator[bool]:
    """Acquire an owner-only non-blocking local lock without following a final symlink."""

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    os.fchmod(descriptor, 0o600)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise OSError("nightly lock target is not a regular file")
    acquired = False
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            acquired = False
        yield acquired
    finally:
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _usage_snapshot(usage: BudgetUsage) -> BudgetUsage:
    return BudgetUsage(
        scan_bytes=usage.scan_bytes,
        episodes=usage.episodes,
        model_calls=usage.model_calls,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
    )


def _usage_delta(before: BudgetUsage, after: BudgetUsage) -> BudgetUsage:
    return BudgetUsage(
        scan_bytes=max(0, after.scan_bytes - before.scan_bytes),
        episodes=max(0, after.episodes - before.episodes),
        model_calls=max(0, after.model_calls - before.model_calls),
        input_tokens=max(0, after.input_tokens - before.input_tokens),
        output_tokens=max(0, after.output_tokens - before.output_tokens),
    )


def _retry_timestamp(now: datetime, minutes: int) -> str:
    return (now.astimezone(UTC) + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")


class NightlyRunner:
    def __init__(
        self,
        *,
        runtime: WorkerRuntime,
        operations: NightlyOperations,
        pipeline: NightlyPipeline,
        loader: JobLoader,
        certification: CertificationGate,
        limits: NightlyBudget,
        lease_seconds: int = 420,
        fail_fast: bool = False,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if lease_seconds < 360:
            raise ValueError("job lease must outlive the maximum default worker call")
        self.runtime = runtime
        self.operations = operations
        self.pipeline = pipeline
        self.loader = loader
        self.certification = certification
        self.limits = limits
        self.lease_seconds = lease_seconds
        self.fail_fast = fail_fast
        self.now = now or (lambda: datetime.now(UTC))

    @property
    def nightly_lock_path(self) -> Path:
        return self.runtime.paths.worker / "nightly.lock"

    @property
    def phase2_lock_path(self) -> Path:
        return self.runtime.paths.worker / "phase2.lock"

    def run(self) -> NightlyReport:
        self.runtime.prepare()
        owner = "nightly-" + uuid.uuid4().hex
        with try_file_lock(self.nightly_lock_path) as acquired:
            if not acquired:
                return NightlyReport(
                    run_id=None,
                    status=NightlyStatus.ALREADY_RUNNING,
                    lease_owner=owner,
                    leased_jobs=0,
                    outcomes=(),
                    usage=BudgetUsage(),
                )
            return self._run_locked(owner)

    def _run_locked(self, owner: str) -> NightlyReport:
        run_id = self.operations.start_nightly_run()
        try:
            certification_ok = self.certification.verify()
            certification_error = "WorkerCertificationRequired"
        except Exception as exc:
            certification_ok = False
            certification_error = type(exc).__name__
        if not certification_ok:
            self.operations.finish_nightly_run(
                run_id,
                status="failed",
                error_summary=certification_error,
            )
            return NightlyReport(
                run_id=run_id,
                status=NightlyStatus.CERTIFICATION_REQUIRED,
                lease_owner=owner,
                leased_jobs=0,
                outcomes=(),
                usage=BudgetUsage(),
                error_codes=(certification_error,),
            )

        budget = BudgetTracker(self.limits)
        outcomes: list[PipelineOutcome] = []
        errors: list[str] = []
        leased_jobs = 0
        terminal = NightlyStatus.COMPLETED
        iteration_limit = max(self.limits.max_model_calls, self.limits.max_episodes, 1) + 10

        try:
            for _ in range(iteration_limit):
                if budget.exhausted():
                    terminal = NightlyStatus.BUDGET_EXHAUSTED
                    break
                leased = self.operations.lease_job(
                    owner=owner,
                    lease_seconds=self.lease_seconds,
                    job_types=(PipelinePhase.PHASE1.value, PipelinePhase.PHASE2.value),
                )
                if leased is None:
                    break
                leased_jobs += 1
                job_id = str(leased.get("job_id", ""))
                try:
                    job = self.loader.load(leased)
                    if job.job_id != job_id:
                        raise ValueError("loaded pipeline job ID does not match lease")
                except Exception as exc:
                    error_code = type(exc).__name__
                    errors.append(error_code)
                    self.operations.fail_job(
                        job_id,
                        owner=owner,
                        error=error_code,
                        retry_at=_retry_timestamp(self.now(), 60),
                    )
                    if self.fail_fast:
                        terminal = NightlyStatus.FAILED
                        break
                    continue

                before = _usage_snapshot(budget.usage)
                try:
                    if job.phase is PipelinePhase.PHASE2:
                        with try_file_lock(self.phase2_lock_path) as phase2_acquired:
                            if not phase2_acquired:
                                error_code = "Phase2LockBusy"
                                errors.append(error_code)
                                self.operations.fail_job(
                                    job.job_id,
                                    owner=owner,
                                    error=error_code,
                                    retry_at=_retry_timestamp(self.now(), 5),
                                )
                                continue
                            outcome = self.pipeline.run_leased_job(
                                job, lease_owner=owner, budget=budget
                            )
                    else:
                        outcome = self.pipeline.run_leased_job(
                            job, lease_owner=owner, budget=budget
                        )
                except Exception as exc:
                    error_code = type(exc).__name__
                    errors.append(error_code)
                    self.operations.fail_job(
                        job.job_id,
                        owner=owner,
                        error=error_code,
                        retry_at=_retry_timestamp(self.now(), 60),
                    )
                    continue
                after = _usage_snapshot(budget.usage)
                delta = _usage_delta(before, after)
                if any(
                    (
                        delta.scan_bytes,
                        delta.episodes,
                        delta.model_calls,
                        delta.input_tokens,
                        delta.output_tokens,
                    )
                ):
                    self.operations.add_nightly_usage(
                        run_id,
                        scan_bytes=delta.scan_bytes,
                        episode_count=delta.episodes,
                        model_calls=delta.model_calls,
                        input_tokens=delta.input_tokens,
                        output_tokens=delta.output_tokens,
                    )
                outcomes.append(outcome)
                if outcome.error_code:
                    errors.append(outcome.error_code)
                if self.fail_fast and outcome.status in {
                    PipelineStatus.FAILED,
                    PipelineStatus.QUARANTINED,
                }:
                    terminal = NightlyStatus.FAILED
                    break
                if outcome.status is PipelineStatus.BUDGET_EXHAUSTED:
                    terminal = NightlyStatus.BUDGET_EXHAUSTED
                    break
            else:
                terminal = NightlyStatus.FAILED
                errors.append("NightlyIterationLimit")

            if terminal is NightlyStatus.COMPLETED and (
                errors
                or any(
                    outcome.status in {PipelineStatus.FAILED, PipelineStatus.QUARANTINED}
                    for outcome in outcomes
                )
            ):
                terminal = NightlyStatus.FAILED
        except Exception as exc:
            terminal = NightlyStatus.FAILED
            errors.append(type(exc).__name__)

        database_status = (
            "budget_exhausted"
            if terminal is NightlyStatus.BUDGET_EXHAUSTED
            else "failed"
            if terminal is NightlyStatus.FAILED
            else "completed"
        )
        safe_errors = tuple(dict.fromkeys(errors))
        self.operations.finish_nightly_run(
            run_id,
            status=database_status,
            error_summary=",".join(safe_errors) or None,
        )
        return NightlyReport(
            run_id=run_id,
            status=terminal,
            lease_owner=owner,
            leased_jobs=leased_jobs,
            outcomes=tuple(outcomes),
            usage=_usage_snapshot(budget.usage),
            error_codes=safe_errors,
        )

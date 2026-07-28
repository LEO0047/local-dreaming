from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from local_dreaming.config import NightlyBudget, RuntimePaths
from local_dreaming.models import Sensitivity, SourceKind, SourcePolicy
from local_dreaming.nightly import NightlyRunner, NightlyStatus, try_file_lock
from local_dreaming.pipeline import (
    BudgetTracker,
    PipelineJob,
    PipelineOutcome,
    PipelinePhase,
    PipelineStatus,
)
from local_dreaming.worker import WorkerRuntime, WorkerUsage


class _Certification:
    def __init__(self, allowed: bool) -> None:
        self.allowed = allowed
        self.calls = 0

    def verify(self) -> bool:
        self.calls += 1
        return self.allowed


class _Operations:
    def __init__(self, jobs: list[dict[str, Any]]) -> None:
        self.jobs = jobs
        self.started: list[str] = []
        self.finished: list[tuple[str, str, str | None]] = []
        self.usage: list[dict[str, int]] = []
        self.failed: list[tuple[str, str, str | None]] = []
        self.completed: list[str] = []

    def start_nightly_run(self, *, run_id: str | None = None) -> str:
        stable_id = run_id or f"run-{len(self.started) + 1}"
        self.started.append(stable_id)
        return stable_id

    def add_nightly_usage(self, run_id: str, **kwargs: int) -> None:
        assert run_id in self.started
        self.usage.append(kwargs)

    def finish_nightly_run(
        self,
        run_id: str,
        *,
        status: str,
        error_summary: str | None = None,
    ) -> None:
        self.finished.append((run_id, status, error_summary))

    def lease_job(
        self,
        *,
        owner: str,
        lease_seconds: int = 300,
        job_types: tuple[str, ...] | None = None,
    ) -> dict[str, Any] | None:
        assert owner.startswith("nightly-")
        assert lease_seconds >= 360
        assert job_types == ("phase1", "phase2")
        return self.jobs.pop(0) if self.jobs else None

    def fail_job(
        self,
        job_id: str,
        *,
        owner: str,
        error: str,
        retry_at: str | None = None,
    ) -> bool:
        del owner
        self.failed.append((job_id, error, retry_at))
        return True

    def complete_job(self, job_id: str) -> None:
        self.completed.append(job_id)


class _Loader:
    def load(self, leased_record: dict[str, Any]) -> PipelineJob:
        job = leased_record["job"]
        if isinstance(job, Exception):
            raise job
        assert isinstance(job, PipelineJob)
        return job


class _Pipeline:
    def __init__(self, operations: _Operations) -> None:
        self.operations = operations
        self.jobs: list[PipelineJob] = []

    def run_leased_job(
        self,
        job: PipelineJob,
        *,
        lease_owner: str,
        budget: BudgetTracker,
    ) -> PipelineOutcome:
        del lease_owner
        self.jobs.append(job)
        budget.reserve(job)
        usage = WorkerUsage(
            input_tokens=job.estimated_input_tokens,
            cached_input_tokens=0,
            output_tokens=job.estimated_output_tokens,
            reasoning_output_tokens=0,
        )
        budget.settle(job, usage)
        self.operations.complete_job(job.job_id)
        return PipelineOutcome(
            job_id=job.job_id,
            status=PipelineStatus.COMPLETED,
            persisted_count=1,
            usage=usage,
        )


def _limits(**overrides: int) -> NightlyBudget:
    defaults = {
        "max_scan_bytes": 10_000,
        "max_episodes": 10,
        "max_model_calls": 10,
        "max_input_tokens": 10_000,
        "max_output_tokens": 10_000,
        "max_wall_seconds": 600,
    }
    defaults.update(overrides)
    return NightlyBudget(**defaults)


def _job(job_id: str, phase: PipelinePhase = PipelinePhase.PHASE1) -> PipelineJob:
    policy = SourcePolicy(
        source_id="source-1",
        source_kind=SourceKind.CODEX_TASK,
        opted_in=True,
        allow_model_egress=True,
    )
    return PipelineJob(
        job_id=job_id,
        phase=phase,
        model_input="bounded synthetic input",
        source_ids=("source-1",),
        source_policies={"source-1": policy},
        source_policy_version="v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=("ev-1",),
        scan_bytes=100,
        episode_count=1 if phase is PipelinePhase.PHASE1 else 0,
        estimated_input_tokens=20,
        estimated_output_tokens=10,
    )


def _runner(
    runtime_home: Path,
    operations: _Operations,
    certification: _Certification,
    *,
    limits: NightlyBudget | None = None,
    fail_fast: bool = False,
) -> tuple[NightlyRunner, _Pipeline]:
    runtime = WorkerRuntime(
        RuntimePaths(home=runtime_home),
        codex_binary=Path("/usr/bin/true"),
        base_environment={"HOME": "/Users/test", "PATH": "/usr/bin:/bin"},
    )
    pipeline = _Pipeline(operations)
    runner = NightlyRunner(
        runtime=runtime,
        operations=operations,
        pipeline=pipeline,
        loader=_Loader(),
        certification=certification,
        limits=limits or _limits(),
        fail_fast=fail_fast,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )
    return runner, pipeline


def test_nightly_fails_closed_before_leasing_without_certification(runtime_home: Path) -> None:
    operations = _Operations([{"job_id": "job-1", "job": _job("job-1")}])
    certification = _Certification(False)
    runner, pipeline = _runner(runtime_home, operations, certification)

    report = runner.run()

    assert report.status is NightlyStatus.CERTIFICATION_REQUIRED
    assert not pipeline.jobs
    assert len(operations.jobs) == 1
    assert operations.finished == [("run-1", "failed", "WorkerCertificationRequired")]


def test_fail_fast_stops_after_first_loader_error(runtime_home: Path) -> None:
    operations = _Operations(
        [
            {"job_id": "bad-job", "job": ValueError("unsafe detail")},
            {"job_id": "later-job", "job": _job("later-job")},
        ]
    )
    runner, pipeline = _runner(
        runtime_home,
        operations,
        _Certification(True),
        fail_fast=True,
    )

    report = runner.run()

    assert report.status is NightlyStatus.FAILED
    assert not pipeline.jobs
    assert len(operations.jobs) == 1
    assert operations.failed[0][1] == "ValueError"


def test_nightly_processes_phase1_then_phase2_and_records_bounded_usage(
    runtime_home: Path,
) -> None:
    operations = _Operations(
        [
            {"job_id": "job-1", "job": _job("job-1")},
            {"job_id": "job-2", "job": _job("job-2", PipelinePhase.PHASE2)},
        ]
    )
    runner, pipeline = _runner(runtime_home, operations, _Certification(True))

    report = runner.run()

    assert report.status is NightlyStatus.COMPLETED
    assert [job.job_id for job in pipeline.jobs] == ["job-1", "job-2"]
    assert operations.completed == ["job-1", "job-2"]
    assert report.usage.model_calls == 2
    assert report.usage.episodes == 1
    assert report.usage.scan_bytes == 200
    assert operations.finished == [("run-1", "completed", None)]


def test_nightly_stops_at_model_call_budget_before_leasing_more(runtime_home: Path) -> None:
    operations = _Operations(
        [
            {"job_id": "job-1", "job": _job("job-1")},
            {"job_id": "job-2", "job": _job("job-2")},
        ]
    )
    runner, pipeline = _runner(
        runtime_home,
        operations,
        _Certification(True),
        limits=_limits(max_model_calls=1),
    )

    report = runner.run()

    assert report.status is NightlyStatus.BUDGET_EXHAUSTED
    assert [job.job_id for job in pipeline.jobs] == ["job-1"]
    assert len(operations.jobs) == 1
    assert operations.finished == [("run-1", "budget_exhausted", None)]


def test_phase2_global_lock_defers_job_without_calling_pipeline(runtime_home: Path) -> None:
    operations = _Operations([{"job_id": "job-2", "job": _job("job-2", PipelinePhase.PHASE2)}])
    runner, pipeline = _runner(runtime_home, operations, _Certification(True))
    runtime_home.mkdir(parents=True, exist_ok=True)
    runner.runtime.prepare()

    with try_file_lock(runner.phase2_lock_path) as held:
        assert held
        report = runner.run()

    assert report.status is NightlyStatus.FAILED
    assert not pipeline.jobs
    assert operations.failed[0][1] == "Phase2LockBusy"
    assert operations.failed[0][2] is not None


def test_global_nightly_lock_prevents_duplicate_run(runtime_home: Path) -> None:
    operations = _Operations([])
    runner, _ = _runner(runtime_home, operations, _Certification(True))
    runner.runtime.prepare()

    with try_file_lock(runner.nightly_lock_path) as held:
        assert held
        report = runner.run()

    assert report.status is NightlyStatus.ALREADY_RUNNING
    assert report.run_id is None
    assert not operations.started


def test_loader_failure_is_sanitized_and_retried_later(runtime_home: Path) -> None:
    operations = _Operations(
        [{"job_id": "job-bad", "job": ValueError("private raw content must not be logged")}]
    )
    runner, _ = _runner(runtime_home, operations, _Certification(True))

    report = runner.run()

    assert report.status is NightlyStatus.FAILED
    assert report.error_codes == ("ValueError",)
    assert operations.failed[0][1] == "ValueError"
    assert "private raw content" not in str(report)
    assert operations.finished == [("run-1", "failed", "ValueError")]

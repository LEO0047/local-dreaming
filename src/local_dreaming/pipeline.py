from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from local_dreaming.config import ModelSettings, NightlyBudget
from local_dreaming.errors import OversizedInputError, PrivacyBoundaryError, WorkerProtocolError
from local_dreaming.evidence import MAX_MODEL_INPUT_CHARS
from local_dreaming.models import ClaimScope, Sensitivity, SourcePolicy
from local_dreaming.redaction import has_unredacted_secret
from local_dreaming.worker import CodexWorker, WorkerRequest, WorkerResult, WorkerUsage, sha256_file

PHASE1_SCHEMA_VERSION = "phase1-v1"
PHASE2_SCHEMA_VERSION = "phase2-v2"
PROMPT_VERSION = "dreaming-v2.3"


class PipelinePhase(StrEnum):
    PHASE1 = "phase1"
    PHASE2 = "phase2"


class PipelineStatus(StrEnum):
    COMPLETED = "completed"
    NO_OP = "no_op"
    QUARANTINED = "quarantined"
    BUDGET_EXHAUSTED = "budget_exhausted"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class PipelineModels:
    phase1_model: str
    phase1_reasoning: str
    phase2_model: str
    phase2_reasoning: str

    @classmethod
    def from_settings(cls, settings: ModelSettings) -> PipelineModels:
        return cls(
            phase1_model=settings.phase1_model,
            phase1_reasoning=settings.phase1_reasoning,
            phase2_model=settings.phase2_model,
            phase2_reasoning=settings.phase2_reasoning,
        )

    def select(self, phase: PipelinePhase) -> tuple[str, str]:
        if phase is PipelinePhase.PHASE1:
            return self.phase1_model, self.phase1_reasoning
        return self.phase2_model, self.phase2_reasoning


@dataclass(frozen=True, slots=True)
class PipelineJob:
    job_id: str
    phase: PipelinePhase
    model_input: str
    source_ids: tuple[str, ...]
    source_policies: Mapping[str, SourcePolicy]
    source_policy_version: str
    maximum_sensitivity: Sensitivity
    allowed_reference_ids: tuple[str, ...]
    allowed_head_set_hashes: tuple[str, ...] = ()
    allowed_claim_ids: tuple[str, ...] = ()
    reference_source_ids: Mapping[str, str] | None = None
    scan_bytes: int = 0
    episode_count: int = 0
    estimated_input_tokens: int = 0
    estimated_output_tokens: int = 2_000
    redaction_count: int = 0
    attempt: int = 1
    require_egress_approval: bool = False
    persistence_context: Mapping[str, Any] | None = None
    approval: EgressApproval | None = None

    def __post_init__(self) -> None:
        if not self.job_id or not self.source_ids:
            raise ValueError("pipeline job requires job_id and source_ids")
        if any(value < 0 for value in self.resource_estimate):
            raise ValueError("pipeline resource estimates cannot be negative")
        if self.attempt < 1:
            raise ValueError("pipeline job attempt must be positive")

    @property
    def resource_estimate(self) -> tuple[int, int, int, int]:
        return (
            self.scan_bytes,
            self.episode_count,
            self.estimated_input_tokens,
            self.estimated_output_tokens,
        )


@dataclass(frozen=True, slots=True)
class InvocationPlan:
    phase: PipelinePhase
    model_id: str
    reasoning_effort: str
    prompt: str
    prompt_hash: str
    schema_path: Path
    schema_hash: str
    schema_version: str
    payload_digest: str


@dataclass(frozen=True, slots=True)
class EgressApproval:
    token_id: str
    exact_payload_digest: str
    source_policy_version: str
    prompt_hash: str
    schema_hash: str
    model_id: str
    reasoning_effort: str
    expires_at: datetime

    def __post_init__(self) -> None:
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("egress approval expiry must be timezone-aware")

    def binding_hash(self) -> str:
        payload = {
            "exact_payload_digest": self.exact_payload_digest,
            "expires_at": self.expires_at.astimezone(UTC).isoformat(),
            "model_id": self.model_id,
            "prompt_hash": self.prompt_hash,
            "reasoning_effort": self.reasoning_effort,
            "schema_hash": self.schema_hash,
            "source_policy_version": self.source_policy_version,
            "token_id": self.token_id,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


class ApprovalLedger(Protocol):
    def consume(self, token_id: str, binding_hash: str) -> bool: ...


class InMemoryApprovalLedger:
    """Single-process ledger used by CLI preview/apply; restart invalidates every token."""

    def __init__(self) -> None:
        self._approved: dict[str, str] = {}
        self._consumed: set[str] = set()
        self._lock = threading.Lock()

    def register(self, approval: EgressApproval) -> None:
        with self._lock:
            if approval.token_id in self._approved or approval.token_id in self._consumed:
                raise ValueError("egress approval token ID already exists")
            self._approved[approval.token_id] = approval.binding_hash()

    def consume(self, token_id: str, binding_hash: str) -> bool:
        with self._lock:
            expected = self._approved.get(token_id)
            if expected != binding_hash or token_id in self._consumed:
                return False
            self._consumed.add(token_id)
            del self._approved[token_id]
            return True


@dataclass(slots=True)
class BudgetUsage:
    scan_bytes: int = 0
    episodes: int = 0
    model_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0


class BudgetExceeded(RuntimeError):
    pass


class BudgetTracker:
    def __init__(
        self,
        limits: NightlyBudget,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        numeric_limits = (
            limits.max_scan_bytes,
            limits.max_episodes,
            limits.max_model_calls,
            limits.max_input_tokens,
            limits.max_output_tokens,
        )
        if any(value < 0 for value in numeric_limits) or limits.max_wall_seconds <= 0:
            raise ValueError("nightly budget limits must be non-negative with positive wall time")
        self.limits = limits
        self.usage = BudgetUsage()
        self._clock = clock
        self._started = clock()

    @property
    def elapsed_seconds(self) -> float:
        return max(0.0, self._clock() - self._started)

    @property
    def remaining_wall_seconds(self) -> float:
        return max(0.0, self.limits.max_wall_seconds - self.elapsed_seconds)

    def _violation(self, job: PipelineJob) -> str | None:
        projected = (
            self.usage.scan_bytes + job.scan_bytes,
            self.usage.episodes + job.episode_count,
            self.usage.model_calls + 1,
            self.usage.input_tokens + job.estimated_input_tokens,
            self.usage.output_tokens + job.estimated_output_tokens,
        )
        limits = (
            self.limits.max_scan_bytes,
            self.limits.max_episodes,
            self.limits.max_model_calls,
            self.limits.max_input_tokens,
            self.limits.max_output_tokens,
        )
        names = ("scan_bytes", "episodes", "model_calls", "input_tokens", "output_tokens")
        for name, value, limit in zip(names, projected, limits, strict=True):
            if value > limit:
                return name
        if self.elapsed_seconds >= self.limits.max_wall_seconds:
            return "wall_seconds"
        return None

    def ensure_available(self, job: PipelineJob) -> None:
        if violation := self._violation(job):
            raise BudgetExceeded(f"nightly budget exhausted: {violation}")

    def reserve(self, job: PipelineJob) -> None:
        self.ensure_available(job)
        self.usage.scan_bytes += job.scan_bytes
        self.usage.episodes += job.episode_count
        self.usage.model_calls += 1
        self.usage.input_tokens += job.estimated_input_tokens
        self.usage.output_tokens += job.estimated_output_tokens

    def settle(self, job: PipelineJob, usage: WorkerUsage) -> None:
        self.usage.input_tokens += usage.input_tokens - job.estimated_input_tokens
        self.usage.output_tokens += usage.total_output_tokens - job.estimated_output_tokens

    def exhausted(self) -> bool:
        # Scan/episode capacity is job-specific: a Phase 2 job may still fit after Phase 1 reaches
        # its episode cap. The dimensions below are consumed by every real model call.
        reached = (
            self.usage.model_calls >= self.limits.max_model_calls,
            self.usage.input_tokens >= self.limits.max_input_tokens,
            self.usage.output_tokens >= self.limits.max_output_tokens,
            self.elapsed_seconds >= self.limits.max_wall_seconds,
        )
        return any(reached)


class OperationsTelemetry(Protocol):
    def create_model_call(
        self,
        *,
        phase: str,
        source_ids: Sequence[str],
        maximum_sensitivity: str,
        input_bytes: int,
        redaction_count: int,
        model_id: str,
        reasoning_effort: str,
        prompt_hash: str,
        schema_version: str,
        call_id: str | None = None,
    ) -> str: ...

    def finish_model_call(
        self,
        call_id: str,
        *,
        status: str,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        error_summary: str | None = None,
    ) -> None: ...

    def complete_job(self, job_id: str, *, owner: str) -> bool: ...

    def fail_job(
        self,
        job_id: str,
        *,
        owner: str,
        error: str,
        retry_at: str | None = None,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class ModelProvenance:
    call_id: str
    model_id: str
    reasoning_effort: str
    prompt_hash: str
    schema_hash: str
    schema_version: str
    prompt_version: str = PROMPT_VERSION


class PhaseOutputSink(Protocol):
    def persist_candidates(
        self,
        job: PipelineJob,
        payload: Mapping[str, Any],
        provenance: ModelProvenance,
    ) -> int: ...

    def persist_proposals(
        self,
        job: PipelineJob,
        payload: Mapping[str, Any],
        provenance: ModelProvenance,
    ) -> int: ...


class WorkerAdapter(Protocol):
    def run(self, request: WorkerRequest) -> WorkerResult: ...


@dataclass(frozen=True, slots=True)
class PipelineOutcome:
    job_id: str
    status: PipelineStatus
    persisted_count: int
    usage: WorkerUsage | None = None
    error_code: str | None = None


def prepare_invocation(job: PipelineJob, models: PipelineModels) -> InvocationPlan:
    package_root = Path(__file__).parent
    model_id, reasoning_effort = models.select(job.phase)
    schema_version = (
        PHASE1_SCHEMA_VERSION if job.phase is PipelinePhase.PHASE1 else PHASE2_SCHEMA_VERSION
    )
    schema_path = package_root / "schemas" / f"{job.phase.value}.schema.json"
    prompt_template = (package_root / "prompts" / f"{job.phase.value}.txt").read_text()
    input_envelope = json.dumps(
        {"untrusted_input": job.model_input},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    prompt = (
        f"{prompt_template.rstrip()}\n\n"
        f"PROMPT_VERSION: {PROMPT_VERSION}\n"
        "UNTRUSTED_INPUT_JSON:\n"
        f"{input_envelope}\n"
    )
    return InvocationPlan(
        phase=job.phase,
        model_id=model_id,
        reasoning_effort=reasoning_effort,
        prompt=prompt,
        prompt_hash=hashlib.sha256(prompt.encode()).hexdigest(),
        schema_path=schema_path,
        schema_hash=sha256_file(schema_path),
        schema_version=schema_version,
        payload_digest=hashlib.sha256(job.model_input.encode()).hexdigest(),
    )


def approval_for_plan(
    plan: InvocationPlan,
    *,
    token_id: str,
    source_policy_version: str,
    expires_at: datetime,
) -> EgressApproval:
    return EgressApproval(
        token_id=token_id,
        exact_payload_digest=plan.payload_digest,
        source_policy_version=source_policy_version,
        prompt_hash=plan.prompt_hash,
        schema_hash=plan.schema_hash,
        model_id=plan.model_id,
        reasoning_effort=plan.reasoning_effort,
        expires_at=expires_at,
    )


def _validate_sources(job: PipelineJob) -> None:
    if has_unredacted_secret(job.model_input):
        raise PrivacyBoundaryError("model input contains an unredacted secret")
    if job.maximum_sensitivity is Sensitivity.SECRET:
        raise PrivacyBoundaryError("secret input can never leave the deterministic local layer")
    sensitivity_rank = {
        Sensitivity.NORMAL: 0,
        Sensitivity.PRIVATE: 1,
        Sensitivity.SECRET: 2,
    }
    for source_id in job.source_ids:
        policy = job.source_policies.get(source_id)
        if policy is None or policy.source_id != source_id:
            raise PrivacyBoundaryError("model input source has no matching source policy")
        if sensitivity_rank[job.maximum_sensitivity] < sensitivity_rank[policy.sensitivity]:
            raise PrivacyBoundaryError("model input understates its source sensitivity")
        if not policy.permits_model_egress(job.maximum_sensitivity):
            raise PrivacyBoundaryError("source policy denies model egress")


def _authorize_private(
    job: PipelineJob,
    plan: InvocationPlan,
    ledger: ApprovalLedger | None,
    now: datetime,
) -> None:
    if job.maximum_sensitivity is not Sensitivity.PRIVATE or not job.require_egress_approval:
        return
    approval = job.approval
    if approval is None or ledger is None:
        raise PrivacyBoundaryError("private egress requires an exact single-use approval")
    expected = (
        approval.exact_payload_digest == plan.payload_digest
        and approval.source_policy_version == job.source_policy_version
        and approval.prompt_hash == plan.prompt_hash
        and approval.schema_hash == plan.schema_hash
        and approval.model_id == plan.model_id
        and approval.reasoning_effort == plan.reasoning_effort
        and now < approval.expires_at.astimezone(UTC)
    )
    if not expected or not ledger.consume(approval.token_id, approval.binding_hash()):
        raise PrivacyBoundaryError("private egress approval is stale, mismatched, or already used")


def _validate_references(job: PipelineJob, payload: Mapping[str, Any]) -> None:
    allowed = set(job.allowed_reference_ids)
    allowed_claims = set(job.allowed_claim_ids)
    if job.phase is PipelinePhase.PHASE1:
        candidates = payload.get("candidate_claims", ())
        if not isinstance(candidates, list):
            raise WorkerProtocolError("candidate_claims must be an array")
        if bool(payload.get("no_op")) == bool(candidates):
            raise WorkerProtocolError("phase1 no_op flag contradicts candidate_claims")
        collections: list[list[Any]] = []
        source_by_reference = dict(job.reference_source_ids or {})
        if len(job.source_ids) == 1:
            source_by_reference.update(
                {
                    reference_id: job.source_ids[0]
                    for reference_id in job.allowed_reference_ids
                    if reference_id not in source_by_reference
                }
            )
        for candidate in candidates:
            if not isinstance(candidate, dict):
                raise WorkerProtocolError("candidate_claims entries must be objects")
            try:
                scope = ClaimScope(str(candidate.get("scope")))
            except ValueError as exc:
                raise WorkerProtocolError("candidate uses an unsupported claim scope") from exc
            references = candidate.get("evidence_event_ids", ())
            if not isinstance(references, list):
                raise WorkerProtocolError("candidate evidence references must be an array")
            if not all(isinstance(reference, str) for reference in references):
                raise WorkerProtocolError("candidate evidence references must be strings")
            collections.append(references)
            for reference_id in references:
                source_id = source_by_reference.get(str(reference_id))
                policy = job.source_policies.get(source_id or "")
                if policy is None:
                    raise WorkerProtocolError("candidate evidence has no bounded source mapping")
                if not policy.permits_claim_scope(scope):
                    raise WorkerProtocolError("source trust policy denies the proposed claim scope")
            possible_supersedes = candidate.get("possible_supersedes_claim_id")
            if possible_supersedes is not None and possible_supersedes not in allowed_claims:
                raise WorkerProtocolError("candidate references an unbounded canonical claim")
    else:
        proposals = payload.get("proposals", ())
        if not isinstance(proposals, list):
            raise WorkerProtocolError("proposals must be an array")
        if bool(payload.get("no_op")) == bool(proposals):
            raise WorkerProtocolError("phase2 no_op flag contradicts proposals")
        collections = []
        proposed_references: set[str] = set()
        allowed_heads = set(job.allowed_head_set_hashes)
        for proposal in proposals:
            if not isinstance(proposal, dict):
                raise WorkerProtocolError("proposal entries must be objects")
            references = proposal.get("candidate_ids", ())
            if not isinstance(references, list):
                raise WorkerProtocolError("proposal candidate references must be an array")
            if not all(isinstance(reference, str) for reference in references):
                raise WorkerProtocolError("proposal candidate references must be strings")
            collections.append(references)
            proposed_references.update(references)
            expected_head = proposal.get("expected_head_set_hash")
            if expected_head not in allowed_heads:
                raise WorkerProtocolError("model output references an unbounded claim head set")
            claim_id = proposal.get("claim_id")
            if claim_id is not None and claim_id not in allowed_claims:
                raise WorkerProtocolError("proposal references an unbounded canonical claim")
        dispositions = payload.get("dispositions", [])
        if not isinstance(dispositions, list):
            raise WorkerProtocolError("dispositions must be an array")
        disposition_references: set[str] = set()
        for disposition in dispositions:
            if not isinstance(disposition, dict):
                raise WorkerProtocolError("disposition entries must be objects")
            candidate_id = disposition.get("candidate_id")
            if not isinstance(candidate_id, str):
                raise WorkerProtocolError("disposition candidate_id must be a string")
            if candidate_id in disposition_references:
                raise WorkerProtocolError("candidate has duplicate dispositions")
            disposition_references.add(candidate_id)
        if proposed_references & disposition_references:
            raise WorkerProtocolError("candidate cannot be both proposed and dispositioned")
        if dispositions and proposed_references | disposition_references != allowed:
            raise WorkerProtocolError("phase2 output must account for every bounded candidate")
    for references in collections:
        if not isinstance(references, list) or not set(references).issubset(allowed):
            raise WorkerProtocolError("model output references evidence outside the bounded input")


def _retry_at(now: datetime, attempt: int, *, quarantined: bool = False) -> str:
    if quarantined:
        delay = timedelta(hours=24)
    else:
        delay = timedelta(minutes=min(2 ** min(attempt, 8), 360))
    return (now.astimezone(UTC) + delay).isoformat().replace("+00:00", "Z")


class DreamingPipeline:
    def __init__(
        self,
        *,
        worker: WorkerAdapter | CodexWorker,
        operations: OperationsTelemetry,
        output_sink: PhaseOutputSink,
        models: PipelineModels,
        approval_ledger: ApprovalLedger | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.worker = worker
        self.operations = operations
        self.output_sink = output_sink
        self.models = models
        self.approval_ledger = approval_ledger
        self.now = now

    def run_leased_job(
        self,
        job: PipelineJob,
        *,
        lease_owner: str,
        budget: BudgetTracker,
    ) -> PipelineOutcome:
        # The database loader is the normal size gate, but the pipeline is the final egress
        # boundary.  Re-check here so alternate loaders, migrated queued jobs, and direct callers
        # cannot bypass the 12,000-character contract.  This happens before invocation planning,
        # call telemetry, budget reservation, or the worker sees any input.
        if len(job.model_input) > MAX_MODEL_INPUT_CHARS:
            self.operations.fail_job(
                job.job_id,
                owner=lease_owner,
                error=OversizedInputError.__name__,
                retry_at=_retry_at(self.now(), job.attempt, quarantined=True),
            )
            return PipelineOutcome(
                job_id=job.job_id,
                status=PipelineStatus.QUARANTINED,
                persisted_count=0,
                error_code=OversizedInputError.__name__,
            )
        plan = prepare_invocation(job, self.models)
        call_id: str | None = None
        call_finished = False
        try:
            _validate_sources(job)
            budget.ensure_available(job)
            _authorize_private(job, plan, self.approval_ledger, self.now())
            budget.reserve(job)
            call_id = self.operations.create_model_call(
                phase=job.phase.value,
                source_ids=job.source_ids,
                maximum_sensitivity=job.maximum_sensitivity.value,
                # input_bytes is the actual UTF-8 stdin envelope sent to Codex, not only the
                # untrusted loader payload nested inside that envelope.
                input_bytes=len(plan.prompt.encode("utf-8")),
                redaction_count=job.redaction_count,
                model_id=plan.model_id,
                reasoning_effort=plan.reasoning_effort,
                prompt_hash=plan.prompt_hash,
                schema_version=plan.schema_version,
            )
            result = self.worker.run(
                WorkerRequest(
                    phase=job.phase.value,
                    model_id=plan.model_id,
                    reasoning_effort=plan.reasoning_effort,
                    prompt=plan.prompt,
                    schema_path=plan.schema_path,
                    timeout_seconds=min(300.0, max(0.1, budget.remaining_wall_seconds)),
                    job_id=job.job_id,
                    call_id=call_id,
                )
            )
            if not isinstance(result.payload, dict):
                raise WorkerProtocolError("worker payload must be an object")
            _validate_references(job, result.payload)
            budget.settle(job, result.usage)
            provenance = ModelProvenance(
                call_id=call_id,
                model_id=plan.model_id,
                reasoning_effort=plan.reasoning_effort,
                prompt_hash=plan.prompt_hash,
                schema_hash=plan.schema_hash,
                schema_version=plan.schema_version,
            )
            no_op = bool(result.payload.get("no_op"))
            if no_op:
                if job.phase is PipelinePhase.PHASE2:
                    self.output_sink.persist_proposals(
                        job,
                        result.payload,
                        provenance,
                    )
                persisted_count = 0
            elif job.phase is PipelinePhase.PHASE1:
                persisted_count = self.output_sink.persist_candidates(
                    job, result.payload, provenance
                )
            else:
                persisted_count = self.output_sink.persist_proposals(
                    job, result.payload, provenance
                )
            self.operations.finish_model_call(
                call_id,
                status="completed",
                input_tokens=result.usage.input_tokens,
                output_tokens=result.usage.total_output_tokens,
            )
            call_finished = True
            if not self.operations.complete_job(job.job_id, owner=lease_owner):
                raise RuntimeError("job lease was lost before completion")
            return PipelineOutcome(
                job_id=job.job_id,
                status=PipelineStatus.NO_OP if no_op else PipelineStatus.COMPLETED,
                persisted_count=persisted_count,
                usage=result.usage,
            )
        except PrivacyBoundaryError as exc:
            if call_id is None:
                call_id = self.operations.create_model_call(
                    phase=job.phase.value,
                    source_ids=job.source_ids,
                    maximum_sensitivity=job.maximum_sensitivity.value,
                    input_bytes=len(plan.prompt.encode("utf-8")),
                    redaction_count=job.redaction_count,
                    model_id=plan.model_id,
                    reasoning_effort=plan.reasoning_effort,
                    prompt_hash=plan.prompt_hash,
                    schema_version=plan.schema_version,
                )
            self.operations.finish_model_call(
                call_id,
                status="quarantined",
                error_summary=type(exc).__name__,
            )
            self.operations.fail_job(
                job.job_id,
                owner=lease_owner,
                error=type(exc).__name__,
                retry_at=_retry_at(self.now(), job.attempt, quarantined=True),
            )
            return PipelineOutcome(
                job_id=job.job_id,
                status=PipelineStatus.QUARANTINED,
                persisted_count=0,
                error_code=type(exc).__name__,
            )
        except BudgetExceeded as exc:
            self.operations.fail_job(
                job.job_id,
                owner=lease_owner,
                error=type(exc).__name__,
                retry_at=_retry_at(self.now(), job.attempt),
            )
            return PipelineOutcome(
                job_id=job.job_id,
                status=PipelineStatus.BUDGET_EXHAUSTED,
                persisted_count=0,
                error_code=type(exc).__name__,
            )
        except Exception as exc:
            if call_id is not None and not call_finished:
                self.operations.finish_model_call(
                    call_id,
                    status="quarantined" if isinstance(exc, WorkerProtocolError) else "failed",
                    error_summary=type(exc).__name__,
                )
            # A failed model attempt remains charged against the bounded nightly call budget.
            self.operations.fail_job(
                job.job_id,
                owner=lease_owner,
                error=type(exc).__name__,
                retry_at=_retry_at(
                    self.now(), job.attempt, quarantined=isinstance(exc, WorkerProtocolError)
                ),
            )
            return PipelineOutcome(
                job_id=job.job_id,
                status=(
                    PipelineStatus.QUARANTINED
                    if isinstance(exc, WorkerProtocolError)
                    else PipelineStatus.FAILED
                ),
                persisted_count=0,
                error_code=type(exc).__name__,
            )

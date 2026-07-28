from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from local_dreaming.config import ModelSettings, NightlyBudget
from local_dreaming.models import Sensitivity, SourceKind, SourcePolicy
from local_dreaming.pipeline import (
    BudgetTracker,
    DreamingPipeline,
    InMemoryApprovalLedger,
    ModelProvenance,
    PipelineJob,
    PipelineModels,
    PipelinePhase,
    PipelineStatus,
    approval_for_plan,
    prepare_invocation,
)
from local_dreaming.worker import ModelCallRecord, WorkerRequest, WorkerResult, WorkerUsage


class _Operations:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.finished: list[tuple[str, dict[str, Any]]] = []
        self.completed: list[tuple[str, str]] = []
        self.failed: list[tuple[str, str, str]] = []

    def create_model_call(self, **kwargs: Any) -> str:
        self.created.append(kwargs)
        return f"call-{len(self.created)}"

    def finish_model_call(self, call_id: str, **kwargs: Any) -> None:
        self.finished.append((call_id, kwargs))

    def complete_job(self, job_id: str, *, owner: str) -> bool:
        self.completed.append((job_id, owner))
        return True

    def fail_job(
        self,
        job_id: str,
        *,
        owner: str,
        error: str,
        retry_at: str | None = None,
    ) -> bool:
        del retry_at
        self.failed.append((job_id, owner, error))
        return True


class _Sink:
    def __init__(self) -> None:
        self.candidates: list[tuple[PipelineJob, dict[str, Any], ModelProvenance]] = []
        self.proposals: list[tuple[PipelineJob, dict[str, Any], ModelProvenance]] = []

    def persist_candidates(
        self,
        job: PipelineJob,
        payload: dict[str, Any],
        provenance: ModelProvenance,
    ) -> int:
        self.candidates.append((job, payload, provenance))
        return len(payload["candidate_claims"])

    def persist_proposals(
        self,
        job: PipelineJob,
        payload: dict[str, Any],
        provenance: ModelProvenance,
    ) -> int:
        self.proposals.append((job, payload, provenance))
        return len(payload["proposals"])


class _Worker:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.requests: list[WorkerRequest] = []

    def run(self, request: WorkerRequest) -> WorkerResult:
        self.requests.append(request)
        usage = WorkerUsage(
            input_tokens=80,
            cached_input_tokens=20,
            output_tokens=30,
            reasoning_output_tokens=5,
        )
        call_id = request.call_id or "unexpected-call"
        record = ModelCallRecord(
            call_id=call_id,
            job_id=request.job_id,
            phase=request.phase,
            model_id=request.model_id,
            reasoning_effort=request.reasoning_effort,
            prompt_sha256="p" * 64,
            schema_sha256="s" * 64,
            config_vector_sha256="c" * 64,
            input_bytes=len(request.prompt.encode()),
            stdout_bytes=100,
            stderr_bytes=0,
            duration_ms=10,
            status="completed",
            exit_code=0,
            usage=usage,
        )
        return WorkerResult(
            call_id=call_id,
            thread_id="thread-1",
            payload=self.payload,
            usage=usage,
            record=record,
        )


def _policy(*, private: bool = False, allowed: bool = True) -> SourcePolicy:
    return SourcePolicy(
        source_id="source-1",
        source_kind=SourceKind.CODEX_TASK,
        opted_in=True,
        allow_model_egress=allowed,
        allow_private_model_egress=private and allowed,
    )


def _job(
    *,
    sensitivity: Sensitivity = Sensitivity.NORMAL,
    policy: SourcePolicy | None = None,
) -> PipelineJob:
    source_policy = policy or _policy()
    return PipelineJob(
        job_id="job-1",
        phase=PipelinePhase.PHASE1,
        model_input='{"event_id":"ev-1","content":"private marker alpha"}',
        source_ids=("source-1",),
        source_policies={"source-1": source_policy},
        source_policy_version="policy-v7",
        maximum_sensitivity=sensitivity,
        allowed_reference_ids=("ev-1",),
        scan_bytes=200,
        episode_count=1,
        estimated_input_tokens=100,
        estimated_output_tokens=50,
        redaction_count=2,
    )


def _phase1_payload(reference: str = "ev-1") -> dict[str, Any]:
    return {
        "episode_summary": "Bounded summary",
        "no_op": False,
        "candidate_claims": [
            {
                "candidate_id": "cand-1",
                "subject": "Leo",
                "predicate": "preference.language",
                "value": "Traditional Chinese",
                "scope": "user_profile",
                "epistemic_status": "stated",
                "confidence": 0.99,
                "evidence_event_ids": [reference],
                "valid_from": None,
                "valid_to": None,
                "possible_supersedes_claim_id": None,
            }
        ],
    }


def _pipeline(
    worker: _Worker,
    operations: _Operations,
    sink: _Sink,
    *,
    ledger: InMemoryApprovalLedger | None = None,
) -> DreamingPipeline:
    return DreamingPipeline(
        worker=worker,
        operations=operations,
        output_sink=sink,
        models=PipelineModels.from_settings(ModelSettings()),
        approval_ledger=ledger,
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )


def _budget(**overrides: int) -> BudgetTracker:
    defaults = {
        "max_scan_bytes": 10_000,
        "max_episodes": 10,
        "max_model_calls": 10,
        "max_input_tokens": 10_000,
        "max_output_tokens": 10_000,
        "max_wall_seconds": 600,
    }
    defaults.update(overrides)
    return BudgetTracker(NightlyBudget(**defaults))


def test_phase1_persists_only_candidates_and_logs_metadata() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(), lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.COMPLETED
    assert outcome.persisted_count == 1
    assert operations.completed == [("job-1", "nightly-1")]
    assert not operations.failed
    assert len(sink.candidates) == 1
    assert not sink.proposals
    assert worker.requests[0].call_id == "call-1"
    assert worker.requests[0].reasoning_effort == "medium"
    telemetry_json = json.dumps(operations.created, sort_keys=True)
    assert "private marker alpha" not in telemetry_json
    assert "model_input" not in telemetry_json


def test_source_policy_deny_quarantines_before_worker_or_budget() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())
    budget = _budget()

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(policy=_policy(allowed=False)),
        lease_owner="nightly-1",
        budget=budget,
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not worker.requests
    assert not sink.candidates
    assert budget.usage.model_calls == 0
    assert operations.finished[0][1]["status"] == "quarantined"
    assert operations.failed[0][2] == "PrivacyBoundaryError"


def test_job_cannot_downgrade_private_source_to_normal() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())
    private_policy = replace(
        _policy(private=True),
        sensitivity=Sensitivity.PRIVATE,
        allow_model_egress=True,
    )

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(sensitivity=Sensitivity.NORMAL, policy=private_policy),
        lease_owner="nightly-1",
        budget=_budget(),
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not worker.requests


def test_opted_in_private_source_can_send_bounded_input_without_batch_token() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())
    job = _job(sensitivity=Sensitivity.PRIVATE, policy=_policy(private=True))

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        job, lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.COMPLETED
    assert len(worker.requests) == 1


def test_private_egress_requires_exact_registered_single_use_approval() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())
    ledger = InMemoryApprovalLedger()
    base_job = replace(
        _job(sensitivity=Sensitivity.PRIVATE, policy=_policy(private=True)),
        require_egress_approval=True,
    )
    plan = prepare_invocation(base_job, PipelineModels.from_settings(ModelSettings()))
    approval = approval_for_plan(
        plan,
        token_id="approval-1",
        source_policy_version=base_job.source_policy_version,
        expires_at=datetime(2026, 7, 19, 12, tzinfo=UTC) + timedelta(minutes=5),
    )
    ledger.register(approval)
    job = replace(base_job, approval=approval)
    pipeline = _pipeline(worker, operations, sink, ledger=ledger)

    first = pipeline.run_leased_job(job, lease_owner="nightly-1", budget=_budget())
    second = pipeline.run_leased_job(
        replace(job, job_id="job-2"), lease_owner="nightly-1", budget=_budget()
    )

    assert first.status is PipelineStatus.COMPLETED
    assert second.status is PipelineStatus.QUARANTINED
    assert len(worker.requests) == 1


def test_private_approval_does_not_authorize_changed_payload() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())
    ledger = InMemoryApprovalLedger()
    base_job = replace(
        _job(sensitivity=Sensitivity.PRIVATE, policy=_policy(private=True)),
        require_egress_approval=True,
    )
    plan = prepare_invocation(base_job, PipelineModels.from_settings(ModelSettings()))
    approval = approval_for_plan(
        plan,
        token_id="approval-1",
        source_policy_version=base_job.source_policy_version,
        expires_at=datetime(2026, 7, 19, 12, tzinfo=UTC) + timedelta(minutes=5),
    )
    ledger.register(approval)
    changed = replace(base_job, model_input="changed after preview", approval=approval)

    outcome = _pipeline(worker, operations, sink, ledger=ledger).run_leased_job(
        changed, lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not worker.requests


def test_unknown_evidence_reference_quarantines_model_output() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload(reference="ev-outside-bounds"))

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(), lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not sink.candidates
    assert operations.finished[0][1]["status"] == "quarantined"


def test_contradictory_no_op_output_is_quarantined() -> None:
    operations = _Operations()
    sink = _Sink()
    payload = _phase1_payload()
    payload["no_op"] = True
    worker = _Worker(payload)

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(), lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not sink.candidates


def test_phase1_cannot_reference_claim_outside_bounded_context() -> None:
    operations = _Operations()
    sink = _Sink()
    payload = _phase1_payload()
    payload["candidate_claims"][0]["possible_supersedes_claim_id"] = "claim-outside"
    worker = _Worker(payload)

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(), lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not sink.candidates


def test_assistant_final_cannot_propose_user_profile_claim() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())
    advisory_policy = replace(_policy(), source_kind=SourceKind.ASSISTANT_FINAL)

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(policy=advisory_policy), lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not sink.candidates


def test_budget_exhaustion_stops_before_model_call() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        _job(), lease_owner="nightly-1", budget=_budget(max_model_calls=0)
    )

    assert outcome.status is PipelineStatus.BUDGET_EXHAUSTED
    assert not worker.requests
    assert not operations.created
    assert operations.failed[0][2] == "BudgetExceeded"


def test_unredacted_secret_is_quarantined_deterministically() -> None:
    operations = _Operations()
    sink = _Sink()
    worker = _Worker(_phase1_payload())
    job = replace(_job(), model_input="token=abcdefghijklmnop")

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        job, lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not worker.requests


def test_phase2_uses_independent_model_config_and_proposal_sink() -> None:
    operations = _Operations()
    sink = _Sink()
    payload = {
        "no_op": False,
        "dispositions": [],
        "proposals": [
            {
                "proposal_id": "proposal-1",
                "operation": "ADD",
                "candidate_ids": ["cand-1"],
                "claim_id": None,
                "expected_head_set_hash": "a" * 64,
                "subject": "Leo",
                "predicate": "preference.language",
                "scope": "user_profile",
                "proposed_value": "Traditional Chinese",
                "proposed_summary": "Leo prefers Traditional Chinese.",
                "epistemic_status": "user_confirmed",
                "valid_from": None,
                "valid_to": None,
                "rationale": "Directly supported",
            }
        ],
    }
    worker = _Worker(payload)
    job = replace(
        _job(),
        phase=PipelinePhase.PHASE2,
        allowed_reference_ids=("cand-1",),
        allowed_head_set_hashes=("a" * 64,),
        episode_count=0,
    )

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        job, lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.COMPLETED
    assert len(sink.proposals) == 1
    assert worker.requests[0].reasoning_effort == "high"


def test_phase2_no_op_preserves_auditable_candidate_dispositions() -> None:
    operations = _Operations()
    sink = _Sink()
    payload = {
        "no_op": True,
        "proposals": [],
        "dispositions": [
            {
                "candidate_id": "cand-1",
                "reason": "semantic_duplicate",
                "rationale": "Already covered by bounded canonical context.",
            }
        ],
    }
    worker = _Worker(payload)
    job = replace(
        _job(),
        phase=PipelinePhase.PHASE2,
        allowed_reference_ids=("cand-1",),
        allowed_head_set_hashes=("a" * 64,),
        episode_count=0,
    )

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        job, lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.NO_OP
    assert sink.proposals[0][1]["dispositions"][0]["reason"] == "semantic_duplicate"


def test_phase2_dispositions_must_account_for_every_bounded_candidate() -> None:
    operations = _Operations()
    sink = _Sink()
    payload = {
        "no_op": True,
        "proposals": [],
        "dispositions": [
            {
                "candidate_id": "cand-1",
                "reason": "not_durable",
                "rationale": "Not durable memory.",
            }
        ],
    }
    worker = _Worker(payload)
    job = replace(
        _job(),
        phase=PipelinePhase.PHASE2,
        allowed_reference_ids=("cand-1", "cand-2"),
        allowed_head_set_hashes=("a" * 64,),
        episode_count=0,
    )

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        job, lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not sink.proposals


def test_phase2_rejects_head_hash_not_present_in_bounded_input() -> None:
    operations = _Operations()
    sink = _Sink()
    payload = {
        "no_op": False,
        "dispositions": [],
        "proposals": [
            {
                "proposal_id": "proposal-1",
                "operation": "UPDATE",
                "candidate_ids": ["cand-1"],
                "claim_id": "claim-1",
                "expected_head_set_hash": "b" * 64,
                "subject": "Leo",
                "predicate": "project.status",
                "scope": "project_state",
                "proposed_value": "done",
                "proposed_summary": "Project is done.",
                "epistemic_status": "provisional",
                "valid_from": None,
                "valid_to": None,
                "rationale": "Candidate says so",
            }
        ],
    }
    worker = _Worker(payload)
    job = replace(
        _job(),
        phase=PipelinePhase.PHASE2,
        allowed_reference_ids=("cand-1",),
        allowed_head_set_hashes=("a" * 64,),
        episode_count=0,
    )

    outcome = _pipeline(worker, operations, sink).run_leased_job(
        job, lease_owner="nightly-1", budget=_budget()
    )

    assert outcome.status is PipelineStatus.QUARANTINED
    assert not sink.proposals

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from local_dreaming.application import CanonicalApplicationService
from local_dreaming.errors import StaleProposalError
from local_dreaming.forgetting import ForgottenLedger, forget_claim
from local_dreaming.models import Sensitivity, SourceKind, SourcePolicy
from local_dreaming.persistence import StorageOutputSink
from local_dreaming.pipeline import ModelProvenance, PipelineJob, PipelinePhase
from local_dreaming.review import ReviewService, fingerprint_head_set
from local_dreaming.storage import ClaimVersionInput, EpisodeInput, EventInput, MemoryStore


def _seed(memory_path: Path) -> tuple[MemoryStore, str, str, str]:
    store = MemoryStore(memory_path)
    source_id = store.create_source(
        source_type="codex_task",
        source_fingerprint="source-1",
        source_id="source-1",
        model_egress_allowed=True,
    )
    partition_id = store.create_partition(
        source_id=source_id,
        partition_fingerprint="partition-1",
        partition_id="partition-1",
    )
    event_id = store.create_event(
        EventInput(
            event_id="event-1",
            source_id=source_id,
            partition_id=partition_id,
            event_type="message",
            content_text="Leo prefers Traditional Chinese.",
            content_fingerprint="event-content-1",
            parser_version="parser-v1",
            redactor_version="redactor-v1",
            occurred_at=datetime(2026, 7, 19, tzinfo=UTC).isoformat(),
        )
    )
    episode_id = store.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id=partition_id,
            episode_type="task",
            content_text="Leo prefers Traditional Chinese.",
            content_fingerprint="episode-content-1",
            segmenter_version="segmenter-v1",
            segmentation_reason="single task",
            event_ids=(event_id,),
        )
    )
    return store, source_id, event_id, episode_id


def _provenance() -> ModelProvenance:
    return ModelProvenance(
        call_id="call-1",
        model_id="gpt-5.6-sol",
        reasoning_effort="medium",
        prompt_hash="p" * 64,
        schema_hash="s" * 64,
        schema_version="phase1-v1",
    )


def _policy(source_id: str) -> SourcePolicy:
    return SourcePolicy(
        source_id=source_id,
        source_kind=SourceKind.CODEX_TASK,
        opted_in=True,
        allow_model_egress=True,
    )


def _semantic_bound_review(memory: Path) -> tuple[MemoryStore, str]:
    store, source_id, event_id, episode_id = _seed(memory)
    candidate_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Local-Dreaming",
        predicate="goal.context",
        scope="project_state",
        value="理解 Leo 的長期脈絡",
        proposal_type="add",
        confidence=0.9,
        extraction_fingerprint="semantic-bound-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    expected = fingerprint_head_set(())
    semantic_hash = store.semantic_context_hash((candidate_id,))
    job = PipelineJob(
        job_id="semantic-bound-phase2",
        phase=PipelinePhase.PHASE2,
        model_input="bounded candidates",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(candidate_id,),
        allowed_head_set_hashes=(expected,),
        persistence_context={
            "candidate_ids": [candidate_id],
            "semantic_context_hash": semantic_hash,
        },
    )
    StorageOutputSink(memory).persist_proposals(
        job,
        {
            "no_op": False,
            "dispositions": [],
            "proposals": [
                {
                    "operation": "ADD",
                    "candidate_ids": [candidate_id],
                    "claim_id": None,
                    "expected_head_set_hash": expected,
                    "subject": "Local-Dreaming",
                    "predicate": "goal.context",
                    "scope": "project_state",
                    "proposed_value": "理解 Leo 的長期脈絡",
                    "proposed_summary": "Local-Dreaming 理解 Leo 的長期脈絡。",
                    "epistemic_status": "provisional",
                    "valid_from": None,
                    "valid_to": None,
                    "rationale": "Direct project statement.",
                }
            ],
        },
        _provenance(),
    )
    with store.connection() as connection:
        batch_id = str(connection.execute("SELECT batch_id FROM review_batches").fetchone()[0])
    return store, batch_id


def test_phase1_sink_is_idempotent_and_binds_evidence(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, source_id, event_id, episode_id = _seed(memory)
    sink = StorageOutputSink(memory)
    job = PipelineJob(
        job_id="phase1-job",
        phase=PipelinePhase.PHASE1,
        model_input="bounded",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(event_id,),
        persistence_context={"episode_id": episode_id},
    )
    payload = {
        "no_op": False,
        "episode_summary": "language preference",
        "candidate_claims": [
            {
                "subject": "Leo",
                "predicate": "preference.language",
                "scope": "user_profile",
                "value": "Traditional Chinese",
                "epistemic_status": "stated",
                "confidence": 0.98,
                "evidence_event_ids": [event_id],
                "valid_from": None,
                "valid_to": None,
                "possible_supersedes_claim_id": None,
            }
        ],
    }

    assert sink.persist_candidates(job, payload, _provenance()) == 1
    assert sink.persist_candidates(job, payload, _provenance()) == 1
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM candidate_claims").fetchone()[0] == 1
        evidence_id = connection.execute("SELECT event_id FROM candidate_evidence").fetchone()[0]
        assert evidence_id == event_id


def test_phase1_outcome_unknown_is_suppressed_for_temporal_engine(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, source_id, event_id, episode_id = _seed(memory)
    sink = StorageOutputSink(memory)
    job = PipelineJob(
        job_id="phase1-outcome-unknown",
        phase=PipelinePhase.PHASE1,
        model_input="bounded",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(event_id,),
        persistence_context={"episode_id": episode_id},
    )
    payload = {
        "candidate_claims": [
            {
                "subject": "Local-Dreaming",
                "predicate": "implementation.status",
                "scope": "project_state",
                "value": "unknown",
                "epistemic_status": "outcome_unknown",
                "confidence": 0.4,
                "evidence_event_ids": [event_id],
                "valid_from": None,
                "valid_to": "2026-07-19T00:00:00+00:00",
                "possible_supersedes_claim_id": None,
            }
        ]
    }

    assert sink.persist_candidates(job, payload, _provenance()) == 1
    with store.connection() as connection:
        row = connection.execute(
            """
            SELECT disposition, reason_code
            FROM current_candidate_dispositions
            """
        ).fetchone()
        assert tuple(row) == (
            "suppressed",
            "phase1_outcome_unknown_requires_temporal_engine",
        )


def test_phase2_no_proposal_suppresses_evaluated_candidate(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, source_id, event_id, episode_id = _seed(memory)
    candidate_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="preference.language",
        scope="user_profile",
        value="Traditional Chinese",
        proposal_type="add",
        confidence=0.8,
        extraction_fingerprint="no-proposal-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    job = PipelineJob(
        job_id="phase2-no-proposal",
        phase=PipelinePhase.PHASE2,
        model_input="bounded candidates",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(candidate_id,),
        persistence_context={"candidate_ids": [candidate_id]},
    )

    assert (
        StorageOutputSink(memory).persist_proposals(
            job,
            {
                "no_op": True,
                "proposals": [],
                "dispositions": [
                    {
                        "candidate_id": candidate_id,
                        "reason": "semantic_duplicate",
                        "rationale": "Already represented by canonical context.",
                    }
                ],
            },
            _provenance(),
        )
        == 0
    )

    with store.connection() as connection:
        disposition = connection.execute(
            """
            SELECT disposition, reason_code FROM current_candidate_dispositions
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()
        assert tuple(disposition) == ("suppressed", "phase2_semantic_duplicate")
        assert connection.execute("SELECT COUNT(*) FROM review_batches").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("subject", "value", "summary", "reason_code"),
    [
        (
            "global AGENTS.md",
            "Agent 記錄每次工作做了什麼",
            "global AGENTS.md 應要求 Agent 記錄每次工作。",
            "phase2_overbroad_governance",
        ),
        (
            "Local-Dreaming feature",
            "時間軸與本機狀況等脈絡資訊",
            "Local-Dreaming 應納入時間軸與本機狀況。",
            "phase2_operations_only",
        ),
    ],
)
def test_phase2_sink_suppresses_deterministically_invalid_governance_proposal(
    tmp_path: Path,
    subject: str,
    value: str,
    summary: str,
    reason_code: str,
) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, source_id, event_id, episode_id = _seed(memory)
    candidate_id = store.create_candidate(
        episode_id=episode_id,
        subject_text=subject,
        predicate="should require",
        scope="project_state",
        value=value,
        proposal_type="add",
        confidence=0.9,
        extraction_fingerprint=f"policy-{reason_code}",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    expected = fingerprint_head_set(())
    job = PipelineJob(
        job_id=f"job-{reason_code}",
        phase=PipelinePhase.PHASE2,
        model_input="bounded candidates",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(candidate_id,),
        allowed_head_set_hashes=(expected,),
        persistence_context={"candidate_ids": [candidate_id]},
    )
    payload = {
        "no_op": False,
        "dispositions": [],
        "proposals": [
            {
                "operation": "ADD",
                "candidate_ids": [candidate_id],
                "claim_id": None,
                "expected_head_set_hash": expected,
                "subject": subject,
                "predicate": "should require",
                "scope": "project_state",
                "proposed_value": value,
                "proposed_summary": summary,
                "epistemic_status": "provisional",
                "valid_from": None,
                "valid_to": None,
                "rationale": "Historical proposal.",
            }
        ],
    }

    assert StorageOutputSink(memory).persist_proposals(job, payload, _provenance()) == 0
    with store.connection() as connection:
        disposition = connection.execute(
            """
            SELECT disposition, reason_code FROM current_candidate_dispositions
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()
        assert tuple(disposition) == ("suppressed", reason_code)
        assert connection.execute("SELECT COUNT(*) FROM review_batches").fetchone()[0] == 0


def test_phase2_sink_rejects_evaluated_candidate_outside_job_allowlist(
    tmp_path: Path,
) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, source_id, event_id, episode_id = _seed(memory)
    allowed_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="project.status",
        scope="project_state",
        value="active",
        proposal_type="add",
        confidence=0.8,
        extraction_fingerprint="allowed-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    outside_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="project.status",
        scope="project_state",
        value="paused",
        proposal_type="add",
        confidence=0.7,
        extraction_fingerprint="outside-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    job = PipelineJob(
        job_id="phase2-evaluated-outside",
        phase=PipelinePhase.PHASE2,
        model_input="bounded candidates",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(allowed_id,),
        persistence_context={"candidate_ids": [outside_id]},
    )

    with pytest.raises(ValueError, match="evaluated candidates exceed"):
        StorageOutputSink(memory).persist_proposals(
            job,
            {"no_op": True, "proposals": []},
            _provenance(),
        )

    with store.connection() as connection:
        dispositions = connection.execute(
            """
            SELECT candidate_id, disposition FROM current_candidate_dispositions
            WHERE candidate_id IN (?, ?) ORDER BY candidate_id
            """,
            tuple(sorted((allowed_id, outside_id))),
        ).fetchall()
        assert [row["disposition"] for row in dispositions] == ["eligible", "eligible"]
        assert connection.execute("SELECT COUNT(*) FROM review_batches").fetchone()[0] == 0


def test_phase2_sink_rejects_proposal_candidate_outside_job_allowlist(
    tmp_path: Path,
) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, source_id, event_id, episode_id = _seed(memory)
    allowed_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="project.status",
        scope="project_state",
        value="active",
        proposal_type="add",
        confidence=0.8,
        extraction_fingerprint="proposal-allowed-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    outside_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="project.status",
        scope="project_state",
        value="paused",
        proposal_type="add",
        confidence=0.7,
        extraction_fingerprint="proposal-outside-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    empty_hash = fingerprint_head_set(())
    job = PipelineJob(
        job_id="phase2-proposal-outside",
        phase=PipelinePhase.PHASE2,
        model_input="bounded candidates",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(allowed_id,),
        allowed_head_set_hashes=(empty_hash,),
        persistence_context={"candidate_ids": [allowed_id]},
    )
    payload = {
        "no_op": False,
        "proposals": [
            {
                "operation": "ADD",
                "candidate_ids": [outside_id],
                "claim_id": None,
                "expected_head_set_hash": empty_hash,
                "subject": "Leo",
                "predicate": "project.status",
                "scope": "project_state",
                "proposed_value": "paused",
                "proposed_summary": "Project is paused.",
                "epistemic_status": "provisional",
                "valid_from": None,
                "valid_to": None,
                "rationale": "Historical candidate.",
            }
        ],
    }

    with pytest.raises(ValueError, match="proposal candidates exceed"):
        StorageOutputSink(memory).persist_proposals(job, payload, _provenance())

    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM review_batches").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM review_proposals").fetchone()[0] == 0


def test_phase2_sink_creates_review_only_and_rejects_unbound_head(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, source_id, event_id, episode_id = _seed(memory)
    sink = StorageOutputSink(memory)
    phase1_job = PipelineJob(
        job_id="phase1-job",
        phase=PipelinePhase.PHASE1,
        model_input="bounded",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(event_id,),
        persistence_context={"episode_id": episode_id},
    )
    sink.persist_candidates(
        phase1_job,
        {
            "candidate_claims": [
                {
                    "subject": "Leo",
                    "predicate": "preference.language",
                    "scope": "user_profile",
                    "value": "Traditional Chinese",
                    "epistemic_status": "stated",
                    "confidence": 0.98,
                    "evidence_event_ids": [event_id],
                    "valid_from": None,
                    "valid_to": None,
                    "possible_supersedes_claim_id": None,
                },
                {
                    "subject": "Leo",
                    "predicate": "preference.language",
                    "scope": "user_profile",
                    "value": "zh-TW",
                    "epistemic_status": "stated",
                    "confidence": 0.9,
                    "evidence_event_ids": [event_id],
                    "valid_from": None,
                    "valid_to": None,
                    "possible_supersedes_claim_id": None,
                },
            ]
        },
        _provenance(),
    )
    with store.connection() as connection:
        candidate_ids = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT candidate_id FROM candidate_claims ORDER BY candidate_id"
            )
        )
    empty_hash = fingerprint_head_set(())
    semantic_context_hash = store.semantic_context_hash(candidate_ids)
    phase2_job = PipelineJob(
        job_id="phase2-job",
        phase=PipelinePhase.PHASE2,
        model_input="bounded candidates",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=candidate_ids,
        allowed_head_set_hashes=(empty_hash,),
        persistence_context={
            "candidate_ids": list(candidate_ids),
            "semantic_context_hash": semantic_context_hash,
        },
    )
    payload = {
        "no_op": False,
        "proposals": [
            {
                "proposal_id": "model-proposal-1",
                "operation": "ADD",
                "candidate_ids": list(candidate_ids),
                "claim_id": None,
                "expected_head_set_hash": empty_hash,
                "subject": "Leo",
                "predicate": "preference.language",
                "scope": "user_profile",
                "proposed_value": "Traditional Chinese",
                "proposed_summary": "Leo prefers Traditional Chinese.",
                "epistemic_status": "provisional",
                "valid_from": None,
                "valid_to": None,
                "rationale": "Direct statement.",
            }
        ],
    }

    assert sink.persist_proposals(phase2_job, payload, _provenance()) == 1
    assert store.current_revision() == 0
    with store.connection() as connection:
        batch_id = str(connection.execute("SELECT batch_id FROM review_batches").fetchone()[0])
        assert connection.execute("SELECT COUNT(*) FROM review_proposals").fetchone()[0] == 1
        assert (
            connection.execute("SELECT COUNT(*) FROM review_proposal_candidates").fetchone()[0] == 2
        )
        assert connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
        dispositions = connection.execute(
            """
            SELECT disposition FROM current_candidate_dispositions
            WHERE candidate_id IN (?, ?) ORDER BY candidate_id
            """,
            candidate_ids,
        ).fetchall()
        assert [row[0] for row in dispositions] == ["proposed", "proposed"]

    unbound = PipelineJob(
        job_id="phase2-unbound",
        phase=PipelinePhase.PHASE2,
        model_input="bounded candidates",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=candidate_ids,
        allowed_head_set_hashes=(),
    )
    with pytest.raises(ValueError, match="not supplied"):
        sink.persist_proposals(unbound, payload, _provenance())

    ReviewService(store).record_approve_all(batch_id)
    revision, version_ids = CanonicalApplicationService(memory).apply_batch(batch_id)
    assert revision == 1
    assert len(version_ids) == 1
    with store.connection() as connection:
        stored_payload = connection.execute(
            "SELECT payload_json FROM review_proposals WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()[0]
        assert "evidence_family_fingerprints" in stored_payload
        assert connection.execute("SELECT COUNT(*) FROM claim_versions").fetchone()[0] == 1


def test_review_stales_when_cross_slot_semantic_context_changes(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, batch_id = _semantic_bound_review(memory)
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="dreaming.preference",
            scope="user_profile",
            value="本機功能應理解 Leo 的長期脈絡",
            summary="Leo 希望本機 Dreaming 功能理解他的長期脈絡。",
        )
    )

    checked = ReviewService(store).validate_batch(batch_id)

    assert not checked.fresh
    assert checked.validations[0].reason == "semantic_context_changed"


def test_apply_stales_when_cross_slot_semantic_context_changes(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store, batch_id = _semantic_bound_review(memory)
    ReviewService(store).record_approve_all(batch_id)
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="dreaming.preference",
            scope="user_profile",
            value="本機功能應理解 Leo 的長期脈絡",
            summary="Leo 希望本機 Dreaming 功能理解他的長期脈絡。",
        )
    )

    with pytest.raises(StaleProposalError, match="semantic context changed"):
        CanonicalApplicationService(memory).apply_batch(batch_id)


def test_forgotten_fact_is_not_relearned_from_existing_event(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    forgotten = tmp_path / "forgotten.jsonl"
    store, source_id, event_id, episode_id = _seed(memory)
    claim_id, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value="Traditional Chinese",
            summary="Leo prefers Traditional Chinese.",
        )
    )
    forget_claim(store, ForgottenLedger(forgotten), claim_id)
    sink = StorageOutputSink(memory, forgotten)
    job = PipelineJob(
        job_id="phase1-after-forget",
        phase=PipelinePhase.PHASE1,
        model_input="bounded",
        source_ids=(source_id,),
        source_policies={source_id: _policy(source_id)},
        source_policy_version="policy-v1",
        maximum_sensitivity=Sensitivity.NORMAL,
        allowed_reference_ids=(event_id,),
        persistence_context={"episode_id": episode_id},
    )
    payload = {
        "candidate_claims": [
            {
                "subject": "Leo",
                "predicate": "preference.language",
                "scope": "user_profile",
                "value": "Traditional Chinese",
                "epistemic_status": "stated",
                "confidence": 0.98,
                "evidence_event_ids": [event_id],
                "valid_from": None,
                "valid_to": None,
                "possible_supersedes_claim_id": None,
            }
        ]
    }

    assert sink.persist_candidates(job, payload, _provenance()) == 0
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM candidate_claims").fetchone()[0] == 0

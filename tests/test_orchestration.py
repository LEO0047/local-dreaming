from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from local_dreaming.config import ModelSettings
from local_dreaming.errors import OversizedInputError, PrivacyBoundaryError
from local_dreaming.ingest import IngestService
from local_dreaming.models import IngestEventInput, Sensitivity, SourceKind, SourcePolicy
from local_dreaming.orchestration import (
    CoordinatingOperationsStore,
    DatabaseJobLoader,
    PilotOperationsStore,
    enqueue_phase1_jobs,
    enqueue_phase2_jobs,
    plan_pilot_scope,
    segment_pending_events,
)
from local_dreaming.pipeline import PipelinePhase
from local_dreaming.review import fingerprint_head_set
from local_dreaming.storage import (
    ClaimVersionInput,
    EpisodeInput,
    EventInput,
    MemoryStore,
    OperationsStore,
    identity_fingerprint,
)


def _ingested_episode(memory: MemoryStore) -> tuple[str, str]:
    source_id = memory.create_source(
        source_type="codex_task",
        source_fingerprint="codex-task-source",
        source_id="source-1",
        model_egress_allowed=True,
    )
    partition_id = memory.create_partition(
        source_id=source_id,
        partition_fingerprint="partition-1",
        external_partition_id="partition-1",
        partition_id="partition-1",
    )
    policy = SourcePolicy(
        source_id=source_id,
        source_kind=SourceKind.CODEX_TASK,
        opted_in=True,
        allow_model_egress=True,
    )
    event = (
        IngestService(memory)
        .ingest_event(
            IngestEventInput(
                source_id=source_id,
                partition_id=partition_id,
                source_kind=SourceKind.CODEX_TASK,
                external_event_id="message-1",
                occurred_at=datetime(2026, 7, 20, 8, tzinfo=UTC),
                content="Leo prefers Traditional Chinese.",
            ),
            policy,
        )
        .event
    )
    report = segment_pending_events(memory)
    return report.episode_ids[0], event.event_id


def test_phase1_queue_contains_only_opaque_reference_and_loader_bounds_input(
    tmp_path: Path,
) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)

    queued = enqueue_phase1_jobs(memory, operations)
    leased = operations.lease_job(owner="test", job_types=("phase1",))
    assert leased is not None
    job = DatabaseJobLoader(memory.path).load(leased)

    assert queued.eligible == 1
    assert leased["payload"] == {"episode_id": episode_id}
    assert "Traditional Chinese" not in str(leased)
    assert job.phase is PipelinePhase.PHASE1
    assert job.allowed_reference_ids == (event_id,)
    assert "Traditional Chinese" in job.model_input
    assert job.persistence_context == {"episode_id": episode_id}


def test_phase1_loader_collapses_effective_duplicate_evidence(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    source_id = memory.create_source(
        source_type="codex_task",
        source_fingerprint="duplicate-source",
        source_id="duplicate-source",
        model_egress_allowed=True,
    )
    partition_id = memory.create_partition(
        source_id=source_id,
        partition_fingerprint="duplicate-partition",
        partition_id="duplicate-partition",
    )
    event_ids = tuple(
        memory.create_event(
            EventInput(
                source_id=source_id,
                partition_id=partition_id,
                external_event_id=f"duplicate-{index}",
                event_type="codex_task",
                content_text="same statement",
                content_fingerprint="same-content",
                parser_version="source-adapters-v2",
                redactor_version="redactor-v1",
                occurred_at=f"2026-07-20T03:01:00.0{index + 1:02d}Z",
                metadata={"role": "user", "source_sequence": index + 10},
            )
        )
        for index in range(2)
    )
    episode_id = memory.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id=partition_id,
            episode_type="task",
            content_text="same statement\n\nsame statement",
            content_fingerprint="duplicate-episode",
            segmenter_version="v1",
            segmentation_reason="end_of_input",
            event_ids=event_ids,
        )
    )

    enqueue_phase1_jobs(memory, operations, episode_allowlist=(episode_id,))
    leased = operations.lease_job(owner="test", job_types=("phase1",))
    assert leased is not None
    job = DatabaseJobLoader(memory.path).load(leased)

    assert job.allowed_reference_ids == (event_ids[0],)
    assert event_ids[1] not in job.model_input
    assert job.model_input.count("same statement") == 1


def test_oversized_event_and_loader_payload_fail_closed(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    source_id = memory.create_source(
        source_type="codex_task",
        source_fingerprint="oversized-source",
        source_id="oversized-source",
        model_egress_allowed=True,
    )
    partition_id = memory.create_partition(
        source_id=source_id,
        partition_fingerprint="oversized-partition",
        partition_id="oversized-partition",
    )
    policy = SourcePolicy(
        source_id=source_id,
        source_kind=SourceKind.CODEX_TASK,
        opted_in=True,
        allow_model_egress=True,
    )
    oversized = IngestService(memory).ingest_event(
        IngestEventInput(
            source_id=source_id,
            partition_id=partition_id,
            source_kind=SourceKind.CODEX_TASK,
            external_event_id="oversized-event",
            occurred_at=datetime(2026, 7, 20, 8, tzinfo=UTC),
            content="X" * 12_001,
        ),
        policy,
    )

    segmentation = segment_pending_events(memory)

    assert segmentation.blocked_events == 1
    assert segmentation.episodes == 0
    assert segmentation.diagnostics[0]["event_id"] == oversized.event.event_id
    assert segmentation.diagnostics[0]["code"] == "oversized_event_blocked"

    oversized_episode_id = memory.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id=partition_id,
            episode_type="task",
            content_text="X" * 12_001,
            content_fingerprint="oversized-episode",
            segmenter_version="v1",
            segmentation_reason="legacy_import",
            event_ids=(oversized.event.event_id,),
        )
    )
    enqueue_phase1_jobs(
        memory,
        operations,
        episode_allowlist=(oversized_episode_id,),
    )
    oversized_lease = operations.lease_job(owner="test", job_types=("phase1",))
    assert oversized_lease is not None
    with pytest.raises(OversizedInputError, match="event .* exceeds"):
        DatabaseJobLoader(memory.path).load(oversized_lease)

    event_ids = tuple(
        memory.create_event(
            EventInput(
                source_id=source_id,
                partition_id=partition_id,
                external_event_id=f"bounded-{index}",
                event_type="codex_task",
                content_text=str(index) * 5_900,
                content_fingerprint=f"bounded-content-{index}",
                parser_version="parser-v1",
                redactor_version="redactor-v1",
                occurred_at=f"2026-07-20T09:0{index}:00Z",
                metadata={"role": "user", "source_sequence": index + 20},
            )
        )
        for index in range(2)
    )
    episode_id = memory.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id=partition_id,
            episode_type="task",
            content_text=("0" * 5_900) + "\n\n" + ("1" * 5_900),
            content_fingerprint="bounded-combined-episode",
            segmenter_version="v1",
            segmentation_reason="end_of_input",
            event_ids=event_ids,
        )
    )
    operations = OperationsStore(tmp_path / "combined-operations.sqlite3")
    enqueue_phase1_jobs(memory, operations, episode_allowlist=(episode_id,))
    leased = operations.lease_job(owner="test", job_types=("phase1",))
    assert leased is not None

    with pytest.raises(OversizedInputError, match="loader input exceeds"):
        DatabaseJobLoader(memory.path).load(leased)
    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0] == 0


def test_phase2_requires_eligible_disposition_and_honors_allowlist(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    candidate_ids = tuple(
        memory.create_candidate(
            episode_id=episode_id,
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value=value,
            proposal_type="add",
            confidence=0.8,
            extraction_fingerprint=f"allowlist-{index}",
            extractor_version="v1",
            prompt_hash="prompt",
            schema_version="schema",
            model_id="model",
            reasoning_effort="medium",
            evidence_event_ids=(event_id,),
        )
        for index, value in enumerate(("Traditional Chinese", "zh-TW"))
    )
    memory.set_candidate_disposition(
        candidate_ids[1],
        disposition="suppressed",
        reason_code="test_suppression",
    )
    operations = OperationsStore(tmp_path / "operations.sqlite3")

    with pytest.raises(ValueError, match="suppressed"):
        enqueue_phase2_jobs(
            memory,
            operations,
            candidate_allowlist=(candidate_ids[0], candidate_ids[1]),
        )
    with pytest.raises(ValueError, match="unknown"):
        enqueue_phase2_jobs(
            memory,
            operations,
            candidate_allowlist=(candidate_ids[0], "candidate-missing"),
        )

    report = enqueue_phase2_jobs(
        memory,
        operations,
        candidate_allowlist=(candidate_ids[0],),
    )
    leased = operations.lease_job(owner="test", job_types=("phase2",))

    assert report.queued == 1
    assert leased is not None
    assert leased["payload"]["candidate_ids"] == [candidate_ids[0]]
    assert leased["payload"]["as_of"]
    assert len(leased["payload"]["semantic_context_hash"]) == 64


def test_phase2_exact_allowlist_rejects_candidates_deferred_by_group_limit(
    tmp_path: Path,
) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    candidate_ids = tuple(
        memory.create_candidate(
            episode_id=episode_id,
            subject_text="Leo",
            predicate=predicate,
            scope="project_state",
            value="active",
            proposal_type="add",
            confidence=0.8,
            extraction_fingerprint=f"limited-{index}",
            extractor_version="v1",
            prompt_hash="prompt",
            schema_version="schema",
            model_id="model",
            reasoning_effort="medium",
            evidence_event_ids=(event_id,),
        )
        for index, predicate in enumerate(("project.alpha", "project.beta"))
    )
    operations = OperationsStore(tmp_path / "operations.sqlite3")

    with pytest.raises(ValueError, match="exceeds the phase2 group limit"):
        enqueue_phase2_jobs(
            memory,
            operations,
            limit=1,
            candidate_allowlist=candidate_ids,
        )

    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_phase2_loader_rejects_candidate_suppressed_after_enqueue(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    candidate_id = memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="project.status",
        scope="project_state",
        value="active",
        proposal_type="add",
        confidence=0.8,
        extraction_fingerprint="stale-after-enqueue",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    enqueue_phase2_jobs(memory, operations, candidate_allowlist=(candidate_id,))
    leased = operations.lease_job(owner="test", job_types=("phase2",))
    assert leased is not None
    memory.set_candidate_disposition(
        candidate_id,
        disposition="suppressed",
        reason_code="operator_suppressed_after_enqueue",
    )

    with pytest.raises(ValueError, match="invalid or stale") as captured:
        DatabaseJobLoader(memory.path).load(leased)
    assert captured.value.__cause__ is not None
    assert "no longer eligible" in str(captured.value.__cause__)

    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0] == 0


def test_phase2_loader_binds_candidate_and_current_head_set(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    claim_id, _, _ = memory.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value="English",
            summary="Leo prefers English.",
        )
    )
    related_claim_id, _, _ = memory.add_claim_version(
        ClaimVersionInput(
            subject_text="Local-Dreaming",
            predicate="language.preference",
            scope="project_state",
            value="使用繁體中文",
            summary="Local-Dreaming 應延續 Leo 使用繁體中文的偏好。",
        )
    )
    candidate_id = memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="preference.language",
        scope="user_profile",
        value="Traditional Chinese",
        proposal_type="add",
        confidence=0.99,
        extraction_fingerprint="candidate-extraction",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )

    report = enqueue_phase2_jobs(memory, operations)
    leased = operations.lease_job(owner="test", job_types=("phase2",))
    assert leased is not None
    job = DatabaseJobLoader(memory.path).load(leased)

    assert report.eligible == 1
    assert job.phase is PipelinePhase.PHASE2
    assert job.allowed_reference_ids == (candidate_id,)
    identity = identity_fingerprint("Leo", "preference.language", "user_profile")
    assert job.allowed_head_set_hashes == (fingerprint_head_set(memory.load_heads(identity)),)
    assert job.allowed_claim_ids == (claim_id,)
    model_input = json.loads(job.model_input)
    assert model_input["as_of"]
    assert model_input["candidates"][0]["effective_evidence_count"] == 1
    assert model_input["candidates"][0]["evidence_occurred_from"]
    related = model_input["related_canonical_heads"]
    assert related["context_hash"] == job.persistence_context["semantic_context_hash"]
    assert related["neighbors"][0]["claim_id"] == related_claim_id
    assert related_claim_id not in job.allowed_claim_ids
    assert model_input["current_heads"][0]["recorded_at"]


def test_dry_run_queue_does_not_touch_operations_db(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    _ingested_episode(memory)

    report = enqueue_phase1_jobs(memory, operations, dry_run=True)

    assert report.dry_run
    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 0


def test_phase1_dedupe_changes_with_model_configuration(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    _ingested_episode(memory)

    first = enqueue_phase1_jobs(memory, operations)
    changed = enqueue_phase1_jobs(
        memory,
        operations,
        models=ModelSettings(phase1_model="gpt-new"),
    )

    assert first.queued == 1
    assert changed.queued == 1
    assert first.job_ids != changed.job_ids


def test_phase2_unrelated_memory_revision_does_not_requeue(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="preference.language",
        scope="user_profile",
        value="Traditional Chinese",
        proposal_type="add",
        confidence=0.9,
        extraction_fingerprint="candidate-stable",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    first = enqueue_phase2_jobs(memory, operations)
    memory.add_claim_version(
        ClaimVersionInput(
            subject_text="Unrelated",
            predicate="project.status",
            scope="project_state",
            value="active",
            summary="An unrelated project is active.",
        )
    )

    second = enqueue_phase2_jobs(memory, operations)

    assert first.queued == 1
    assert second.queued == 0


def test_private_head_uses_only_explicit_safe_summary_in_phase2(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    memory.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value="RAW PRIVATE VALUE",
            summary="RAW PRIVATE SUMMARY",
            mcp_safe_summary="Approved safe language preference.",
            sensitivity="private",
        )
    )
    memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="preference.language",
        scope="user_profile",
        value="Traditional Chinese",
        proposal_type="update",
        confidence=0.9,
        extraction_fingerprint="private-head-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    enqueue_phase2_jobs(memory, operations)
    leased = operations.lease_job(owner="test", job_types=("phase2",))
    assert leased is not None

    job = DatabaseJobLoader(memory.path).load(leased)

    assert job.maximum_sensitivity is Sensitivity.NORMAL
    assert "Approved safe language preference." in job.model_input
    assert "RAW PRIVATE" not in job.model_input


def test_advisory_episodes_are_not_queued_as_phase1_claim_extraction(
    tmp_path: Path,
) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    source_id = memory.create_source(
        source_type="chronicle",
        source_fingerprint="advisory-source",
        source_id="advisory-source",
        trust_level="advisory",
        model_egress_allowed=True,
    )
    partition_id = memory.create_partition(
        source_id=source_id,
        partition_fingerprint="advisory-partition",
        partition_id="advisory-partition",
    )
    IngestService(memory).ingest_event(
        IngestEventInput(
            source_id=source_id,
            partition_id=partition_id,
            source_kind=SourceKind.CHRONICLE,
            external_event_id="advisory-event",
            occurred_at=datetime(2026, 7, 20, 8, tzinfo=UTC),
            content="Hint only.",
        ),
        SourcePolicy(
            source_id=source_id,
            source_kind=SourceKind.CHRONICLE,
            opted_in=True,
            allow_model_egress=True,
        ),
    )
    report = segment_pending_events(memory)
    assert report.episodes == 1

    queued = enqueue_phase1_jobs(memory, operations)

    assert queued.queued == 0
    assert queued.skipped_advisory == 1


def test_segmentation_episode_budget_defers_without_losing_events(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    service = IngestService(memory)
    for index in range(2):
        source_id = memory.create_source(
            source_type="codex_task",
            source_fingerprint=f"budget-source-{index}",
            source_id=f"budget-source-{index}",
            model_egress_allowed=True,
        )
        partition_id = memory.create_partition(
            source_id=source_id,
            partition_fingerprint=f"budget-partition-{index}",
            partition_id=f"budget-partition-{index}",
        )
        service.ingest_event(
            IngestEventInput(
                source_id=source_id,
                partition_id=partition_id,
                source_kind=SourceKind.CODEX_TASK,
                external_event_id=f"budget-event-{index}",
                occurred_at=datetime(2026, 7, 20, 8 + index, tzinfo=UTC),
                content=f"Bounded event {index}.",
            ),
            SourcePolicy(
                source_id=source_id,
                source_kind=SourceKind.CODEX_TASK,
                opted_in=True,
                allow_model_egress=True,
            ),
        )

    first = segment_pending_events(memory, limit=1)
    second = segment_pending_events(memory, limit=1)

    assert first.episodes == 1
    assert first.persisted == 1
    assert first.deferred_episodes == 1
    assert second.episodes == 1
    assert second.persisted == 1
    assert second.deferred_episodes == 0
    assert first.episode_ids != second.episode_ids

    with memory.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM episode_events").fetchone()[0] == 2


def test_private_head_without_egress_or_safe_summary_fails_closed(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    memory.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value="RAW PRIVATE VALUE",
            summary="RAW PRIVATE SUMMARY",
            sensitivity="private",
        )
    )
    memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="preference.language",
        scope="user_profile",
        value="Traditional Chinese",
        proposal_type="update",
        confidence=0.9,
        extraction_fingerprint="unsafe-private-head-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    enqueue_phase2_jobs(memory, operations)
    leased = operations.lease_job(owner="test", job_types=("phase2",))
    assert leased is not None

    with pytest.raises(PrivacyBoundaryError, match="safe summary"):
        DatabaseJobLoader(memory.path).load(leased)


def test_last_phase1_completion_queues_phase2_for_same_bounded_run(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = CoordinatingOperationsStore(
        tmp_path / "operations.sqlite3",
        memory=memory,
        models=ModelSettings(),
        phase2_limit=40,
    )
    episode_id, event_id = _ingested_episode(memory)
    memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="preference.language",
        scope="user_profile",
        value="Traditional Chinese",
        proposal_type="add",
        confidence=0.9,
        extraction_fingerprint="candidate-after-phase1",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    job_id = operations.enqueue_job(
        job_type="phase1", dedupe_key="phase1-to-complete", payload={"episode_id": episode_id}
    )
    leased = operations.lease_job(owner="nightly", job_types=("phase1",))
    assert leased is not None

    assert operations.complete_job(job_id, owner="nightly")

    with operations.connection() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE job_type = 'phase2' AND status = 'queued'"
            ).fetchone()[0]
            == 1
        )


def test_pilot_scope_leases_only_allowlisted_jobs_and_new_candidates(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    base_operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    baseline_candidate = memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="historical.candidate",
        scope="project_state",
        value="must not be swept into pilot",
        proposal_type="add",
        confidence=0.8,
        extraction_fingerprint="pilot-baseline-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    selected_job = base_operations.enqueue_job(
        job_type="phase1",
        dedupe_key="pilot-selected-phase1",
        payload={"episode_id": episode_id},
        priority=1,
    )
    foreign_job = base_operations.enqueue_job(
        job_type="phase1",
        dedupe_key="pilot-foreign-phase1",  # gitleaks:allow
        payload={"episode_id": episode_id},
        priority=100,
    )
    scope = plan_pilot_scope(
        memory,
        base_operations,
        phase1_job_allowlist=(selected_job,),
    )
    pilot = PilotOperationsStore(
        base_operations.path,
        memory=memory,
        models=ModelSettings(),
        scope=scope,
        phase2_limit=5,
    )
    new_candidate = memory.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="pilot.new_candidate",
        scope="project_state",
        value="only this candidate is in scope",
        proposal_type="add",
        confidence=0.8,
        extraction_fingerprint="pilot-new-candidate",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )

    leased = pilot.lease_job(owner="pilot-owner", job_types=("phase1", "phase2"))
    assert leased is not None
    assert leased["job_id"] == selected_job
    assert pilot.complete_job(selected_job, owner="pilot-owner")

    with pilot.connection() as connection:
        assert (
            connection.execute(
                "SELECT status FROM jobs WHERE job_id = ?", (foreign_job,)
            ).fetchone()[0]
            == "queued"
        )
        phase2 = connection.execute(
            "SELECT payload_json FROM jobs WHERE job_type = 'phase2'"
        ).fetchone()
    assert phase2 is not None
    payload = json.loads(str(phase2[0]))
    assert payload["candidate_ids"] == [new_candidate]
    assert baseline_candidate not in payload["candidate_ids"]

    leased_phase2 = pilot.lease_job(owner="pilot-owner", job_types=("phase1", "phase2"))
    assert leased_phase2 is not None
    assert leased_phase2["job_type"] == "phase2"
    assert leased_phase2["job_id"] in pilot.phase2_job_ids


def test_pilot_phase2_group_limit_fails_closed_before_enqueue(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    base_operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id, event_id = _ingested_episode(memory)
    selected_job = base_operations.enqueue_job(
        job_type="phase1",
        dedupe_key="pilot-limited-phase1",  # gitleaks:allow
        payload={"episode_id": episode_id},
    )
    scope = plan_pilot_scope(
        memory,
        base_operations,
        phase1_job_allowlist=(selected_job,),
    )
    pilot = PilotOperationsStore(
        base_operations.path,
        memory=memory,
        models=ModelSettings(),
        scope=scope,
        phase2_limit=1,
    )
    for index in range(2):
        memory.create_candidate(
            episode_id=episode_id,
            subject_text="Leo",
            predicate=f"pilot.limit.{index}",
            scope="project_state",
            value=index,
            proposal_type="add",
            confidence=0.8,
            extraction_fingerprint=f"pilot-limit-candidate-{index}",
            extractor_version="v1",
            prompt_hash="prompt",
            schema_version="schema",
            model_id="model",
            reasoning_effort="medium",
            evidence_event_ids=(event_id,),
        )
    leased = pilot.lease_job(owner="pilot-owner", job_types=("phase1", "phase2"))
    assert leased is not None

    with pytest.raises(ValueError, match="exceeds the phase2 group limit"):
        pilot.complete_job(selected_job, owner="pilot-owner")
    with pilot.connection() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM jobs WHERE job_type = 'phase2'").fetchone()[0]
            == 0
        )

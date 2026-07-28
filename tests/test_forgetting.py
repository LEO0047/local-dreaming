from __future__ import annotations

import fcntl
import os
from pathlib import Path

from local_dreaming.forgetting import (
    ForgottenEntry,
    ForgottenLedger,
    apply_forgotten_ledger,
    forget_claim,
    forget_source,
)
from local_dreaming.storage import (
    ClaimVersionInput,
    EpisodeInput,
    EventInput,
    EvidenceInput,
    MemoryStore,
    ReviewProposalInput,
    identity_fingerprint,
    memory_maintenance_lock_path,
    online_backup,
    restore_database,
)


class _LockAssertingLedger(ForgottenLedger):
    def __init__(self, path: Path, memory_path: Path) -> None:
        super().__init__(path)
        self.lock_path = memory_maintenance_lock_path(memory_path)
        self.observed_locked_prepare = False

    def prepare(
        self,
        *,
        target_kind: str,
        target_fingerprint: str,
        identity_fingerprint: str | None = None,
        value_fingerprint: str | None = None,
        reason: str | None = None,
    ) -> ForgottenEntry:
        descriptor = os.open(self.lock_path, os.O_RDWR)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.observed_locked_prepare = True
            else:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                raise AssertionError("forget ledger prepare ran outside maintenance lock")
        finally:
            os.close(descriptor)
        return super().prepare(
            target_kind=target_kind,
            target_fingerprint=target_fingerprint,
            identity_fingerprint=identity_fingerprint,
            value_fingerprint=value_fingerprint,
            reason=reason,
        )


def _source_event(store: MemoryStore, suffix: str) -> tuple[str, str]:
    source_id = store.create_source(
        source_type="codex_task",
        source_fingerprint=f"source-fingerprint-{suffix}",
        source_id=f"source-{suffix}",
    )
    partition_id = store.create_partition(
        source_id=source_id,
        partition_fingerprint=f"partition-fingerprint-{suffix}",
        partition_id=f"partition-{suffix}",
    )
    event_id = store.create_event(
        EventInput(
            event_id=f"event-{suffix}",
            source_id=source_id,
            partition_id=partition_id,
            event_type="message",
            content_text=f"evidence {suffix}",
            content_fingerprint=f"event-fingerprint-{suffix}",
            parser_version="parser-v1",
            redactor_version="redactor-v1",
        )
    )
    return source_id, event_id


def test_forget_claim_removes_fts_and_blocks_old_snapshot_relearning(tmp_path: Path) -> None:
    memory_path = tmp_path / "memory.sqlite3"
    snapshot = tmp_path / "before-forget.sqlite3"
    ledger = ForgottenLedger(tmp_path / "forgotten.jsonl")
    store = MemoryStore(memory_path)
    claim_id, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="temporary_preference",
            scope="user_profile",
            value="old value",
            summary="This should be forgotten.",
        )
    )
    online_backup(memory_path, snapshot)

    report = forget_claim(store, ledger, claim_id)

    assert report.deleted_claims == 1
    assert report.memory_revision == 2
    assert store.search("forgotten") == []
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
    entries = ledger.effective_entries()
    assert len(entries) == 1
    assert entries[0].status == "applied"
    assert entries[0].target_kind == "claim_exact"

    restore_database(snapshot, memory_path, expected_role="memory")
    restored = MemoryStore(memory_path)
    assert restored.search("forgotten")
    assert apply_forgotten_ledger(restored, ledger) >= 1
    assert restored.search("forgotten") == []


def test_identity_wide_forget_suppresses_any_value(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    ledger = ForgottenLedger(tmp_path / "forgotten.jsonl")
    claim_id, version_id, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="old_plan",
            scope="project_state",
            value="plan A",
            summary="Old plan A.",
        )
    )
    with store.connection() as connection:
        row = connection.execute(
            """
            SELECT c.identity_fingerprint, cv.value_fingerprint
            FROM claims AS c JOIN claim_versions AS cv ON cv.claim_id = c.claim_id
            WHERE cv.claim_version_id = ?
            """,
            (version_id,),
        ).fetchone()

    forget_claim(store, ledger, claim_id, identity_wide=True)

    assert ledger.is_claim_forgotten(str(row[0]), str(row[1]))
    assert ledger.is_claim_forgotten(str(row[0]), "completely-new-value")


def test_forget_claim_dry_run_has_no_side_effects(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    ledger = ForgottenLedger(tmp_path / "forgotten.jsonl")
    claim_id, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference",
            scope="user_profile",
            value="keep",
            summary="Keep this.",
        )
    )

    report = forget_claim(store, ledger, claim_id, dry_run=True)

    assert report.dry_run
    assert ledger.entries() == []
    assert store.search("Keep")
    assert store.current_revision() == 1


def test_forget_claim_prepares_suppression_under_canonical_writer_lock(
    tmp_path: Path,
) -> None:
    memory_path = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory_path)
    ledger = _LockAssertingLedger(tmp_path / "forgotten.jsonl", memory_path)
    claim_id, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="race_preference",
            scope="user_profile",
            value="old",
            summary="Old value.",
        )
    )

    forget_claim(store, ledger, claim_id)

    assert ledger.observed_locked_prepare
    assert store.search("Old") == []


def test_forget_source_prepares_suppression_under_canonical_writer_lock(
    tmp_path: Path,
) -> None:
    memory_path = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory_path)
    ledger = _LockAssertingLedger(tmp_path / "forgotten.jsonl", memory_path)
    source_id, _ = _source_event(store, "race")

    forget_source(store, ledger, source_id)

    assert ledger.observed_locked_prepare
    with store.connection() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM sources WHERE source_id = ?", (source_id,)
            ).fetchone()[0]
            == 0
        )


def test_forget_source_preserves_multi_source_claim_and_prunes_unique_claim(
    tmp_path: Path,
) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    ledger = ForgottenLedger(tmp_path / "forgotten.jsonl")
    source_a, event_a = _source_event(store, "a")
    _, event_b = _source_event(store, "b")
    shared_claim, shared_version, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Project",
            predicate="status",
            scope="project_state",
            value="active",
            summary="Project is active.",
            provenance_kind="tool_verified",
            evidence=(
                EvidenceInput(event_a, "tool", "evidence-a"),
                EvidenceInput(event_b, "tool", "evidence-b"),
            ),
        )
    )
    unique_claim, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Project",
            predicate="temporary_state",
            scope="project_state",
            value="only A",
            summary="Only source A supports this.",
            provenance_kind="model_proposal",
            evidence=(EvidenceInput(event_a, "conversation", "evidence-only-a"),),
        )
    )

    report = forget_source(store, ledger, source_a)

    assert report.deleted_sources == 1
    assert report.deleted_claim_versions == 1
    assert ledger.is_source_forgotten("source-fingerprint-a")
    with store.connection() as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM claims WHERE claim_id = ?", (shared_claim,)
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM claim_evidence WHERE claim_version_id = ?",
                (shared_version,),
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM claims WHERE claim_id = ?", (unique_claim,)
            ).fetchone()[0]
            == 0
        )
    assert store.search("active")
    assert store.search("Only") == []


def test_source_ledger_reapplies_after_restore(tmp_path: Path) -> None:
    memory_path = tmp_path / "memory.sqlite3"
    snapshot = tmp_path / "source-before.sqlite3"
    store = MemoryStore(memory_path)
    ledger = ForgottenLedger(tmp_path / "forgotten.jsonl")
    source_id, _ = _source_event(store, "restore")
    online_backup(memory_path, snapshot)
    forget_source(store, ledger, source_id)

    restore_database(snapshot, memory_path, expected_role="memory")
    restored = MemoryStore(memory_path)
    assert apply_forgotten_ledger(restored, ledger) >= 1
    with restored.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM sources").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


def test_forget_source_purges_candidate_and_review_payloads(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    ledger = ForgottenLedger(tmp_path / "forgotten.jsonl")
    source_id, event_id = _source_event(store, "review")
    episode_id = store.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id="partition-review",
            episode_type="task",
            content_text="private source-derived fact",
            content_fingerprint="episode-review",
            segmenter_version="v1",
            segmentation_reason="test",
            event_ids=(event_id,),
        )
    )
    candidate_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="source.fact",
        scope="project_state",
        value="forget me",
        proposal_type="add",
        confidence=0.8,
        extraction_fingerprint="extract-review",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="v1",
        model_id="model",
        reasoning_effort="medium",
        evidence_event_ids=(event_id,),
    )
    slot = identity_fingerprint("Leo", "source.fact", "project_state")
    store.create_review_batch(
        [
            ReviewProposalInput(
                proposal_type="add",
                target_slot_fingerprint=slot,
                expected_head_set_hash="empty",
                precondition_hash="precondition",
                proposal_payload_fingerprint="payload",
                evidence_set_fingerprint="evidence",
                payload={"summary": "private source-derived fact"},
                candidate_id=candidate_id,
                candidate_ids=(candidate_id,),
            )
        ]
    )

    forget_source(store, ledger, source_id)

    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM candidate_claims").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM review_proposals").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM review_batches").fetchone()[0] == 0

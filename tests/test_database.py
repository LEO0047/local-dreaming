from __future__ import annotations

import fcntl
import json
import multiprocessing as mp
import os
import sqlite3
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

import local_dreaming.storage as storage_module
from local_dreaming.database import DatabaseError, connect_memory, get_memory_revision
from local_dreaming.episode_identity import episode_id_for, event_sequence_fingerprint
from local_dreaming.review import fingerprint_head_set, fingerprint_target_slot
from local_dreaming.source_adapters import (
    build_automation_snapshot,
    capture_health_snapshot,
    capture_workspace_snapshot,
)
from local_dreaming.storage import (
    ClaimVersionInput,
    EpisodeInput,
    EventInput,
    MemoryStore,
    OperationsStore,
    ReviewProposalInput,
    identity_fingerprint,
    online_backup,
    operations_maintenance_lock_path,
    restore_database,
)


def _source_and_event(store: MemoryStore) -> tuple[str, str]:
    source_id = store.create_source(
        source_type="codex_task",
        source_fingerprint="source-fingerprint",
        source_id="source-1",
    )
    event_id = store.create_event(
        EventInput(
            event_id="event-1",
            source_id=source_id,
            event_type="message",
            content_text="Leo prefers Traditional Chinese",
            content_fingerprint="event-fingerprint",
            parser_version="parser-v1",
            redactor_version="redactor-v1",
        )
    )
    return source_id, event_id


def _connect_memory_in_process(path: str, gate: object, attempted: object) -> None:
    gate.wait()  # type: ignore[attr-defined]
    attempted.set()  # type: ignore[attr-defined]
    with connect_memory(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def _enqueue_operations_in_process(path: str, gate: object, attempted: object) -> None:
    gate.wait()  # type: ignore[attr-defined]
    attempted.set()  # type: ignore[attr-defined]
    OperationsStore(path, initialize=False).enqueue_job(
        job_type="test",
        dedupe_key="cross-process-transaction",
    )


def test_review_target_slot_matches_canonical_claim_identity() -> None:
    assert identity_fingerprint(
        " Leo ", "Preferred_Language", "USER_PROFILE"
    ) == fingerprint_target_slot(
        subject_text=" Leo ",
        predicate="Preferred_Language",
        scope="USER_PROFILE",
    )


def test_initializes_separate_private_databases_and_revision(tmp_path: Path) -> None:
    memory_path = tmp_path / "runtime" / "memory.sqlite3"
    operations_path = tmp_path / "runtime" / "operations.sqlite3"

    memory = MemoryStore(memory_path)
    operations = OperationsStore(operations_path)
    operations.enqueue_job(job_type="ingest", dedupe_key="ingest:1")

    assert memory.current_revision() == 0
    assert stat.S_IMODE(memory_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(memory_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(operations_path.stat().st_mode) == 0o600
    with operations.connection() as connection:
        assert (
            connection.execute("SELECT value FROM metadata WHERE key = 'database_role'").fetchone()[
                0
            ]
            == "operations"
        )
        assert (
            connection.execute(
                "SELECT value FROM metadata WHERE key = 'memory_revision'"
            ).fetchone()
            is None
        )


def test_operational_snapshots_never_touch_memory_revision(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    captured_at = datetime(2026, 7, 19, tzinfo=UTC)
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    operations.record_operational_snapshot(
        capture_workspace_snapshot(workspace, captured_at=captured_at, git_dirty_count=1)
    )
    operations.record_operational_snapshot(
        build_automation_snapshot(
            automation_type="launchd",
            automation_id="dreaming",
            status="gated",
            captured_at=captured_at,
        )
    )
    operations.record_operational_snapshot(
        capture_health_snapshot({"workspace": workspace}, captured_at=captured_at)
    )

    assert memory.current_revision() == 0
    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM workspace_snapshots").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM automation_snapshots").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM health_snapshots").fetchone()[0] == 1


def test_repeated_source_identity_refresh_can_revoke_egress_and_opt_in(
    tmp_path: Path,
) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    source_id = store.create_source(
        source_type="codex_task",
        source_fingerprint="policy-source",
        source_id="source-policy",
        sensitivity="private",
        model_egress_allowed=True,
        policy_version="v1",
        metadata={"allow_private_model_egress": True, "label": "keep"},
    )
    partition_id = store.create_partition(
        source_id=source_id,
        partition_fingerprint="policy-partition",
        partition_id="partition-policy",
        opted_in=True,
        sensitivity="private",
    )

    assert (
        store.create_source(
            source_type="codex_task",
            source_fingerprint="policy-source",
            source_id="source-policy",
            sensitivity="private",
            model_egress_allowed=False,
            policy_version="v2",
        )
        == source_id
    )
    assert (
        store.create_partition(
            source_id=source_id,
            partition_fingerprint="policy-partition",
            partition_id="partition-policy",
            opted_in=False,
            sensitivity="private",
        )
        == partition_id
    )

    with store.connection() as connection:
        source = connection.execute(
            "SELECT * FROM sources WHERE source_id = ?", (source_id,)
        ).fetchone()
        partition = connection.execute(
            "SELECT * FROM source_partitions WHERE partition_id = ?", (partition_id,)
        ).fetchone()
    assert source is not None and partition is not None
    assert source["model_egress_allowed"] == 0
    assert source["policy_version"] == "v2"
    assert json.loads(source["metadata_json"])["allow_private_model_egress"] is False
    assert partition["opted_in"] == 0
    assert store.current_revision() == 0


def test_explicit_policy_updates_are_fail_closed_and_do_not_bump_revision(
    tmp_path: Path,
) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    source_id = store.create_source(
        source_type="manual",
        source_fingerprint="explicit-policy-source",
    )
    partition_id = store.create_partition(
        source_id=source_id,
        partition_fingerprint="explicit-policy-partition",
    )

    assert store.set_source_policy(
        source_id,
        sensitivity="normal",
        model_egress_allowed=False,
        policy_version="v2",
        metadata={"allow_private_model_egress": True, "note": "revoked"},
    )
    assert store.set_partition_policy(
        partition_id,
        opted_in=False,
        sensitivity="private",
        metadata={"note": "revoked"},
    )
    assert not store.set_source_policy(
        source_id,
        sensitivity="normal",
        model_egress_allowed=False,
        policy_version="v2",
        metadata={"allow_private_model_egress": False, "note": "revoked"},
    )

    with store.connection() as connection:
        source = connection.execute(
            "SELECT * FROM sources WHERE source_id = ?", (source_id,)
        ).fetchone()
        partition = connection.execute(
            "SELECT * FROM source_partitions WHERE partition_id = ?", (partition_id,)
        ).fetchone()
    assert source is not None and partition is not None
    assert json.loads(source["metadata_json"])["allow_private_model_egress"] is False
    assert source["model_egress_allowed"] == 0
    assert partition["opted_in"] == 0
    assert store.current_revision() == 0
    with pytest.raises(KeyError, match="unknown source"):
        store.set_source_policy(
            "missing",
            sensitivity="normal",
            model_egress_allowed=False,
            policy_version="v1",
        )
    with pytest.raises(KeyError, match="unknown partition"):
        store.set_partition_policy("missing", opted_in=False, sensitivity="normal")
    with pytest.raises(ValueError, match="secret sources"):
        store.set_source_policy(
            source_id,
            sensitivity="secret",
            model_egress_allowed=True,
            policy_version="v3",
        )


def test_events_are_immutable_and_idempotent(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    source_id, first_id = _source_and_event(store)
    duplicate_id = store.create_event(
        EventInput(
            event_id="different-proposed-id",
            source_id=source_id,
            event_type="message",
            content_text="Leo prefers Traditional Chinese",
            content_fingerprint="event-fingerprint",
            parser_version="parser-v1",
            redactor_version="redactor-v1",
        )
    )

    assert duplicate_id == first_id
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE events SET content_text = 'changed' WHERE event_id = ?", (first_id,)
            )


def test_episode_identity_v2_migration_preserves_ids_candidates_and_revision(
    tmp_path: Path,
) -> None:
    memory_path = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory_path)
    source_id = store.create_source(
        source_type="codex_task",
        source_fingerprint="migration-source",
        source_id="migration-source",
    )
    partition_id = store.create_partition(
        source_id=source_id,
        partition_fingerprint="migration-partition",
        partition_id="migration-partition",
    )
    event_id = store.create_event(
        EventInput(
            source_id=source_id,
            partition_id=partition_id,
            external_event_id="migration-event",
            event_type="message",
            content_text="same words",
            content_fingerprint="content-event",
            parser_version="parser-v1",
            redactor_version="redactor-v1",
            occurred_at="2026-07-20T00:00:00Z",
        )
    )
    episode_id = episode_id_for(
        source_id=source_id,
        partition_id=partition_id,
        event_ids=(event_id,),
        segmenter_version="v1",
    )
    assert (
        store.create_episode(
            EpisodeInput(
                source_id=source_id,
                partition_id=partition_id,
                event_ids=(event_id,),
                episode_type="task",
                content_text="same words",
                content_fingerprint="content-episode",
                segmenter_version="v1",
                segmentation_reason="end_of_input",
                occurred_from="2026-07-20T00:00:00Z",
                occurred_to="2026-07-20T00:00:00Z",
                episode_id=episode_id,
            )
        )
        == episode_id
    )
    candidate_id = store.create_candidate(
        episode_id=episode_id,
        subject_text="Leo",
        predicate="project.state",
        scope="project_state",
        value={"state": "pilot"},
        proposal_type="add",
        confidence=0.9,
        extraction_fingerprint="migration-extraction",
        extractor_version="v1",
        prompt_hash="prompt",
        schema_version="schema",
        model_id="synthetic",
        reasoning_effort="low",
        evidence_event_ids=(event_id,),
    )
    revision_before = store.current_revision()

    downgrade = sqlite3.connect(memory_path)
    try:
        downgrade.execute("PRAGMA foreign_keys = OFF")
        downgrade.execute("DROP TRIGGER episodes_no_update")
        downgrade.execute("DROP TRIGGER candidate_dispositions_no_update")
        downgrade.execute("DROP VIEW current_candidate_dispositions")
        downgrade.execute("DROP TABLE candidate_disposition_events")
        downgrade.execute(
            """
            CREATE TABLE episodes_v1 (
                episode_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
                partition_id TEXT REFERENCES source_partitions(partition_id) ON DELETE CASCADE,
                parent_episode_id TEXT REFERENCES episodes_v1(episode_id) ON DELETE SET NULL,
                episode_type TEXT NOT NULL,
                title TEXT,
                content_text TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL,
                segmenter_version TEXT NOT NULL,
                segmentation_reason TEXT NOT NULL,
                sensitivity TEXT NOT NULL DEFAULT 'normal',
                occurred_from TEXT,
                occurred_to TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(source_id, content_fingerprint, segmenter_version)
            ) STRICT
            """
        )
        downgrade.execute(
            """
            INSERT INTO episodes_v1(
                episode_id, source_id, partition_id, parent_episode_id,
                episode_type, title, content_text, content_fingerprint,
                segmenter_version, segmentation_reason, sensitivity,
                occurred_from, occurred_to, created_at
            )
            SELECT episode_id, source_id, partition_id, parent_episode_id,
                   episode_type, title, content_text, content_fingerprint,
                   segmenter_version, segmentation_reason, sensitivity,
                   occurred_from, occurred_to, created_at
            FROM episodes
            """
        )
        downgrade.execute("DROP TABLE episodes")
        downgrade.execute("ALTER TABLE episodes_v1 RENAME TO episodes")
        downgrade.execute("UPDATE metadata SET value = '1' WHERE key = 'schema_version'")
        downgrade.commit()
    finally:
        downgrade.close()

    mismatched_path = tmp_path / "mismatched-episode-id.sqlite3"
    online_backup(memory_path, mismatched_path)
    mismatched = sqlite3.connect(mismatched_path)
    try:
        mismatched.execute("PRAGMA foreign_keys = OFF")
        mismatched.execute("UPDATE episode_events SET episode_id = 'ep_requires_explicit_mapping'")
        mismatched.execute(
            "UPDATE candidate_claims SET episode_id = 'ep_requires_explicit_mapping'"
        )
        mismatched.execute("UPDATE episodes SET episode_id = 'ep_requires_explicit_mapping'")
        mismatched.commit()
    finally:
        mismatched.close()

    with pytest.raises(DatabaseError, match="explicit migration mapping"):
        MemoryStore(mismatched_path)

    migrated = MemoryStore(memory_path)
    with migrated.connection() as connection:
        episode = connection.execute(
            "SELECT * FROM episodes WHERE episode_id = ?", (episode_id,)
        ).fetchone()
        candidate = connection.execute(
            "SELECT episode_id FROM candidate_claims WHERE candidate_id = ?", (candidate_id,)
        ).fetchone()
        linked_events = connection.execute(
            "SELECT event_id FROM episode_events WHERE episode_id = ? ORDER BY ordinal",
            (episode_id,),
        ).fetchall()
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert (
            connection.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()[0]
            == "4"
        )
        disposition = connection.execute(
            """
            SELECT disposition, reason_code FROM current_candidate_dispositions
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()
    assert episode is not None
    assert episode["event_sequence_fingerprint"] == event_sequence_fingerprint((event_id,))
    assert candidate is not None and candidate["episode_id"] == episode_id
    assert tuple(disposition) == ("suppressed", "schema_v3_legacy_suppression")
    assert [row["event_id"] for row in linked_events] == [event_id]
    assert migrated.current_revision() == revision_before


def test_secret_event_accepts_only_redacted_context(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    source_id = store.create_source(
        source_type="manual", source_fingerprint="manual", source_id="manual"
    )

    event_id = store.create_event(
        EventInput(
            source_id=source_id,
            external_event_id="first-secret",
            event_type="note",
            content_text="token below\n[REDACTED_SECRET]",
            content_fingerprint="redacted",
            parser_version="v1",
            redactor_version="v1",
            sensitivity="secret",
        )
    )
    assert event_id.startswith("evt_")
    second_id = store.create_event(
        EventInput(
            source_id=source_id,
            external_event_id="second-secret",
            event_type="note",
            content_text="another token\n[REDACTED_SECRET]",
            content_fingerprint="redacted",
            parser_version="v1",
            redactor_version="v1",
            sensitivity="secret",
        )
    )
    assert second_id != event_id
    with pytest.raises(ValueError, match="REDACTED_SECRET"):
        store.create_event(
            EventInput(
                source_id=source_id,
                event_type="note",
                content_text="raw-looking secret",
                content_fingerprint="raw",
                parser_version="v1",
                redactor_version="v1",
                sensitivity="secret",
            )
        )


def test_claim_versions_are_bitemporal_and_current_heads_use_relations(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    first_claim, first_version, first_revision = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preferred_language",
            scope="user_profile",
            value={"language": "Traditional Chinese"},
            summary="Leo prefers Traditional Chinese.",
            valid_from="2026-01-01",
        )
    )
    same_claim, second_version, second_revision = store.add_claim_version(
        ClaimVersionInput(
            subject_text="leo",
            predicate="preferred_language",
            scope="user_profile",
            value={"language": "Traditional Chinese (Taiwan)"},
            summary="Leo prefers Traditional Chinese as used in Taiwan.",
            valid_from="2026-07-19",
            supersedes=(first_version,),
        )
    )

    assert same_claim == first_claim
    assert first_revision == 1
    assert second_revision == 2
    assert store.head_version_ids(first_claim) == [second_version]
    assert store.search("Taiwan")[0]["claim_version_id"] == second_version
    assert store.search("prefers") != []
    with store.connection() as connection:
        old = connection.execute(
            "SELECT recorded_revision, valid_from FROM claim_versions WHERE claim_version_id = ?",
            (first_version,),
        ).fetchone()
        assert tuple(old) == (1, "2026-01-01")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE claim_versions SET summary = 'rewrite' WHERE claim_version_id = ?",
                (first_version,),
            )


def test_canonical_claim_rejects_secret_in_value_or_summary(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")

    with pytest.raises(ValueError, match="secret content"):
        store.add_claim_version(
            ClaimVersionInput(
                subject_text="Leo",
                predicate="credential",
                scope="other",
                value={"token": "abcdefghijklmnop"},
                summary="Never persist credentials.",
            )
        )
    with pytest.raises(ValueError, match="secret content"):
        store.add_claim_version(
            ClaimVersionInput(
                subject_text="Leo",
                predicate="credential",
                scope="other",
                value="[REDACTED_SECRET]",
                summary="A redacted marker is still not canonical memory.",
            )
        )


def test_claim_pin_is_append_only_and_revisioned(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    claim_id, _, initial_revision = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project.focus",
            scope="project_state",
            value="Local-Dreaming",
            summary="Local-Dreaming is active.",
        )
    )

    changed, pin_revision = store.set_claim_pin(claim_id)
    duplicate_changed, duplicate_revision = store.set_claim_pin(claim_id)
    unpinned, unpin_revision = store.set_claim_pin(claim_id, pinned=False)

    assert changed is True
    assert duplicate_changed is False
    assert unpinned is True
    assert (initial_revision, pin_revision, duplicate_revision, unpin_revision) == (1, 2, 2, 3)
    assert store.pinned_claim_ids() == ()
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM claim_pin_events").fetchone()[0] == 2


def test_review_storage_uses_shared_multi_head_fingerprint(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    claim_id, first, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="possible_location",
            scope="project_state",
            value="Perth",
            summary="Perth is one possible location.",
            status="disputed",
        )
    )
    _, second, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="possible_location",
            scope="project_state",
            value="Sydney",
            summary="Sydney is another possible location.",
            status="disputed",
            contradicts=(first,),
        )
    )
    with store.connection() as connection:
        identity = connection.execute(
            "SELECT identity_fingerprint FROM claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()[0]
    heads = store.load_heads(str(identity))
    assert {head.claim_version_id for head in heads} == {first, second}
    expected = fingerprint_head_set(heads)
    assert store.head_set_hash(claim_id) == expected

    batch_id = store.create_review_batch(
        [
            ReviewProposalInput(
                proposal_type="update",
                target_slot_fingerprint="slot",
                expected_head_set_hash=expected,
                precondition_hash="precondition",
                proposal_payload_fingerprint="payload",
                evidence_set_fingerprint="evidence",
                payload={"summary": "choice pending"},
                target_claim_id=claim_id,
            )
        ]
    )
    loaded = store.load_review_batch(batch_id)
    assert loaded["status"] == "pending"
    assert loaded["proposals"][0]["payload"] == {"summary": "choice pending"}


def test_online_backup_and_role_checked_atomic_restore(tmp_path: Path) -> None:
    live = tmp_path / "memory.sqlite3"
    backup = tmp_path / "snapshot.sqlite3"
    store = MemoryStore(live)
    _source_and_event(store)
    online_backup(live, backup)

    with store.connection() as connection:
        connection.execute("DELETE FROM sources")
    restore_database(backup, live, expected_role="memory")
    with connect_memory(live) as restored:
        assert restored.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert get_memory_revision(restored) == 0

    with pytest.raises(DatabaseError, match="Expected 'operations'"):
        restore_database(backup, tmp_path / "wrong.sqlite3", expected_role="operations")


def test_first_connection_after_restore_serializes_wal_setup_across_processes(
    tmp_path: Path,
) -> None:
    live = tmp_path / "memory.sqlite3"
    backup = tmp_path / "snapshot.sqlite3"
    MemoryStore(live)
    online_backup(live, backup)
    restore_database(backup, live, expected_role="memory")
    with sqlite3.connect(live) as raw_connection:
        assert raw_connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"

    context = mp.get_context("fork")
    gate = context.Event()
    attempted = context.Event()
    process = context.Process(
        target=_connect_memory_in_process,
        args=(str(live), gate, attempted),
    )
    process.start()

    lock_path = live.parent / f".{live.name}.connection.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        gate.set()
        assert attempted.wait(timeout=5)
        process.join(timeout=0.25)
        assert process.is_alive(), "connection setup bypassed the cross-process lock"
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    process.join(timeout=5)
    assert process.exitcode == 0


def test_operations_transactions_use_reentrant_cross_process_maintenance_lock(
    tmp_path: Path,
) -> None:
    live = tmp_path / "operations.sqlite3"
    store = OperationsStore(live)
    with store.maintenance_lock():
        store.enqueue_job(job_type="test", dedupe_key="reentrant-transaction")
        with store.connection() as reader:
            assert reader.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1

    context = mp.get_context("fork")
    gate = context.Event()
    attempted = context.Event()
    process = context.Process(
        target=_enqueue_operations_in_process,
        args=(str(live), gate, attempted),
    )
    process.start()

    lock_path = operations_maintenance_lock_path(live)
    descriptor = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        gate.set()
        assert attempted.wait(timeout=5)
        process.join(timeout=0.25)
        assert process.is_alive(), "operations transaction bypassed the maintenance lock"
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    process.join(timeout=5)
    assert process.exitcode == 0
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2


def test_operations_initialization_holds_maintenance_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live = tmp_path / "operations.sqlite3"
    original_initialize = storage_module.initialize_operations
    observed_locked = False

    def checked_initialize(path: str | Path) -> None:
        nonlocal observed_locked
        descriptor = os.open(operations_maintenance_lock_path(path), os.O_RDWR)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                observed_locked = True
            else:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                raise AssertionError("operations initialization ran outside maintenance lock")
        finally:
            os.close(descriptor)
        original_initialize(path)

    monkeypatch.setattr(storage_module, "initialize_operations", checked_initialize)
    OperationsStore(live)
    assert observed_locked


def test_job_leases_are_owned_and_payload_is_round_tripped(tmp_path: Path) -> None:
    store = OperationsStore(tmp_path / "operations.sqlite3")
    job_id = store.enqueue_job(
        job_type="extract",
        dedupe_key="episode:1:extractor:v1",
        payload={"episode_id": "episode-1"},
    )

    leased = store.lease_job(owner="worker-1")
    assert leased is not None
    assert leased["job_id"] == job_id
    assert leased["payload"] == {"episode_id": "episode-1"}
    assert not store.complete_job(job_id, owner="worker-2")
    assert store.complete_job(job_id, owner="worker-1")


def test_expired_final_attempt_lease_becomes_failed(tmp_path: Path) -> None:
    store = OperationsStore(tmp_path / "operations.sqlite3")
    job_id = store.enqueue_job(
        job_type="extract",
        dedupe_key="episode:final-attempt",
        max_attempts=1,
    )

    assert store.lease_job(owner="worker-1", lease_seconds=0) is not None
    assert store.lease_job(owner="worker-2") is None

    with store.connection() as connection:
        row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    assert row is not None
    assert row["status"] == "failed"
    assert row["attempts"] == row["max_attempts"] == 1
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert row["last_error"] == "lease expired after final permitted attempt"


def test_exact_job_lease_does_not_recover_unrelated_expired_rows(tmp_path: Path) -> None:
    store = OperationsStore(tmp_path / "operations.sqlite3")
    foreign_job = store.enqueue_job(
        job_type="phase1",
        dedupe_key="foreign-expired-job",
    )
    selected_job = store.enqueue_job(
        job_type="phase1",
        dedupe_key="selected-pilot-job",
    )
    assert (
        store.lease_job(
            owner="foreign-worker",
            lease_seconds=0,
            job_ids=(foreign_job,),
        )
        is not None
    )
    with store.connection() as connection:
        foreign_before = dict(
            connection.execute("SELECT * FROM jobs WHERE job_id = ?", (foreign_job,)).fetchone()
        )

    selected = store.lease_job(owner="pilot-worker", job_ids=(selected_job,))

    assert selected is not None
    assert selected["job_id"] == selected_job
    with store.connection() as connection:
        foreign_after = dict(
            connection.execute("SELECT * FROM jobs WHERE job_id = ?", (foreign_job,)).fetchone()
        )
    assert foreign_after == foreign_before


def test_operations_usage_does_not_create_memory_revisions(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    call_id = operations.create_model_call(
        phase="phase1",
        source_ids=("source-b", "source-a", "source-a"),
        maximum_sensitivity="normal",
        input_bytes=120,
        redaction_count=2,
        model_id="gpt-5.6-sol",
        reasoning_effort="medium",
        prompt_hash="prompt-v1",
        schema_version="schema-v1",
    )
    operations.finish_model_call(call_id, status="completed", input_tokens=20, output_tokens=5)
    run_id = operations.start_nightly_run()
    operations.add_nightly_usage(
        run_id,
        scan_bytes=120,
        episode_count=1,
        model_calls=1,
        input_tokens=20,
        output_tokens=5,
    )
    operations.finish_nightly_run(run_id, status="completed")
    operations.record_retrieval_usage(
        tool_name="memory_search",
        memory_revision=memory.current_revision(),
        result_count=1,
        returned_bytes=80,
        maximum_sensitivity="normal",
    )

    assert memory.current_revision() == 0
    with operations.connection() as connection:
        call = connection.execute(
            "SELECT source_ids_json, status FROM model_calls WHERE call_id = ?", (call_id,)
        ).fetchone()
        assert call[0] == '["source-a","source-b"]'
        assert call[1] == "completed"
        run = connection.execute(
            "SELECT status, input_tokens FROM nightly_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        assert tuple(run) == ("completed", 20)

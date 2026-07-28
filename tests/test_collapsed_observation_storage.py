from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from local_dreaming.cli import _persist_scan_result
from local_dreaming.config import RuntimePaths, Settings
from local_dreaming.source_adapters import CodexSessionAdapter
from local_dreaming.storage import ClaimVersionInput, MemoryStore


def _session_records(session_id: str) -> list[dict[str, object]]:
    return [
        {
            "timestamp": "2026-07-20T03:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "timestamp": "2026-07-20T03:00:00Z",
            },
        },
        {
            "timestamp": "2026-07-20T03:01:00.001Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": f"{session_id}-primary",
                "role": "user",
                "content": [{"type": "input_text", "text": "same bounded request"}],
            },
        },
        {
            "timestamp": "2026-07-20T03:01:00.010Z",
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "client_id": f"{session_id}-duplicate",
                "message": "same bounded request",
            },
        },
    ]


def _write_session(path: Path, session_id: str) -> None:
    path.write_text(
        "\n".join(json.dumps(record, sort_keys=True) for record in _session_records(session_id))
        + "\n",
        encoding="utf-8",
    )


def test_collapsed_observation_replay_is_idempotent(tmp_path: Path) -> None:
    session = tmp_path / "session.jsonl"
    _write_session(session, "synthetic-replay-session")
    settings = Settings(paths=RuntimePaths(home=tmp_path / "runtime"))
    scan = CodexSessionAdapter([session], allow_model_egress=True).scan()

    first = _persist_scan_result(scan, settings=settings, dry_run=False)
    second = _persist_scan_result(scan, settings=settings, dry_run=False)

    assert first["event_ids"] == second["event_ids"]
    assert first["collapsed_observation_ids"] == second["collapsed_observation_ids"]
    memory = MemoryStore(settings.paths.memory_db, initialize=False)
    with memory.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert (
            connection.execute("SELECT COUNT(*) FROM event_collapsed_observations").fetchone()[0]
            == 1
        )
    audit = memory.get_event_observation_audit(first["event_ids"][0])
    assert audit.collapsed_observation_count == 1
    assert audit.total_observation_count == 2
    assert memory.current_revision() == 0


def test_same_text_in_different_sources_keeps_provenance_isolated(tmp_path: Path) -> None:
    first_path = tmp_path / "first.jsonl"
    second_path = tmp_path / "second.jsonl"
    _write_session(first_path, "synthetic-source-one")
    _write_session(second_path, "synthetic-source-two")
    settings = Settings(paths=RuntimePaths(home=tmp_path / "runtime"))

    scan = CodexSessionAdapter([first_path, second_path], allow_model_egress=True).scan()
    persisted = _persist_scan_result(scan, settings=settings, dry_run=False)

    assert persisted["events"] == 2
    assert persisted["collapsed_observations"] == 2
    memory = MemoryStore(settings.paths.memory_db, initialize=False)
    with memory.connection() as connection:
        rows = connection.execute(
            """
            SELECT observation.source_id AS observation_source,
                   event.source_id AS event_source,
                   observation.partition_id AS observation_partition,
                   event.partition_id AS event_partition
            FROM event_collapsed_observations AS observation
            JOIN events AS event ON event.event_id = observation.event_id
            ORDER BY observation.source_id
            """
        ).fetchall()
    assert len(rows) == 2
    assert len({str(row["observation_source"]) for row in rows}) == 2
    assert all(row["observation_source"] == row["event_source"] for row in rows)
    assert all(row["observation_partition"] == row["event_partition"] for row in rows)


def test_schema_v4_sidecar_migration_preserves_canonical_state(tmp_path: Path) -> None:
    memory_path = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory_path)
    claim_id, first_version, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project_state",
            scope="project_state",
            value={"state": "first"},
            summary="First synthetic state.",
        )
    )
    _, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project_state",
            scope="project_state",
            value={"state": "second"},
            summary="Second synthetic state.",
            supersedes=(first_version,),
        )
    )
    revision_before = store.current_revision()
    with store.connection() as connection:
        claims_before = connection.execute("SELECT * FROM claims ORDER BY claim_id").fetchall()
        versions_before = connection.execute(
            "SELECT * FROM claim_versions ORDER BY claim_version_id"
        ).fetchall()
        relations_before = connection.execute(
            "SELECT * FROM claim_relations ORDER BY relation_id"
        ).fetchall()

    downgrade = sqlite3.connect(memory_path)
    try:
        downgrade.execute("DROP TABLE event_collapsed_observations")
        downgrade.execute("UPDATE metadata SET value = '3' WHERE key = 'schema_version'")
        downgrade.commit()
    finally:
        downgrade.close()

    migrated = MemoryStore(memory_path)
    with migrated.connection() as connection:
        schema_version = connection.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        claims_after = connection.execute("SELECT * FROM claims ORDER BY claim_id").fetchall()
        versions_after = connection.execute(
            "SELECT * FROM claim_versions ORDER BY claim_version_id"
        ).fetchall()
        relations_after = connection.execute(
            "SELECT * FROM claim_relations ORDER BY relation_id"
        ).fetchall()
        sidecar_exists = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'event_collapsed_observations'
            """
        ).fetchone()

    assert schema_version == "4"
    assert sidecar_exists is not None
    assert [tuple(row) for row in claims_after] == [tuple(row) for row in claims_before]
    assert [tuple(row) for row in versions_after] == [tuple(row) for row in versions_before]
    assert [tuple(row) for row in relations_after] == [tuple(row) for row in relations_before]
    assert migrated.current_revision() == revision_before
    assert claim_id

from __future__ import annotations

import json
from pathlib import Path

from local_dreaming.cli import _persist_scan_result
from local_dreaming.config import RuntimePaths, Settings
from local_dreaming.source_adapters import CodexSessionAdapter, ScanLimits
from local_dreaming.storage import MemoryStore, OperationsStore


def test_cross_cursor_duplicate_updates_persisted_audit_provenance(tmp_path: Path) -> None:
    """A duplicate discovered after cursor commit must remain auditable in storage."""

    session = tmp_path / "cross-cursor-duplicate.jsonl"
    records = [
        {
            "timestamp": "2026-07-20T03:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": "synthetic-cross-cursor-session",
                "timestamp": "2026-07-20T03:00:00Z",
            },
        },
        {
            "timestamp": "2026-07-20T03:01:00.001Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "id": "synthetic-primary",
                "role": "user",
                "content": [{"type": "input_text", "text": "same bounded request"}],
            },
        },
        {
            "timestamp": "2026-07-20T03:01:00.010Z",
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "client_id": "synthetic-duplicate",
                "message": "same bounded request",
            },
        },
    ]
    lines = [json.dumps(record, ensure_ascii=False) for record in records]
    session.write_text("\n".join(lines) + "\n", encoding="utf-8")
    first_batch_bytes = len((lines[0] + "\n" + lines[1] + "\n").encode())

    runtime_home = tmp_path / "runtime"
    settings = Settings(paths=RuntimePaths(home=runtime_home))
    adapter = CodexSessionAdapter(
        [session],
        limits=ScanLimits(
            max_file_bytes=first_batch_bytes,
            max_total_bytes=first_batch_bytes * 2,
            max_record_bytes=4096,
        ),
        allow_model_egress=True,
    )

    first = adapter.scan()
    first_persisted = _persist_scan_result(first, settings=settings, dry_run=False)
    assert first_persisted["events"] == 1

    operations = OperationsStore(settings.paths.operations_db, initialize=False)
    second = adapter.scan(cursor_loader=operations.get_cursor)
    assert second.events == ()
    assert any(item.code == "adjacent_duplicate_collapsed" for item in second.diagnostics)
    _persist_scan_result(second, settings=settings, dry_run=False)

    memory = MemoryStore(settings.paths.memory_db, initialize=False)
    with memory.connection() as connection:
        event_row = connection.execute("SELECT event_id, metadata_json FROM events").fetchone()
    assert event_row is not None
    metadata = json.loads(str(event_row["metadata_json"]))
    assert metadata["collapsed_observation_count"] == 1
    audit = memory.get_event_observation_audit(str(event_row["event_id"]))
    assert audit.collapsed_observation_count == 1
    assert audit.total_observation_count == 2
    assert audit.collapsed_provenance_fingerprint

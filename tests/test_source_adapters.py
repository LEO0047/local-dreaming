from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from local_dreaming.ingest import prepare_event
from local_dreaming.models import ClaimScope, Sensitivity, SourceKind
from local_dreaming.redaction import REDACTED_SECRET
from local_dreaming.source_adapters import (
    AdvisoryMarkdownAdapter,
    ChronicleSummaryAdapter,
    CodexSessionAdapter,
    OperationalSnapshotKind,
    ScanLimits,
    build_automation_snapshot,
    capture_health_snapshot,
    capture_workspace_snapshot,
    extract_terminal_handoff,
)

NOW = datetime(2026, 7, 20, 3, 30, tzinfo=UTC)


def _write_jsonl(path: Path, records: list[object]) -> None:
    path.write_text(
        "\n".join(
            item if isinstance(item, str) else json.dumps(item, ensure_ascii=False)
            for item in records
        ),
        encoding="utf-8",
    )


def _session_meta() -> dict[str, object]:
    return {
        "timestamp": "2026-07-20T03:00:00Z",
        "type": "session_meta",
        "payload": {"id": "synthetic-session", "timestamp": "2026-07-20T03:00:00Z"},
    }


def _message(role: str, text: str, *, phase: str | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "message",
        "role": role,
        "content": [{"type": "output_text", "text": text}],
    }
    if phase is not None:
        payload["phase"] = phase
    return {
        "timestamp": "2026-07-20T03:01:00Z",
        "type": "response_item",
        "payload": payload,
    }


def _handoff() -> str:
    return """Task completed.

[DREAMING_HANDOFF]
workspace: /synthetic/project
state: in_progress
completed: adapter
verified: focused tests
leo_corrections: none
pending: integration
[/DREAMING_HANDOFF]"""


def test_codex_session_scan_is_stable_and_extracts_trust_classes(tmp_path: Path) -> None:
    session = tmp_path / "rollout.jsonl"
    _write_jsonl(
        session,
        [
            _session_meta(),
            {
                "timestamp": "2026-07-20T03:01:00Z",
                "type": "event_msg",
                "payload": {"type": "user_message", "message": "Please continue."},
            },
            _message("user", "Please continue."),
            _message("assistant", "intermediate", phase="commentary"),
            {
                "timestamp": "2026-07-20T03:02:00Z",
                "type": "response_item",
                "payload": {
                    "type": "function_call_output",
                    "call_id": "synthetic-call",
                    "output": "pytest passed",
                },
            },
            _message("assistant", _handoff(), phase="final_answer"),
        ],
    )

    adapter = CodexSessionAdapter([session], allow_model_egress=True)
    first = adapter.scan()
    second = adapter.scan()

    assert first == second
    assert first.files_scanned == 1
    assert [event.source_kind for event in first.events] == [
        SourceKind.CODEX_TASK,
        SourceKind.TOOL_RESULT,
        SourceKind.ASSISTANT_FINAL,
        SourceKind.DREAMING_HANDOFF,
    ]
    assert first.events[-1].metadata["claim_scope"] == "project_state"
    assert first.events[-1].content.startswith("[DREAMING_HANDOFF]")
    assistant_final = next(
        event for event in first.events if event.source_kind is SourceKind.ASSISTANT_FINAL
    )
    assert assistant_final.content == "Task completed."
    assert "[DREAMING_HANDOFF]" not in assistant_final.content
    assert all(source.policy().opted_in for source in first.sources)
    assert all(source.policy().allow_model_egress for source in first.sources)
    policies = {source.source_id: source.policy() for source in first.sources}
    assert all(prepare_event(event, policies[event.source_id]) for event in first.events)


def test_codex_collapses_adjacent_dual_user_observations_with_audit_metadata(
    tmp_path: Path,
) -> None:
    session = tmp_path / "dual-observation.jsonl"
    _write_jsonl(
        session,
        [
            _session_meta(),
            {
                "timestamp": "2026-07-20T03:01:00.001Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "id": "synthetic-message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "same request"}],
                },
            },
            {
                "timestamp": "2026-07-20T03:01:00.018Z",
                "type": "event_msg",
                "payload": {
                    "type": "user_message",
                    "client_id": "synthetic-client",
                    "message": "same request",
                },
            },
        ],
    )

    result = CodexSessionAdapter([session], allow_model_egress=True).scan()

    assert [event.content for event in result.events] == ["same request"]
    assert result.events[0].metadata["collapsed_observation_count"] == 2
    assert "source_lineage_fingerprint" in result.events[0].metadata
    assert "collapsed_provenance_fingerprint" in result.events[0].metadata
    assert any(item.code == "adjacent_duplicate_collapsed" for item in result.diagnostics)


def test_codex_same_text_from_different_sessions_is_not_merged(tmp_path: Path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    first_meta = _session_meta()
    second_meta = _session_meta()
    second_meta["payload"] = {
        "id": "synthetic-session-two",
        "timestamp": "2026-07-20T03:00:00Z",
    }
    _write_jsonl(first, [first_meta, _message("user", "same text")])
    _write_jsonl(second, [second_meta, _message("user", "same text")])

    result = CodexSessionAdapter([first, second], allow_model_egress=True).scan()

    assert len(result.events) == 2
    assert len({event.source_id for event in result.events}) == 2


def test_codex_private_policy_is_explicit_and_applied_to_events(tmp_path: Path) -> None:
    session = tmp_path / "private.jsonl"
    _write_jsonl(session, [_session_meta(), _message("user", "private context")])

    result = CodexSessionAdapter(
        [session],
        sensitivity=Sensitivity.PRIVATE,
        allow_private_model_egress=True,
    ).scan()

    assert result.events[0].sensitivity is Sensitivity.PRIVATE
    assert result.sources[0].sensitivity is Sensitivity.PRIVATE
    assert result.sources[0].policy().allow_private_model_egress
    assert result.sources[0].policy().permits_model_egress(Sensitivity.PRIVATE)


def test_adapter_egress_configuration_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"

    with pytest.raises(ValueError, match="secret adapter sources"):
        CodexSessionAdapter(
            [source],
            sensitivity=Sensitivity.SECRET,
            allow_model_egress=True,
        )
    with pytest.raises(ValueError, match="private model egress requires private"):
        AdvisoryMarkdownAdapter(
            [tmp_path / "summary.md"],
            source_kind=SourceKind.CHRONICLE,
            namespace="test",
            allow_private_model_egress=True,
        )


def test_pure_terminal_handoff_is_not_duplicated_as_assistant_final(tmp_path: Path) -> None:
    session = tmp_path / "handoff-only.jsonl"
    handoff_only = _handoff()[_handoff().index("[DREAMING_HANDOFF]") :]
    _write_jsonl(session, [_session_meta(), _message("assistant", handoff_only)])

    result = CodexSessionAdapter([session]).scan()

    assert [event.source_kind for event in result.events] == [SourceKind.DREAMING_HANDOFF]
    assert "[DREAMING_HANDOFF]" in result.events[0].content


def test_codex_malformed_records_are_skipped_with_safe_diagnostics(tmp_path: Path) -> None:
    session = tmp_path / "rollout.jsonl"
    _write_jsonl(
        session,
        [
            "{bad json",
            [],
            {
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"text": "missing timestamp"}],
                },
            },
        ],
    )

    result = CodexSessionAdapter([session]).scan()

    assert not result.events
    assert [diagnostic.code for diagnostic in result.diagnostics] == [
        "malformed_json",
        "malformed_record",
        "missing_timestamp",
    ]


def test_codex_adapter_redacts_secret_before_returning_records(tmp_path: Path) -> None:
    session = tmp_path / "rollout.jsonl"
    secret = "abcdefghijklmnop"
    _write_jsonl(session, [_session_meta(), _message("user", f"token={secret}")])

    result = CodexSessionAdapter([session]).scan()

    assert result.events[0].content == REDACTED_SECRET
    assert result.events[0].sensitivity is Sensitivity.SECRET
    assert secret not in repr(result)


def test_file_and_record_limits_fail_bounded(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.jsonl"
    oversized.write_text("x" * 200, encoding="utf-8")
    result = CodexSessionAdapter(
        [oversized],
        limits=ScanLimits(max_file_bytes=64, max_total_bytes=128, max_record_bytes=32),
    ).scan()
    assert not result.events
    assert result.diagnostics[0].code == "oversized_record_skipped"
    assert result.bytes_scanned <= 64
    assert result.cursor_updates

    bounded = tmp_path / "bounded.jsonl"
    _write_jsonl(bounded, [_session_meta(), _message("user", "one"), _message("user", "two")])
    record_result = CodexSessionAdapter([bounded], limits=ScanLimits(max_records_per_file=2)).scan()
    assert len(record_result.events) == 1
    assert record_result.truncated
    assert any(item.code == "record_limit_reached" for item in record_result.diagnostics)


def test_codex_stream_cursor_resumes_without_replaying_prior_records(tmp_path: Path) -> None:
    session = tmp_path / "streamed.jsonl"
    lines = [
        json.dumps(_session_meta(), ensure_ascii=False),
        json.dumps(_message("user", "first retained message"), ensure_ascii=False),
        json.dumps(_message("user", "second retained message"), ensure_ascii=False),
    ]
    session.write_text("\n".join(lines) + "\n", encoding="utf-8")
    first_batch_bytes = len((lines[0] + "\n" + lines[1] + "\n").encode())
    limits = ScanLimits(
        max_file_bytes=first_batch_bytes,
        max_total_bytes=first_batch_bytes * 2,
        max_record_bytes=4096,
    )
    adapter = CodexSessionAdapter([session], limits=limits)

    first = adapter.scan()
    crash_replay = adapter.scan()
    cursors = {item.name: item.value for item in first.cursor_updates}
    second = adapter.scan(cursor_loader=cursors.get)

    assert [event.content for event in first.events] == ["first retained message"]
    assert crash_replay.events == first.events
    assert [event.content for event in second.events] == ["second retained message"]
    assert first.events[0].source_id == second.events[0].source_id
    assert first.bytes_scanned <= first_batch_bytes
    assert second.bytes_scanned <= first_batch_bytes
    assert not second.truncated


def test_codex_directory_batches_skip_completed_eof_cursors(tmp_path: Path) -> None:
    for index, name in enumerate(("a.jsonl", "b.jsonl", "c.jsonl"), start=1):
        _write_jsonl(
            tmp_path / name,
            [_session_meta(), _message("user", f"retained batch {index}")],
        )
    adapter = CodexSessionAdapter(
        [tmp_path],
        limits=ScanLimits(max_files=1, max_file_bytes=4096, max_total_bytes=4096),
    )
    cursors: dict[str, str] = {}
    observed: list[str] = []

    for _ in range(3):
        result = adapter.scan(cursor_loader=cursors.get)
        observed.extend(event.content for event in result.events)
        cursors.update({item.name: item.value for item in result.cursor_updates})

    final = adapter.scan(cursor_loader=cursors.get)
    assert observed == ["retained batch 1", "retained batch 2", "retained batch 3"]
    assert final.files_scanned == 0
    assert final.events == ()
    assert not final.truncated


def test_codex_cursor_collapses_duplicate_split_across_scan_batches(tmp_path: Path) -> None:
    session = tmp_path / "cursor-duplicate.jsonl"
    response = {
        "timestamp": "2026-07-20T03:01:00.001Z",
        "type": "response_item",
        "payload": {
            "type": "message",
            "id": "synthetic-message",
            "role": "user",
            "content": [{"type": "input_text", "text": "bounded duplicate"}],
        },
    }
    event = {
        "timestamp": "2026-07-20T03:01:00.010Z",
        "type": "event_msg",
        "payload": {"type": "user_message", "message": "bounded duplicate"},
    }
    lines = [
        json.dumps(_session_meta(), ensure_ascii=False),
        json.dumps(response, ensure_ascii=False),
        json.dumps(event, ensure_ascii=False),
    ]
    session.write_text("\n".join(lines) + "\n", encoding="utf-8")
    first_batch_bytes = len((lines[0] + "\n" + lines[1] + "\n").encode())
    adapter = CodexSessionAdapter(
        [session],
        limits=ScanLimits(
            max_file_bytes=first_batch_bytes,
            max_total_bytes=first_batch_bytes * 2,
            max_record_bytes=4096,
        ),
    )

    first_result = adapter.scan()
    cursors = {item.name: item.value for item in first_result.cursor_updates}
    second_result = adapter.scan(cursor_loader=cursors.get)

    assert [item.content for item in first_result.events] == ["bounded duplicate"]
    assert second_result.events == ()
    assert any(item.code == "adjacent_duplicate_collapsed" for item in second_result.diagnostics)
    assert "bounded duplicate" not in repr(cursors)


def test_streaming_discards_large_image_payload_without_copying_it(tmp_path: Path) -> None:
    session = tmp_path / "large-image.jsonl"
    image_payload = "A" * 12_000
    image_record = {
        "timestamp": "2026-07-20T03:01:30Z",
        "type": "response_item",
        "payload": {
            "type": "function_call_output",
            "call_id": "image-call",
            "output": {"image": {"data": image_payload, "mime_type": "image/png"}},
        },
    }
    lines = [
        json.dumps(_session_meta(), ensure_ascii=False),
        json.dumps(image_record, ensure_ascii=False),
        json.dumps(_message("user", "safe after image"), ensure_ascii=False),
    ]
    session.write_text("\n".join(lines) + "\n", encoding="utf-8")
    adapter = CodexSessionAdapter(
        [session],
        limits=ScanLimits(
            max_file_bytes=2048,
            max_total_bytes=2048,
            max_record_bytes=512,
        ),
    )
    cursor_values: dict[str, str] = {}
    observed_events = []
    observed_diagnostics = []
    for _ in range(10):
        result = adapter.scan(cursor_loader=cursor_values.get)
        observed_events.extend(result.events)
        observed_diagnostics.extend(result.diagnostics)
        cursor_values.update({item.name: item.value for item in result.cursor_updates})
        if not result.truncated:
            break

    assert [event.content for event in observed_events] == ["safe after image"]
    assert any(item.code == "oversized_record_skipped" for item in observed_diagnostics)
    assert image_payload not in repr(observed_events)
    assert image_payload not in repr(cursor_values)


def test_chronicle_reads_only_explicit_persisted_markdown_as_advisory(tmp_path: Path) -> None:
    summaries = tmp_path / "resources"
    summaries.mkdir()
    (summaries / "summary.md").write_text(
        "---\ncreated_at: 2026-07-19T22:10:00+08:00\n---\n# Persisted summary\nProject note.",
        encoding="utf-8",
    )
    (summaries / "raw-screen.txt").write_text("ephemeral OCR", encoding="utf-8")

    result = ChronicleSummaryAdapter([summaries]).scan()

    assert len(result.events) == 1
    assert result.events[0].source_kind is SourceKind.CHRONICLE
    assert result.events[0].metadata["persisted_summary_only"] is True
    assert result.sources[0].trust_level == "advisory"
    assert result.sources[0].advisory
    # Any canonical scope is denied by the SourcePolicy trust matrix.
    assert not result.sources[0].policy().permits_claim_scope(ClaimScope.PROJECT_STATE)
    assert result.files_scanned == 1


def test_codex_memory_markdown_is_explicit_and_advisory_only(tmp_path: Path) -> None:
    memory = tmp_path / "memory.md"
    memory.write_text("# Hint only", encoding="utf-8")
    result = AdvisoryMarkdownAdapter(
        [memory],
        source_kind=SourceKind.CODEX_MEMORY,
        namespace="synthetic-built-in-memory",
        default_occurred_at=NOW,
        allow_model_egress=True,
    ).scan()

    assert len(result.events) == 1
    assert result.events[0].metadata["canonical_support"] is False
    assert result.sources[0].trust_level == "advisory"
    assert result.sources[0].policy().allow_model_egress


def test_advisory_without_stable_time_is_skipped(tmp_path: Path) -> None:
    summary = tmp_path / "summary.md"
    summary.write_text("# No time", encoding="utf-8")

    result = ChronicleSummaryAdapter([summary]).scan()

    assert not result.events
    assert result.diagnostics[0].code == "missing_timestamp"


def test_terminal_handoff_adapter_accepts_only_terminal_valid_block() -> None:
    valid = extract_terminal_handoff(_handoff(), task_id="task-1", occurred_at=NOW)
    repeated = extract_terminal_handoff(_handoff(), task_id="task-1", occurred_at=NOW)
    invalid = extract_terminal_handoff(
        _handoff() + "\ntrailing text", task_id="task-1", occurred_at=NOW
    )

    assert valid == repeated
    assert valid.events[0].source_kind is SourceKind.DREAMING_HANDOFF
    assert valid.events[0].metadata["claim_scope"] == "project_state"
    assert not invalid.events
    assert invalid.diagnostics[0].code == "invalid_terminal_handoff"


def test_symlinks_and_credential_named_files_are_not_read(tmp_path: Path) -> None:
    actual = tmp_path / "actual.jsonl"
    _write_jsonl(actual, [_session_meta(), _message("user", "safe")])
    link = tmp_path / "linked.jsonl"
    link.symlink_to(actual)
    credentials = tmp_path / "credentials.jsonl"
    _write_jsonl(credentials, [_session_meta(), _message("user", "must not scan")])

    result = CodexSessionAdapter([link, credentials]).scan()

    assert not result.events
    assert any(item.code == "symlink_skipped" for item in result.diagnostics)
    assert any(item.code == "credential_path_skipped" for item in result.diagnostics)


def test_operational_snapshots_are_stable_and_never_memory_events(tmp_path: Path) -> None:
    workspace = tmp_path / "project"
    (workspace / ".git").mkdir(parents=True)
    (workspace / ".git" / "HEAD").write_text("ref: refs/heads/main\n", encoding="utf-8")

    first = capture_workspace_snapshot(
        workspace, captured_at=NOW, git_dirty_count=2, extra={"disk_free_gb": 46}
    )
    second = capture_workspace_snapshot(
        workspace, captured_at=NOW, git_dirty_count=2, extra={"disk_free_gb": 46}
    )
    automation = build_automation_snapshot(
        automation_type="launchd",
        automation_id="local-dreaming",
        status="ready",
        captured_at=NOW,
    )
    health = capture_health_snapshot(
        {"workspace": workspace, "missing": tmp_path / "missing"}, captured_at=NOW
    )

    assert first == second
    assert first.operations_only
    assert first.snapshot_kind is OperationalSnapshotKind.WORKSPACE
    assert first.payload["git_branch"] == "main"
    assert automation.operations_only
    assert automation.snapshot_kind is OperationalSnapshotKind.AUTOMATION
    assert health.snapshot_kind is OperationalSnapshotKind.HEALTH
    assert health.status == "degraded"
    assert health.payload["available_count"] == 1


def test_operation_payload_redacts_secret_values() -> None:
    secret = "abcdefghijklmnop"
    snapshot = build_automation_snapshot(
        automation_type="launchd",
        automation_id="dreaming",
        status="ready",
        captured_at=NOW,
        payload={"detail": f"token={secret}"},
    )

    assert snapshot.payload["detail"] == REDACTED_SECRET
    assert secret not in repr(snapshot)


def test_naive_operation_timestamp_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        capture_workspace_snapshot(tmp_path, captured_at=datetime(2026, 7, 20))

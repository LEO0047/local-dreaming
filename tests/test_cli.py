from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import site
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

import local_dreaming.cli as cli_module
from local_dreaming.cli import app
from local_dreaming.doctor import CheckStatus, DoctorCheck, DoctorReport
from local_dreaming.models import ProposalType
from local_dreaming.nightly import NightlyReport, NightlyStatus
from local_dreaming.pipeline import BudgetUsage
from local_dreaming.review import fingerprint_head_set, fingerprint_precondition
from local_dreaming.storage import (
    ClaimVersionInput,
    MemoryStore,
    OperationsStore,
    ReviewProposalInput,
    fingerprint,
    identity_fingerprint,
    memory_maintenance_lock_path,
    operations_maintenance_lock_path,
)

runner = CliRunner()


def _env(runtime_home: Path) -> dict[str, str]:
    return {"LOCAL_DREAMING_HOME": str(runtime_home)}


def _record_verified_runs(runtime_home: Path, count: int = 7) -> None:
    operations = OperationsStore(runtime_home / "data" / "operations.sqlite3")
    for _ in range(count):
        run_id = operations.start_nightly_run()
        operations.add_nightly_usage(
            run_id,
            episode_count=1,
            model_calls=1,
            input_tokens=10,
        )
        operations.finish_nightly_run(run_id, status="completed")


def test_init_dry_run_is_non_mutating(runtime_home: Path) -> None:
    result = runner.invoke(app, ["init", "--dry-run", "--json"], env=_env(runtime_home))

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["initialized"] is False
    assert not runtime_home.exists()


def test_failed_live_doctor_writes_marker_and_exits_nonzero(
    runtime_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    report = DoctorReport(
        checks=(
            DoctorCheck(
                name="live_probe_phase1",
                status=CheckStatus.FAIL,
                detail="synthetic failure",
            ),
        ),
        live_probe_requested=True,
        codex_version="codex-cli test",
    )
    monkeypatch.setattr(cli_module, "run_doctor", lambda *args, **kwargs: report)

    result = runner.invoke(app, ["doctor", "--live", "--json"], env=_env(runtime_home))

    assert result.exit_code == 1
    assert json.loads(result.stdout)["failure_written"] is True
    marker = runtime_home / "worker" / "doctor-last-failure.json"
    assert marker.is_file()
    assert marker.stat().st_mode & 0o777 == 0o600


def test_ingest_segment_and_extract_cli(runtime_home: Path, tmp_path: Path) -> None:
    input_path = tmp_path / "note.txt"
    input_path.write_text("Leo prefers Traditional Chinese.", encoding="utf-8")
    assert runner.invoke(app, ["init", "--json"], env=_env(runtime_home)).exit_code == 0

    ingest = runner.invoke(
        app,
        [
            "ingest",
            str(input_path),
            "--source-id",
            "manual-note",
            "--allow-model-egress",
            "--json",
        ],
        env=_env(runtime_home),
    )
    segment = runner.invoke(app, ["segment", "--json"], env=_env(runtime_home))
    extract = runner.invoke(app, ["extract", "--json"], env=_env(runtime_home))

    assert ingest.exit_code == 0, ingest.output
    assert json.loads(ingest.stdout)["events"] == 1
    expected_time = datetime.fromtimestamp(input_path.stat().st_mtime, UTC).isoformat()
    with MemoryStore(runtime_home / "data" / "memory.sqlite3").connection() as connection:
        assert connection.execute("SELECT occurred_at FROM events").fetchone()[0] == expected_time
    assert segment.exit_code == 0, segment.output
    assert json.loads(segment.stdout)["persisted"] == 1
    assert extract.exit_code == 0, extract.output
    assert json.loads(extract.stdout)["eligible"] == 1


def test_codex_session_adapter_cli_registers_generated_source(
    runtime_home: Path, tmp_path: Path
) -> None:
    session = tmp_path / "session.jsonl"
    session.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "timestamp": "2026-07-20T03:00:00Z",
                        "type": "session_meta",
                        "payload": {
                            "id": "synthetic-session",
                            "timestamp": "2026-07-20T03:00:00Z",
                        },
                    }
                ),
                json.dumps(
                    {
                        "timestamp": "2026-07-20T03:01:00Z",
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "output_text", "text": "Continue project."}],
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    result = runner.invoke(
        app,
        [
            "ingest",
            str(session),
            "--adapter",
            "codex-session",
            "--allow-model-egress",
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["events"] == 1
    assert payload["sources"] == 1
    assert len(payload["source_ids"]) == 1
    assert len(payload["partition_ids"]) == 1
    assert payload["cursor_updates"] == 1
    assert payload["records_scanned"] == 2

    resumed = runner.invoke(
        app,
        [
            "ingest",
            str(session),
            "--adapter",
            "codex-session",
            "--allow-model-egress",
            "--json",
        ],
        env=_env(runtime_home),
    )
    assert resumed.exit_code == 0, resumed.output
    assert json.loads(resumed.stdout)["events"] == 0
    with MemoryStore(runtime_home / "data" / "memory.sqlite3").connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_anonymous_multi_source_session_with_repeated_episode_content_segments(
    runtime_home: Path, tmp_path: Path
) -> None:
    session = tmp_path / "anonymous-multi-source.jsonl"
    records = [
        {
            "timestamp": "2026-07-20T00:00:00Z",
            "type": "session_meta",
            "payload": {"id": "anonymous-multi-source"},
        },
        {
            "timestamp": "2026-07-20T00:01:00Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "id": "user-early",
                "content": [{"text": "same words"}],
            },
        },
        {
            "timestamp": "2026-07-20T00:02:00Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "tool-result",
                "output": "same words",
            },
        },
        {
            "timestamp": "2026-07-20T00:03:00Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "phase": "final_answer",
                "id": "assistant-final",
                "content": [{"text": "same words"}],
            },
        },
        {
            "timestamp": "2026-07-20T04:30:00Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "id": "user-late",
                "content": [{"text": "same words"}],
            },
        },
    ]
    session.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    ingest = runner.invoke(
        app,
        [
            "ingest",
            str(session),
            "--adapter",
            "codex-session",
            "--allow-model-egress",
            "--json",
        ],
        env=_env(runtime_home),
    )
    segment = runner.invoke(app, ["segment", "--json"], env=_env(runtime_home))
    replay_segment = runner.invoke(app, ["segment", "--json"], env=_env(runtime_home))

    assert ingest.exit_code == 0, ingest.output
    assert segment.exit_code == 0, segment.output
    assert replay_segment.exit_code == 0, replay_segment.output
    assert json.loads(ingest.stdout)["events"] == 4
    assert json.loads(ingest.stdout)["sources"] == 3
    assert json.loads(segment.stdout)["episodes"] == 4
    assert json.loads(segment.stdout)["persisted"] == 4
    assert json.loads(replay_segment.stdout)["eligible_events"] == 0
    memory = MemoryStore(runtime_home / "data" / "memory.sqlite3")
    with memory.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0] == 4
        assert (
            connection.execute(
                "SELECT COUNT(DISTINCT content_fingerprint) FROM episodes"
            ).fetchone()[0]
            == 1
        )


def test_codex_cursor_is_not_advanced_when_event_persistence_fails(
    runtime_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = tmp_path / "cursor-crash.jsonl"
    session.write_text(
        "\n".join(
            json.dumps(record)
            for record in (
                {
                    "timestamp": "2026-07-20T00:00:00Z",
                    "type": "session_meta",
                    "payload": {"id": "cursor-crash"},
                },
                {
                    "timestamp": "2026-07-20T00:01:00Z",
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "role": "user",
                        "content": [{"text": "must replay after crash"}],
                    },
                },
            )
        )
        + "\n",
        encoding="utf-8",
    )
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    def fail_event_persistence(*args: object, **kwargs: object) -> str:
        raise RuntimeError("synthetic persistence crash")

    monkeypatch.setattr(MemoryStore, "create_event", fail_event_persistence)
    result = runner.invoke(
        app,
        ["ingest", str(session), "--adapter", "codex-session", "--json"],
        env=_env(runtime_home),
    )

    assert result.exit_code != 0
    operations = OperationsStore(runtime_home / "data" / "operations.sqlite3")
    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM cursors").fetchone()[0] == 0


def test_tool_image_payload_is_not_copied_into_memory_db(
    runtime_home: Path, tmp_path: Path
) -> None:
    session = tmp_path / "tool-image.jsonl"
    image_payload = "A" * 12_000
    records = [
        {
            "timestamp": "2026-07-20T00:00:00Z",
            "type": "session_meta",
            "payload": {"id": "tool-image"},
        },
        {
            "timestamp": "2026-07-20T00:01:00Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "call_id": "image-output",
                "output": {"image": {"data": image_payload, "mime_type": "image/png"}},
            },
        },
        {
            "timestamp": "2026-07-20T00:02:00Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"text": "safe retained text"}],
            },
        },
    ]
    session.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    result = runner.invoke(
        app,
        ["ingest", str(session), "--adapter", "codex-session", "--json"],
        env=_env(runtime_home),
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["events"] == 1
    with MemoryStore(runtime_home / "data" / "memory.sqlite3").connection() as connection:
        content = "\n".join(
            str(row["content_text"])
            for row in connection.execute("SELECT content_text FROM events").fetchall()
        )
    assert content == "safe retained text"
    assert image_payload not in content


def test_advisory_ingest_reports_unique_source_and_all_partition_ids(
    runtime_home: Path, tmp_path: Path
) -> None:
    summaries = tmp_path / "summaries"
    summaries.mkdir()
    for index in (1, 2):
        (summaries / f"summary-{index}.md").write_text(
            f"---\ncreated_at: 2026-07-20T03:00:00Z\n---\n# Advisory summary {index}\n",
            encoding="utf-8",
        )
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    result = runner.invoke(
        app,
        ["ingest", str(summaries), "--adapter", "chronicle", "--json"],
        env=_env(runtime_home),
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["sources"] == 1
    assert len(payload["source_ids"]) == 1
    assert payload["partitions"] == 2
    assert len(payload["partition_ids"]) == 2


def test_operations_adapter_writes_only_ops_and_status_returns_safe_latest_summaries(
    runtime_home: Path, tmp_path: Path
) -> None:
    workspace = tmp_path / "private-workspace-name"
    (workspace / ".git").mkdir(parents=True)
    (workspace / ".git" / "HEAD").write_text("ref: refs/heads/operations-test\n", encoding="utf-8")
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    memory = MemoryStore(runtime_home / "data" / "memory.sqlite3")
    revision_before = memory.current_revision()

    ingested = runner.invoke(
        app,
        [
            "ingest",
            str(workspace),
            "--adapter",
            "operations",
            "--occurred-at",
            "2026-07-20T03:30:00+08:00",
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert ingested.exit_code == 0, ingested.output
    payload = json.loads(ingested.stdout)
    assert payload["operations_snapshots"] == 3
    assert payload["sources"] == 0
    assert payload["events"] == 0
    with memory.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    assert memory.current_revision() == revision_before
    operations = OperationsStore(runtime_home / "data" / "operations.sqlite3")
    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM workspace_snapshots").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM automation_snapshots").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM health_snapshots").fetchone()[0] == 1

    status = runner.invoke(app, ["status", "--json"], env=_env(runtime_home))

    assert status.exit_code == 0, status.output
    latest = json.loads(status.stdout)["operations"]["latest_snapshots"]
    assert latest["workspace"]["git_branch"] == "operations-test"
    assert latest["automation"]["automation_type"] == "launchd"
    assert latest["health"]["check_count"] == 2
    assert str(workspace) not in status.stdout


def test_operations_adapter_rejects_memory_policy_options(
    runtime_home: Path, tmp_path: Path
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    result = runner.invoke(
        app,
        [
            "ingest",
            str(tmp_path),
            "--adapter",
            "operations",
            "--allow-model-egress",
        ],
        env=_env(runtime_home),
    )

    assert result.exit_code != 0
    assert "operations adapter does not accept" in result.output


def test_nonmanual_private_egress_requires_private_sensitivity(
    runtime_home: Path, tmp_path: Path
) -> None:
    session = tmp_path / "session.jsonl"
    session.write_text("", encoding="utf-8")
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    result = runner.invoke(
        app,
        [
            "ingest",
            str(session),
            "--adapter",
            "codex-session",
            "--allow-private-model-egress",
        ],
        env=_env(runtime_home),
    )

    assert result.exit_code != 0
    assert "requires --sensitivity private" in result.output


def test_nonmanual_ingest_can_revoke_existing_private_egress_policy(
    runtime_home: Path, tmp_path: Path
) -> None:
    session = tmp_path / "private-session.jsonl"
    session.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "timestamp": "2026-07-20T03:00:00Z",
                        "type": "session_meta",
                        "payload": {"id": "policy-revocation-session"},
                    }
                ),
                json.dumps(
                    {
                        "timestamp": "2026-07-20T03:01:00Z",
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "private note"},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    enabled = runner.invoke(
        app,
        [
            "ingest",
            str(session),
            "--adapter",
            "codex-session",
            "--sensitivity",
            "private",
            "--allow-private-model-egress",
            "--json",
        ],
        env=_env(runtime_home),
    )
    assert enabled.exit_code == 0, enabled.output
    source_id = json.loads(enabled.stdout)["source_ids"][0]
    store = MemoryStore(runtime_home / "data" / "memory.sqlite3")
    revision_before = store.current_revision()

    disabled = runner.invoke(
        app,
        [
            "ingest",
            str(session),
            "--adapter",
            "codex-session",
            "--sensitivity",
            "private",
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert disabled.exit_code == 0, disabled.output
    with store.connection() as connection:
        source = connection.execute(
            "SELECT sensitivity, model_egress_allowed, metadata_json FROM sources "
            "WHERE source_id = ?",
            (source_id,),
        ).fetchone()
        partition = connection.execute(
            "SELECT opted_in, sensitivity FROM source_partitions WHERE source_id = ?",
            (source_id,),
        ).fetchone()
    assert source is not None
    assert source["sensitivity"] == "private"
    assert source["model_egress_allowed"] == 0
    assert json.loads(source["metadata_json"])["allow_private_model_egress"] is False
    assert partition is not None
    assert partition["opted_in"] == 1
    assert partition["sensitivity"] == "private"
    assert store.current_revision() == revision_before


def test_review_approve_is_the_explicit_canonical_write(runtime_home: Path) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    memory_path = runtime_home / "data" / "memory.sqlite3"
    store = MemoryStore(memory_path)
    slot = fingerprint(
        "claim-identity-v1",
        "leo",
        "preference.language",
        "user_profile",
    )
    expected = fingerprint_head_set(())
    payload = {
        "subject_text": "Leo",
        "predicate": "preference.language",
        "scope": "user_profile",
        "value": "zh-TW",
        "summary": "Leo偏好繁體中文",
        "evidence_event_ids": [],
    }
    batch_id = store.create_review_batch(
        [
            ReviewProposalInput(
                proposal_type="add",
                target_slot_fingerprint=slot,
                expected_head_set_hash=expected,
                precondition_hash=fingerprint_precondition(
                    proposal_type=ProposalType.ADD,
                    target_slot_fingerprint=slot,
                    expected_head_set_hash=expected,
                ),
                proposal_payload_fingerprint=fingerprint(payload),
                evidence_set_fingerprint=fingerprint([]),
                payload=payload,
            )
        ]
    )

    preview = runner.invoke(
        app,
        ["review", "approve", batch_id, "--dry-run", "--json"],
        env=_env(runtime_home),
    )
    assert preview.exit_code == 0, preview.output
    assert store.current_revision() == 0

    applied = runner.invoke(
        app,
        ["review", "approve", batch_id, "--json"],
        env=_env(runtime_home),
    )

    assert applied.exit_code == 0, applied.output
    assert json.loads(applied.stdout)["memory_revision"] == 1
    assert store.current_revision() == 1
    assert store.load_review_batch(batch_id)["status"] == "applied"


def test_run_nightly_dry_run_never_creates_jobs(runtime_home: Path) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    result = runner.invoke(
        app,
        ["run-nightly", "--dry-run", "--json"],
        env=_env(runtime_home),
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["status"] == "dry_run"
    with MemoryStore(runtime_home / "data" / "memory.sqlite3").connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM review_batches").fetchone()[0] == 0


def test_run_nightly_operational_failure_exits_nonzero(
    runtime_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0

    class FailedRunner:
        def run(self) -> NightlyReport:
            return NightlyReport(
                run_id="run-failed",
                status=NightlyStatus.CERTIFICATION_REQUIRED,
                lease_owner="test",
                leased_jobs=0,
                outcomes=(),
                usage=BudgetUsage(),
                error_codes=("WorkerCertificationRequired",),
            )

    monkeypatch.setattr(
        cli_module,
        "build_nightly_runner",
        lambda *args, **kwargs: FailedRunner(),
    )

    result = runner.invoke(app, ["run-nightly", "--json"], env=_env(runtime_home))

    assert result.exit_code == 1
    assert json.loads(result.stdout)["status"] == "certification_required"


def test_schedule_install_recomputes_gates_and_supports_dry_run(
    runtime_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    _record_verified_runs(runtime_home)
    executable = runtime_home / "venv" / "bin" / "dream"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o700)
    destination = tmp_path / "agents" / "nightly.plist"
    monkeypatch.setattr(cli_module, "verify_certification_stamp", lambda *args, **kwargs: True)
    monkeypatch.setattr(cli_module, "find_schedule_conflicts", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        cli_module,
        "find_openclaw_schedule_conflicts",
        lambda *args, **kwargs: [],
    )

    preview = runner.invoke(
        app,
        [
            "schedule",
            "install",
            "--job",
            "nightly",
            "--dream-executable",
            str(executable),
            "--destination",
            str(destination),
            "--dry-run",
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert preview.exit_code == 0, preview.output
    payload = json.loads(preview.stdout)
    assert payload["gate_passed"] is True
    assert payload["target"]["hour"] == 4
    assert not destination.exists()

    calls: list[tuple[Path, str]] = []

    def fake_bootstrap(path: Path, *, label: str) -> bool:
        calls.append((path, label))
        return True

    monkeypatch.setattr(cli_module, "bootstrap_launch_agent", fake_bootstrap)
    installed = runner.invoke(
        app,
        [
            "schedule",
            "install",
            "--job",
            "nightly",
            "--dream-executable",
            str(executable),
            "--destination",
            str(destination),
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert installed.exit_code == 0, installed.output
    assert json.loads(installed.stdout)["installed"] is True
    assert destination.stat().st_mode & 0o777 == 0o600
    assert calls == [(destination, "com.leo.local-dreaming.nightly")]


def test_schedule_install_fails_closed_without_certification(
    runtime_home: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    _record_verified_runs(runtime_home)
    executable = tmp_path / "dream"
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o700)
    monkeypatch.setattr(cli_module, "verify_certification_stamp", lambda *args, **kwargs: False)
    monkeypatch.setattr(cli_module, "find_schedule_conflicts", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        cli_module,
        "find_openclaw_schedule_conflicts",
        lambda *args, **kwargs: [],
    )

    result = runner.invoke(
        app,
        [
            "schedule",
            "install",
            "--dream-executable",
            str(executable),
            "--destination",
            str(tmp_path / "nightly.plist"),
            "--dry-run",
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["worker_certified"] is False


def test_status_gate_requires_certification_and_uses_0400(
    runtime_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    _record_verified_runs(runtime_home)
    monkeypatch.setattr(cli_module, "find_schedule_conflicts", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        cli_module,
        "find_openclaw_schedule_conflicts",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(cli_module, "verify_certification_stamp", lambda *args, **kwargs: False)

    blocked = runner.invoke(app, ["status", "--json"], env=_env(runtime_home))
    assert blocked.exit_code == 0, blocked.output
    blocked_payload = json.loads(blocked.stdout)
    assert blocked_payload["schedule_target"]["hour"] == 4
    assert blocked_payload["launchd_install_gate_passed"] is False

    monkeypatch.setattr(cli_module, "verify_certification_stamp", lambda *args, **kwargs: True)
    ready = runner.invoke(app, ["status", "--json"], env=_env(runtime_home))
    assert json.loads(ready.stdout)["launchd_install_gate_passed"] is True


def test_consolidate_dry_run_reports_temporal_review_without_writing(
    runtime_home: Path,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    store = MemoryStore(runtime_home / "data" / "memory.sqlite3")
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="plan.deadline",
            scope="project_state",
            value="expired",
            summary="Expired plan",
            valid_to="2020-01-01",
        )
    )

    result = runner.invoke(
        app,
        ["consolidate", "--dry-run", "--json"],
        env=_env(runtime_home),
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["temporal"]["proposal_count"] == 1
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM review_batches").fetchone()[0] == 0


def test_review_correct_supersedes_current_head_and_stales_target(
    runtime_home: Path,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    store = MemoryStore(runtime_home / "data" / "memory.sqlite3")
    _, old_version, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project.status",
            scope="project_state",
            value="planned",
            summary="Project is planned.",
        )
    )
    slot = identity_fingerprint("Leo", "project.status", "project_state")
    expected = fingerprint_head_set(store.load_heads(slot))
    payload = {
        "subject_text": "Leo",
        "predicate": "project.status",
        "scope": "project_state",
        "value": "done",
        "summary": "Project is done.",
        "evidence_event_ids": [],
    }
    batch_id = store.create_review_batch(
        [
            ReviewProposalInput(
                proposal_type="update",
                target_slot_fingerprint=slot,
                expected_head_set_hash=expected,
                precondition_hash=fingerprint_precondition(
                    proposal_type=ProposalType.UPDATE,
                    target_slot_fingerprint=slot,
                    expected_head_set_hash=expected,
                ),
                proposal_payload_fingerprint=fingerprint(payload),
                evidence_set_fingerprint=fingerprint([]),
                payload=payload,
                proposal_id="proposal-correct",
            )
        ]
    )

    corrected = runner.invoke(
        app,
        [
            "review",
            "correct",
            batch_id,
            "proposal-correct",
            "--value",
            '"in_progress"',
            "--summary",
            "Leo corrected the project to in progress.",
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert corrected.exit_code == 0, corrected.output
    heads = store.load_heads(slot)
    assert len(heads) == 1
    assert heads[0].claim_version_id != old_version
    assert store.load_review_batch(batch_id)["proposals"][0]["status"] == "stale"


@pytest.mark.parametrize("mismatch", ["payload", "evidence"])
def test_review_correct_rejects_initially_inconsistent_proposal_binding(
    runtime_home: Path, mismatch: str
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    store = MemoryStore(runtime_home / "data" / "memory.sqlite3")
    slot = identity_fingerprint("Leo", "project.status", "project_state")
    expected = fingerprint_head_set(())
    payload = {
        "subject_text": "Leo",
        "predicate": "project.status",
        "scope": "project_state",
        "value": "planned",
        "summary": "Project is planned.",
        "evidence_event_ids": [],
    }
    batch_id = store.create_review_batch(
        [
            ReviewProposalInput(
                proposal_type="add",
                target_slot_fingerprint=slot,
                expected_head_set_hash=expected,
                precondition_hash=fingerprint_precondition(
                    proposal_type=ProposalType.ADD,
                    target_slot_fingerprint=slot,
                    expected_head_set_hash=expected,
                ),
                proposal_payload_fingerprint=(
                    "mismatched" if mismatch == "payload" else fingerprint(payload)
                ),
                evidence_set_fingerprint=(
                    "mismatched" if mismatch == "evidence" else fingerprint([])
                ),
                payload=payload,
                proposal_id="proposal-inconsistent",
            )
        ]
    )

    corrected = runner.invoke(
        app,
        [
            "review",
            "correct",
            batch_id,
            "proposal-inconsistent",
            "--value",
            '"in_progress"',
            "--summary",
            "Leo corrected the project to in progress.",
            "--json",
        ],
        env=_env(runtime_home),
    )

    assert corrected.exit_code != 0
    assert f"proposal {mismatch} fingerprint mismatch" in corrected.output
    assert store.current_revision() == 0
    stored = store.load_review_batch(batch_id)
    assert stored["status"] == "pending"
    assert stored["proposals"][0]["status"] == "pending"


def test_snapshot_restore_validates_both_roles_before_replacing_either_db(
    runtime_home: Path,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    created = runner.invoke(app, ["snapshot", "create", "--json"], env=_env(runtime_home))
    assert created.exit_code == 0, created.output
    snapshot = Path(json.loads(created.stdout)["snapshot"])
    operations = OperationsStore(runtime_home / "data" / "operations.sqlite3")
    operations.enqueue_job(job_type="phase1", dedupe_key="active-only")
    memory_snapshot = snapshot / "memory.sqlite3"
    connection = sqlite3.connect(memory_snapshot)
    try:
        connection.execute("UPDATE metadata SET value = 'operations' WHERE key = 'database_role'")
        connection.commit()
    finally:
        connection.close()
    manifest_path = snapshot / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["memory.sqlite3"] = hashlib.sha256(memory_snapshot.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    restored = runner.invoke(
        app,
        ["snapshot", "restore", str(snapshot), "--json"],
        env=_env(runtime_home),
    )

    assert restored.exit_code != 0
    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1


def test_snapshot_restore_holds_writer_locks_in_fixed_order(
    runtime_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert runner.invoke(app, ["init"], env=_env(runtime_home)).exit_code == 0
    created = runner.invoke(app, ["snapshot", "create", "--json"], env=_env(runtime_home))
    assert created.exit_code == 0, created.output
    snapshot = Path(json.loads(created.stdout)["snapshot"])
    memory_path = runtime_home / "data" / "memory.sqlite3"
    operations_path = runtime_home / "data" / "operations.sqlite3"
    events: list[str] = []
    original_memory_lock = cli_module.memory_maintenance_lock
    original_operations_lock = cli_module.operations_maintenance_lock
    original_restore = cli_module.restore_database

    @contextmanager
    def tracked_memory_lock(path: str | Path):
        events.append("memory-enter")
        with original_memory_lock(path):
            try:
                yield
            finally:
                events.append("memory-exit")

    @contextmanager
    def tracked_operations_lock(path: str | Path):
        events.append("operations-enter")
        with original_operations_lock(path):
            try:
                yield
            finally:
                events.append("operations-exit")

    def checked_restore(
        snapshot_path: str | Path,
        target_path: str | Path,
        *,
        expected_role: str,
    ) -> Path:
        events.append(f"restore:{expected_role}")
        for lock_path in (
            memory_maintenance_lock_path(memory_path),
            operations_maintenance_lock_path(operations_path),
        ):
            descriptor = os.open(lock_path, os.O_RDWR)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
        return original_restore(snapshot_path, target_path, expected_role=expected_role)

    monkeypatch.setattr(cli_module, "memory_maintenance_lock", tracked_memory_lock)
    monkeypatch.setattr(cli_module, "operations_maintenance_lock", tracked_operations_lock)
    monkeypatch.setattr(cli_module, "restore_database", checked_restore)

    restored = runner.invoke(
        app,
        ["snapshot", "restore", str(snapshot), "--json"],
        env=_env(runtime_home),
    )

    assert restored.exit_code == 0, restored.output
    assert events == [
        "memory-enter",
        "operations-enter",
        "restore:operations",
        "restore:memory",
        "operations-exit",
        "memory-exit",
    ]


def test_wheel_console_script_runs_from_noneditable_install(tmp_path: Path) -> None:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is unavailable")
    project_root = Path(__file__).parents[1]
    wheel_dir = tmp_path / "dist"
    environment = tmp_path / "venv"
    subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(wheel_dir)],
        cwd=project_root,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [uv, "venv", str(environment), "--python", sys.executable],
        check=True,
        capture_output=True,
    )
    wheel = next(wheel_dir.glob("local_dreaming-*.whl"))
    subprocess.run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(environment / "bin" / "python"),
            "--no-deps",
            str(wheel),
        ],
        check=True,
        capture_output=True,
    )
    child_env = dict(os.environ)
    child_env["PYTHONPATH"] = site.getsitepackages()[0]
    help_result = subprocess.run(
        [str(environment / "bin" / "dream"), "--help"],
        cwd=tmp_path,
        env=child_env,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Practical local memory for Codex" in help_result.stdout
    direct_url = subprocess.run(
        [
            str(environment / "bin" / "python"),
            "-c",
            (
                "import importlib.metadata as m; "
                "print(m.distribution('local-dreaming').read_text('direct_url.json'))"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert ".whl" in direct_url
    assert '"editable":true' not in direct_url.replace(" ", "")
    module_path = subprocess.run(
        [
            str(environment / "bin" / "python"),
            "-c",
            "import local_dreaming; print(local_dreaming.__file__)",
        ],
        env=child_env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert str(environment) in module_path

from __future__ import annotations

import json
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from local_dreaming.config import ModelSettings, RuntimePaths
from local_dreaming.doctor import (
    CheckStatus,
    DoctorCheck,
    DoctorPolicy,
    DoctorReport,
    run_doctor,
    verify_certification_stamp,
    write_certification_failure,
    write_certification_stamp,
)
from local_dreaming.errors import WorkerProtocolError
from local_dreaming.worker import (
    CodexWorker,
    ModelCallRecord,
    WorkerRequest,
    WorkerResult,
    WorkerRuntime,
    WorkerUsage,
    build_codex_command,
    build_worker_environment,
    parse_codex_jsonl,
    validate_json_schema,
)

PACKAGE_ROOT = Path(__file__).parents[1] / "src" / "local_dreaming"


def _runtime(runtime_home: Path, **environment: str) -> WorkerRuntime:
    paths = RuntimePaths(home=runtime_home)
    paths.ensure()
    return WorkerRuntime(
        paths=paths,
        codex_binary=Path("/pinned/codex"),
        base_environment={
            "HOME": "/Users/tester",
            "PATH": "/usr/bin:/bin",
            "LANG": "en_US.UTF-8",
            **environment,
        },
    )


def _request(*, phase: str = "doctor") -> WorkerRequest:
    return WorkerRequest(
        phase=phase,
        model_id="gpt-5.6-sol",
        reasoning_effort="medium",
        prompt='Return {"ok":true,"nonce":"safe-nonce"}.',
        schema_path=PACKAGE_ROOT / "schemas" / "doctor_probe.schema.json",
        job_id="job-1",
    )


def _jsonl(message: dict[str, Any]) -> bytes:
    events = [
        {"type": "thread.started", "thread_id": "thread-safe"},
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {
                "id": "item-1",
                "type": "agent_message",
                "text": json.dumps(message, separators=(",", ":")),
            },
        },
        {
            "type": "turn.completed",
            "usage": {"input_tokens": 11, "cached_input_tokens": 3, "output_tokens": 5},
        },
    ]
    return ("\n".join(json.dumps(event) for event in events) + "\n").encode()


def test_command_uses_sterile_cwd_permission_profile_and_stdin(runtime_home: Path) -> None:
    runtime = _runtime(runtime_home)
    command = build_codex_command(runtime, _request())

    assert command[:3] == ("/pinned/codex", "exec", "--strict-config")
    assert "--ephemeral" in command
    assert "--ignore-user-config" in command
    assert "--ignore-rules" in command
    assert "--sandbox" not in command
    assert command[-1] == "-"
    assert str(runtime.paths.sterile_cwd.resolve()) in command
    overrides = [command[index + 1] for index, value in enumerate(command[:-1]) if value == "-c"]
    assert 'default_permissions="dream_worker"' in overrides
    assert 'permissions.dream_worker.filesystem.:root="deny"' in overrides
    assert 'permissions.dream_worker.filesystem.:minimal="read"' in overrides
    assert "permissions.dream_worker.network.enabled=false" in overrides
    assert "mcp_servers={}" in overrides
    assert "features.memories=false" in overrides


def test_environment_is_allowlisted_and_uses_dedicated_home(runtime_home: Path) -> None:
    runtime = _runtime(
        runtime_home,
        OPENAI_API_KEY="never-copy-this",
        CODEX_API_KEY="never-copy-this-either",
        HTTP_PROXY="http://username:password@example.test",
        SSH_AUTH_SOCK="/private/ssh-agent",
        AWS_ACCESS_KEY_ID="never-copy-cloud-credentials",
    )

    environment = build_worker_environment(runtime)

    assert environment["CODEX_HOME"] == str(runtime.paths.codex_home.resolve())
    assert environment["TMPDIR"] == str(runtime.private_tmp.resolve())
    assert environment["HOME"] == str(runtime.private_os_home.resolve())
    assert environment["PATH"] == "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin"
    assert environment["USER"] == "dream-worker"
    assert "OPENAI_API_KEY" not in environment
    assert "CODEX_API_KEY" not in environment
    assert "HTTP_PROXY" not in environment
    assert "SSH_AUTH_SOCK" not in environment
    assert "AWS_ACCESS_KEY_ID" not in environment


def test_parser_accepts_schema_valid_json_without_reasoning_usage_field() -> None:
    schema = json.loads((PACKAGE_ROOT / "schemas" / "doctor_probe.schema.json").read_text())

    result = parse_codex_jsonl(_jsonl({"ok": True, "nonce": "safe-nonce"}), schema)

    assert result.thread_id == "thread-safe"
    assert result.payload == {"ok": True, "nonce": "safe-nonce"}
    assert result.usage.reasoning_output_tokens == 0


@pytest.mark.parametrize("item_type", ["command_execution", "mcp_tool_call", "file_change"])
def test_parser_rejects_any_tool_item(item_type: str) -> None:
    schema = json.loads((PACKAGE_ROOT / "schemas" / "doctor_probe.schema.json").read_text())
    output = _jsonl({"ok": True, "nonce": "safe-nonce"}).decode().splitlines()
    output.insert(
        2,
        json.dumps(
            {
                "type": "item.started",
                "item": {"id": "unsafe", "type": item_type, "command": "true"},
            }
        ),
    )

    with pytest.raises(WorkerProtocolError, match="forbidden tool/item"):
        parse_codex_jsonl("\n".join(output), schema)


def test_parser_rejects_unknown_event() -> None:
    schema = json.loads((PACKAGE_ROOT / "schemas" / "doctor_probe.schema.json").read_text())
    output = '{"type":"future.unknown"}\n'

    with pytest.raises(WorkerProtocolError, match="unsafe or unknown"):
        parse_codex_jsonl(output, schema)


@pytest.mark.parametrize(
    "events, expected",
    [
        (
            [
                {"type": "thread.started", "thread_id": "one"},
                {"type": "thread.started", "thread_id": "one"},
            ],
            "first and only",
        ),
        (
            [
                {"type": "thread.started", "thread_id": "one"},
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "{}"},
                },
            ],
            "before turn.started",
        ),
        (
            [
                {"type": "thread.started", "thread_id": "one"},
                {"type": "turn.started"},
                {
                    "type": "turn.completed",
                    "usage": {"input_tokens": 1, "cached_input_tokens": 0, "output_tokens": 1},
                },
                {"type": "turn.started"},
            ],
            "after turn.completed",
        ),
    ],
)
def test_parser_rejects_duplicate_or_out_of_order_lifecycle(
    events: list[dict[str, Any]], expected: str
) -> None:
    schema = json.loads((PACKAGE_ROOT / "schemas" / "doctor_probe.schema.json").read_text())
    output = "\n".join(json.dumps(event) for event in events)

    with pytest.raises(WorkerProtocolError, match=expected):
        parse_codex_jsonl(output, schema)


def test_schema_validator_rejects_extra_or_missing_fields() -> None:
    schema = json.loads((PACKAGE_ROOT / "schemas" / "doctor_probe.schema.json").read_text())

    with pytest.raises(WorkerProtocolError, match="unknown keys"):
        validate_json_schema({"ok": True, "nonce": "x", "extra": "leak"}, schema)
    with pytest.raises(WorkerProtocolError, match="missing required"):
        validate_json_schema({"ok": True}, schema)


class _CallLog:
    def __init__(self) -> None:
        self.records: list[ModelCallRecord] = []

    def record_model_call(self, record: ModelCallRecord) -> None:
        self.records.append(record)


def test_worker_passes_prompt_only_on_stdin_and_logs_metadata(
    runtime_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _runtime(runtime_home, OPENAI_API_KEY="do-not-inherit")
    sink = _CallLog()
    captured: dict[str, Any] = {}

    def fake_run(command: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=_jsonl({"ok": True, "nonce": "safe-nonce"}),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = CodexWorker(runtime, call_log=sink).run(_request())

    assert result.payload == {"ok": True, "nonce": "safe-nonce"}
    assert captured["input"] == _request().prompt.encode()
    assert _request().prompt not in captured["command"]
    assert "OPENAI_API_KEY" not in captured["env"]
    assert len(sink.records) == 1
    record = sink.records[0]
    assert record.status == "completed"
    assert not hasattr(record, "prompt")
    assert not hasattr(record, "output")
    assert record.prompt_sha256
    assert record.input_bytes == len(_request().prompt.encode())


def test_doctor_offline_checks_are_synthetic_and_never_forward_credentials(
    runtime_home: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    binary = tmp_path / "codex"
    binary.write_bytes(b"pinned test binary")
    binary.chmod(0o700)
    sentinel_root = tmp_path / "sentinel-root"
    runtime = _runtime(runtime_home, OPENAI_API_KEY="never-forward-this")
    runtime = WorkerRuntime(
        paths=runtime.paths,
        codex_binary=binary,
        base_environment=runtime.base_environment,
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(command: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        commands.append(command)
        assert "OPENAI_API_KEY" not in kwargs["env"]
        if command[-1] == "--version":
            return subprocess.CompletedProcess(command, 0, stdout=b"codex-cli test\n", stderr=b"")
        if "app-server" in command:
            assert kwargs["input"] == b""
            return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")
        if "/usr/bin/nc" in command or "/usr/bin/touch" in command:
            return subprocess.CompletedProcess(command, 1, stdout=b"", stderr=b"")
        if "/usr/bin/head" in command and command[-1] != "/usr/bin/env":
            return subprocess.CompletedProcess(command, 1, stdout=b"", stderr=b"denied")
        return subprocess.CompletedProcess(command, 0, stdout=b"X", stderr=b"")

    policy = DoctorPolicy(
        expected_codex_version="codex-cli test",
        sentinel_root=sentinel_root,
    )

    report = run_doctor(runtime, policy=policy, runner=fake_run)

    assert report.offline_ready
    assert not report.certified
    filevault_check = next(check for check in report.checks if check.name == "filevault")
    assert not filevault_check.required
    live_checks = [check for check in report.checks if check.name.startswith("live_probe_")]
    assert len(live_checks) == 2
    assert all(check.status is CheckStatus.WARN for check in live_checks)
    flattened = " ".join(" ".join(command) for command in commands).lower()
    assert "auth.json" not in flattened
    assert "login" not in flattened


def test_doctor_version_change_is_warning_not_sha_pin(runtime_home: Path, tmp_path: Path) -> None:
    binary = tmp_path / "codex"
    binary.write_bytes(b"version one")
    binary.chmod(0o700)
    sentinel_root = tmp_path / "sentinel-root"
    runtime = WorkerRuntime(
        paths=RuntimePaths(home=runtime_home),
        codex_binary=binary,
        base_environment={"HOME": "/Users/test", "PATH": "/usr/bin:/bin"},
    )

    def fake_run(command: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        del kwargs
        if command[-1] == "--version":
            return subprocess.CompletedProcess(command, 0, stdout=b"codex-cli newer\n", stderr=b"")
        if "app-server" in command:
            return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")
        if "/usr/bin/nc" in command or "/usr/bin/touch" in command:
            return subprocess.CompletedProcess(command, 1, stdout=b"", stderr=b"")
        if "/usr/bin/head" in command and command[-1] != "/usr/bin/env":
            return subprocess.CompletedProcess(command, 1, stdout=b"", stderr=b"")
        return subprocess.CompletedProcess(command, 0, stdout=b"X", stderr=b"")

    report = run_doctor(
        runtime,
        policy=DoctorPolicy(
            expected_codex_version="codex-cli prior",
            sentinel_root=sentinel_root,
        ),
        runner=fake_run,
    )

    version_check = next(check for check in report.checks if check.name == "codex_version")
    digest_check = next(check for check in report.checks if check.name == "codex_sha256")
    assert version_check.status is CheckStatus.WARN
    assert digest_check.status is CheckStatus.PASS
    assert report.offline_ready


class _SyntheticProbeWorker:
    def __init__(self) -> None:
        self.requests: list[WorkerRequest] = []

    def run(self, request: WorkerRequest) -> WorkerResult:
        self.requests.append(request)
        nonce = request.prompt.split("Synthetic nonce: ", maxsplit=1)[1].strip()
        usage = WorkerUsage(1, 0, 1, 0)
        record = ModelCallRecord(
            call_id="doctor-call",
            job_id=None,
            phase="doctor",
            model_id=request.model_id,
            reasoning_effort=request.reasoning_effort,
            prompt_sha256="p" * 64,
            schema_sha256="s" * 64,
            config_vector_sha256="c" * 64,
            input_bytes=len(request.prompt.encode()),
            stdout_bytes=1,
            stderr_bytes=0,
            duration_ms=1,
            status="completed",
            exit_code=0,
            usage=usage,
        )
        return WorkerResult(
            call_id=record.call_id,
            thread_id="doctor-thread",
            payload={"ok": True, "nonce": nonce},
            usage=usage,
            record=record,
        )


def test_live_doctor_certifies_both_model_configs_and_stamp_expires(
    runtime_home: Path, tmp_path: Path
) -> None:
    binary = tmp_path / "codex"
    binary.write_text("#!/bin/sh\necho 'codex-cli test'\n")
    binary.chmod(0o700)
    sentinel_root = tmp_path / "sentinel-root"
    runtime = WorkerRuntime(
        paths=RuntimePaths(home=runtime_home),
        codex_binary=binary,
        base_environment={"HOME": "/Users/test", "PATH": "/usr/bin:/bin"},
    )

    def fake_run(command: tuple[str, ...], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        del kwargs
        if command[-1] == "--version":
            return subprocess.CompletedProcess(command, 0, stdout=b"codex-cli test\n", stderr=b"")
        if "app-server" in command:
            return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")
        if "/usr/bin/nc" in command or "/usr/bin/touch" in command:
            return subprocess.CompletedProcess(command, 1, stdout=b"", stderr=b"")
        if "/usr/bin/head" in command and command[-1] != "/usr/bin/env":
            return subprocess.CompletedProcess(command, 1, stdout=b"", stderr=b"")
        return subprocess.CompletedProcess(command, 0, stdout=b"X", stderr=b"")

    models = ModelSettings()
    probe = _SyntheticProbeWorker()
    report = run_doctor(
        runtime,
        policy=DoctorPolicy(
            expected_codex_version="codex-cli test",
            sentinel_root=sentinel_root,
        ),
        live_probe=True,
        worker=probe,
        models=models,
        runner=fake_run,
    )
    issued = datetime(2026, 7, 19, 12, tzinfo=UTC)
    stamp_path = runtime.paths.worker / "certification.json"
    write_certification_stamp(
        stamp_path,
        report=report,
        runtime=runtime,
        models=models,
        now=issued,
        ttl=timedelta(hours=1),
    )

    assert report.certified
    required_sentinels = {
        "external_text_read_denied",
        "external_image_read_denied",
        "external_file_write_denied",
        "file_change_event_fail_closed",
        "live_ephemeral_state",
    }
    assert {check.name for check in report.checks if check.status is CheckStatus.PASS}.issuperset(
        required_sentinels
    )
    assert [(request.model_id, request.reasoning_effort) for request in probe.requests] == [
        (models.phase1_model, models.phase1_reasoning),
        (models.phase2_model, models.phase2_reasoning),
    ]
    assert verify_certification_stamp(
        stamp_path, runtime=runtime, models=models, now=issued + timedelta(minutes=30)
    )
    failure_report = DoctorReport(
        checks=(
            DoctorCheck(
                "live_probe_phase1",
                CheckStatus.FAIL,
                "synthetic probe failed",
            ),
        ),
        live_probe_requested=True,
        codex_version="codex-cli test",
    )
    write_certification_failure(
        stamp_path.with_name("doctor-last-failure.json"),
        report=failure_report,
        now=issued + timedelta(minutes=40),
    )
    assert not verify_certification_stamp(
        stamp_path, runtime=runtime, models=models, now=issued + timedelta(minutes=45)
    )
    write_certification_stamp(
        stamp_path,
        report=report,
        runtime=runtime,
        models=models,
        now=issued + timedelta(minutes=50),
        ttl=timedelta(hours=1),
    )
    assert verify_certification_stamp(
        stamp_path, runtime=runtime, models=models, now=issued + timedelta(minutes=55)
    )
    binary.write_text("#!/bin/sh\necho 'codex-cli test'\n# changed without a version change\n")
    assert verify_certification_stamp(
        stamp_path, runtime=runtime, models=models, now=issued + timedelta(minutes=60)
    )
    binary.write_text("#!/bin/sh\necho 'codex-cli upgraded'\n")
    assert not verify_certification_stamp(
        stamp_path, runtime=runtime, models=models, now=issued + timedelta(minutes=60)
    )
    binary.write_text("#!/bin/sh\necho 'codex-cli test'\n")
    assert not verify_certification_stamp(
        stamp_path,
        runtime=runtime,
        models=replace(models, phase2_reasoning="medium"),
        now=issued + timedelta(minutes=60),
    )
    assert not verify_certification_stamp(
        stamp_path, runtime=runtime, models=models, now=issued + timedelta(hours=2)
    )

    sessions = runtime.paths.codex_home / "sessions"
    sessions.mkdir()
    (sessions / "rollout-synthetic.jsonl").write_text("synthetic")
    dirty_report = run_doctor(
        runtime,
        policy=DoctorPolicy(
            expected_codex_version="codex-cli test",
            sentinel_root=sentinel_root,
        ),
        models=models,
        runner=fake_run,
    )
    state_check = next(
        check for check in dirty_report.checks if check.name == "ephemeral_state_clean"
    )
    assert state_check.status is CheckStatus.FAIL
    assert not dirty_report.offline_ready

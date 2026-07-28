from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from local_dreaming.config import ModelSettings
from local_dreaming.errors import WorkerProtocolError
from local_dreaming.worker import (
    CodexWorker,
    WorkerRequest,
    WorkerResult,
    WorkerRuntime,
    build_worker_environment,
    config_override_arguments,
    parse_codex_jsonl,
    sha256_file,
)

CERTIFIED_CODEX_VERSION = "codex-cli 0.144.5"


class CheckStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class DoctorCheck:
    name: str
    status: CheckStatus
    detail: str
    required: bool = True


@dataclass(frozen=True, slots=True)
class DoctorPolicy:
    expected_codex_version: str = CERTIFIED_CODEX_VERSION
    command_timeout_seconds: float = 10.0
    sentinel_root: Path | None = None


@dataclass(frozen=True, slots=True)
class DoctorReport:
    checks: tuple[DoctorCheck, ...]
    live_probe_requested: bool
    codex_version: str | None = None

    @property
    def offline_ready(self) -> bool:
        return all(check.status is not CheckStatus.FAIL for check in self.checks if check.required)

    @property
    def certified(self) -> bool:
        live_checks = [check for check in self.checks if check.name.startswith("live_probe_")]
        return (
            self.offline_ready
            and bool(live_checks)
            and all(check.status is CheckStatus.PASS for check in live_checks)
        )


@dataclass(frozen=True, slots=True)
class CertificationStamp:
    schema_version: int
    config_vector_sha256: str
    codex_version: str
    phase1_model: str
    phase1_reasoning: str
    phase2_model: str
    phase2_reasoning: str
    certified_at: datetime
    expires_at: datetime


class ProbeWorker(Protocol):
    def run(self, request: WorkerRequest) -> WorkerResult: ...


type CommandRunner = Callable[..., subprocess.CompletedProcess[bytes]]


def _check(name: str, passed: bool, detail: str, *, required: bool = True) -> DoctorCheck:
    return DoctorCheck(
        name=name,
        status=CheckStatus.PASS if passed else CheckStatus.FAIL,
        detail=detail,
        required=required,
    )


def _warning(name: str, detail: str) -> DoctorCheck:
    return DoctorCheck(name=name, status=CheckStatus.WARN, detail=detail, required=False)


def _run(
    runner: CommandRunner,
    command: Sequence[str],
    *,
    runtime: WorkerRuntime,
    timeout: float,
    input_bytes: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    return runner(
        tuple(command),
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=runtime.paths.sterile_cwd,
        env=build_worker_environment(runtime),
        timeout=timeout,
        check=False,
    )


def _permission_command(runtime: WorkerRuntime, executable: str, *arguments: str) -> list[str]:
    return [
        str(runtime.codex_binary),
        "sandbox",
        *config_override_arguments(),
        "-P",
        "dream_worker",
        "--include-managed-config",
        "-C",
        str(runtime.paths.sterile_cwd.resolve()),
        executable,
        *arguments,
    ]


def _path_mode_is_private(path: Path) -> bool:
    return path.is_dir() and (path.stat().st_mode & 0o077) == 0


def _forbidden_worker_artifacts(runtime: WorkerRuntime) -> tuple[str, ...]:
    forbidden_directories = {
        "archived_sessions",
        "memories",
        "rollout_summaries",
        "sessions",
    }
    forbidden_files = {"history.jsonl", "session_index.jsonl"}
    found: list[str] = []
    roots = (runtime.paths.codex_home, runtime.private_os_home)
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            relative = path.relative_to(root)
            if (
                forbidden_directories.intersection(relative.parts)
                or path.name in forbidden_files
                or path.name.startswith("rollout-")
            ):
                found.append(f"{root.name}/{relative}")
        state_tables = {
            "goals_*.sqlite": ("thread_goals",),
            "logs_*.sqlite": ("logs",),
            "memories_*.sqlite": ("stage1_outputs", "jobs"),
            "state_*.sqlite": (
                "threads",
                "agent_jobs",
                "agent_job_items",
                "thread_spawn_edges",
            ),
        }
        for pattern, table_names in state_tables.items():
            for database_path in root.glob(pattern):
                try:
                    uri = f"file:{database_path.resolve()}?mode=ro"
                    connection = sqlite3.connect(uri, uri=True, timeout=1.0)
                    try:
                        existing = {
                            str(row[0])
                            for row in connection.execute(
                                "SELECT name FROM sqlite_master WHERE type = 'table'"
                            )
                        }
                        for table_name in table_names:
                            if table_name not in existing:
                                continue
                            # Table names are fixed local constants, never external input.
                            count = int(
                                connection.execute(
                                    f'SELECT COUNT(*) FROM "{table_name}"'
                                ).fetchone()[0]
                            )
                            if count:
                                found.append(f"{root.name}/{database_path.name}:{table_name}")
                    finally:
                        connection.close()
                except sqlite3.Error:
                    found.append(f"{root.name}/{database_path.name}:unreadable")
    return tuple(sorted(set(found)))


def _file_change_parser_is_fail_closed() -> bool:
    package_root = Path(__file__).parent
    schema = json.loads((package_root / "schemas" / "doctor_probe.schema.json").read_text())
    output = "\n".join(
        (
            json.dumps({"type": "thread.started", "thread_id": "synthetic"}),
            json.dumps({"type": "turn.started"}),
            json.dumps(
                {
                    "type": "item.started",
                    "item": {"id": "synthetic", "type": "file_change"},
                }
            ),
        )
    )
    try:
        parse_codex_jsonl(output, schema)
    except WorkerProtocolError:
        return True
    return False


def run_doctor(
    runtime: WorkerRuntime,
    *,
    policy: DoctorPolicy | None = None,
    live_probe: bool = False,
    worker: ProbeWorker | None = None,
    models: ModelSettings | None = None,
    runner: CommandRunner = subprocess.run,
) -> DoctorReport:
    """Certify the worker boundary without reading credential files or private evidence.

    Offline checks inspect only binary identity, generated private directories, effective command
    configuration, and synthetic OS sentinels. The optional live probe sends a generated nonce and
    no user data. Authentication remains Codex's responsibility; this function never opens auth
    state or reads credential environment variables.
    """

    policy = policy or DoctorPolicy()
    models = models or ModelSettings()
    checks: list[DoctorCheck] = []
    try:
        runtime.prepare()
    except Exception as exc:
        return DoctorReport(
            checks=(
                DoctorCheck(
                    "runtime_paths",
                    CheckStatus.FAIL,
                    f"runtime preparation failed ({type(exc).__name__})",
                ),
            ),
            live_probe_requested=live_probe,
            codex_version=None,
        )

    binary_ok = runtime.codex_binary.is_file() and os.access(runtime.codex_binary, os.X_OK)
    checks.append(_check("codex_binary", binary_ok, "binary exists and is executable"))
    if not binary_ok:
        return DoctorReport(
            checks=tuple(checks),
            live_probe_requested=live_probe,
            codex_version=None,
        )

    try:
        version_result = _run(
            runner,
            (str(runtime.codex_binary), "--version"),
            runtime=runtime,
            timeout=policy.command_timeout_seconds,
        )
        version = version_result.stdout.decode("utf-8", errors="strict").strip()
        version_observed = version_result.returncode == 0 and bool(version)
        version_expected = version_observed and version == policy.expected_codex_version
    except OSError, UnicodeDecodeError, subprocess.TimeoutExpired:
        version = "unavailable"
        version_observed = False
        version_expected = False
    if not version_observed:
        checks.append(_check("codex_version", False, "could not observe Codex version"))
    elif version_expected:
        checks.append(_check("codex_version", True, f"observed version: {version}", required=False))
    else:
        checks.append(_warning("codex_version", f"version changed; observed: {version}"))

    try:
        digest = sha256_file(runtime.codex_binary)
        checks.append(
            _check(
                "codex_sha256",
                True,
                f"observed for diagnostics only: {digest}",
                required=False,
            )
        )
    except OSError:
        checks.append(_warning("codex_sha256", "diagnostic digest unavailable"))

    private_paths = (
        runtime.paths.home,
        runtime.paths.worker,
        runtime.paths.codex_home,
        runtime.paths.sterile_cwd,
        runtime.private_tmp,
        runtime.private_os_home,
    )
    paths_private = all(_path_mode_is_private(path) for path in private_paths)
    checks.append(_check("private_paths", paths_private, "worker directories are owner-only"))

    filevault_binary = Path("/usr/bin/fdesetup")
    if filevault_binary.is_file():
        try:
            filevault_result = _run(
                runner,
                (str(filevault_binary), "status"),
                runtime=runtime,
                timeout=policy.command_timeout_seconds,
            )
            filevault_enabled = (
                filevault_result.returncode == 0
                and b"filevault is on" in filevault_result.stdout.lower()
            )
        except OSError, subprocess.TimeoutExpired:
            filevault_enabled = False
        checks.append(
            _check(
                "filevault",
                True,
                "FileVault is enabled",
                required=False,
            )
            if filevault_enabled
            else _warning("filevault", "FileVault enabled state was not confirmed")
        )
    else:
        checks.append(_warning("filevault", "FileVault status tool is unavailable"))

    default_home = (Path.home() / ".codex").resolve()
    isolated_home = runtime.paths.codex_home.resolve() != default_home
    checks.append(
        _check("dedicated_codex_home", isolated_home, "interactive CODEX_HOME is not used")
    )

    environment = build_worker_environment(runtime)
    sensitive_names = {
        name
        for name in environment
        if any(marker in name.upper() for marker in ("TOKEN", "SECRET", "PASSWORD", "API_KEY"))
    }
    checks.append(
        _check(
            "environment_allowlist",
            not sensitive_names,
            "no credential-shaped environment names are inherited",
        )
    )

    # app-server accepts --strict-config and cleanly exits on stdio EOF without model/auth access.
    strict_command = (
        str(runtime.codex_binary),
        "app-server",
        "--strict-config",
        *config_override_arguments(),
        "--listen",
        "stdio://",
    )
    try:
        strict_result = _run(
            runner,
            strict_command,
            runtime=runtime,
            timeout=policy.command_timeout_seconds,
            input_bytes=b"",
        )
        strict_ok = strict_result.returncode == 0
    except OSError, subprocess.TimeoutExpired:
        strict_ok = False
    checks.append(_check("strict_config", strict_ok, "deny vector parsed in strict mode"))

    try:
        minimal_result = _run(
            runner,
            _permission_command(runtime, "/usr/bin/head", "-c", "1", "/usr/bin/env"),
            runtime=runtime,
            timeout=policy.command_timeout_seconds,
        )
        minimal_ok = minimal_result.returncode == 0 and len(minimal_result.stdout) == 1
    except OSError, subprocess.TimeoutExpired:
        minimal_ok = False
    checks.append(_check("minimal_read", minimal_ok, "minimal system root remains readable"))

    text_read_denied = False
    image_read_denied = False
    file_write_denied = False
    try:
        # The worker directory is part of Codex's ``:minimal`` runtime roots, so a sentinel
        # placed there would be readable by design.  Probe the actual data boundary instead:
        # the worker must never be able to read or modify memory/operations DB siblings.
        sentinel_parent = policy.sentinel_root or runtime.paths.data
        sentinel_parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        sentinel_parent.chmod(0o700)
        with tempfile.TemporaryDirectory(prefix="doctor-synthetic-", dir=sentinel_parent) as raw:
            sentinel_directory = Path(raw)
            sentinel_directory.chmod(0o700)
            if sentinel_directory.resolve().is_relative_to(runtime.paths.sterile_cwd.resolve()):
                raise ValueError("doctor sentinels must be outside the sterile cwd")
            text_path = sentinel_directory / "external.txt"
            image_path = sentinel_directory / "external.png"
            write_path = sentinel_directory / "write-target.txt"
            text_path.write_text("synthetic external text", encoding="utf-8")
            image_path.write_bytes(b"\x89PNG\r\n\x1a\nsynthetic")
            write_path.write_text("unchanged", encoding="utf-8")
            for path in (text_path, image_path, write_path):
                path.chmod(0o600)

            text_result = _run(
                runner,
                _permission_command(runtime, "/usr/bin/head", "-c", "1", str(text_path)),
                runtime=runtime,
                timeout=policy.command_timeout_seconds,
            )
            text_read_denied = text_result.returncode != 0 and not text_result.stdout

            image_result = _run(
                runner,
                _permission_command(runtime, "/usr/bin/head", "-c", "1", str(image_path)),
                runtime=runtime,
                timeout=policy.command_timeout_seconds,
            )
            image_read_denied = image_result.returncode != 0 and not image_result.stdout

            original_bytes = write_path.read_bytes()
            original_mtime = write_path.stat().st_mtime_ns
            write_result = _run(
                runner,
                _permission_command(runtime, "/usr/bin/touch", str(write_path)),
                runtime=runtime,
                timeout=policy.command_timeout_seconds,
            )
            file_write_denied = (
                write_result.returncode != 0
                and write_path.read_bytes() == original_bytes
                and write_path.stat().st_mtime_ns == original_mtime
            )
    except OSError, ValueError, subprocess.TimeoutExpired:
        pass
    checks.extend(
        (
            _check(
                "external_text_read_denied",
                text_read_denied,
                "synthetic external text read was denied",
            ),
            _check(
                "external_image_read_denied",
                image_read_denied,
                "synthetic external image read was denied",
            ),
            _check(
                "external_file_write_denied",
                file_write_denied,
                "synthetic external file modification was denied",
            ),
            _check(
                "file_change_event_fail_closed",
                _file_change_parser_is_fail_closed(),
                "file-change/apply-patch events invalidate worker output",
            ),
        )
    )

    network_socket: socket.socket | None = None
    try:
        network_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        network_socket.bind(("127.0.0.1", 0))
        network_socket.listen(1)
        port = network_socket.getsockname()[1]
        network_result = _run(
            runner,
            _permission_command(
                runtime,
                "/usr/bin/nc",
                "-z",
                "-w",
                "1",
                "127.0.0.1",
                str(port),
            ),
            runtime=runtime,
            timeout=policy.command_timeout_seconds,
        )
        network_ok = network_result.returncode != 0
    except OSError, subprocess.TimeoutExpired:
        network_ok = False
    finally:
        if network_socket is not None:
            network_socket.close()
    checks.append(_check("network_denied", network_ok, "sandboxed loopback connection was denied"))

    initial_forbidden_artifacts = _forbidden_worker_artifacts(runtime)
    checks.append(
        _check(
            "ephemeral_state_clean",
            not initial_forbidden_artifacts,
            (
                "no persisted rollout/history/memory records exist"
                if not initial_forbidden_artifacts
                else f"found {len(initial_forbidden_artifacts)} forbidden worker artifact(s)"
            ),
        )
    )

    if live_probe:
        probe_worker = worker or CodexWorker(runtime)
        package_root = Path(__file__).parent
        prompt_template = (package_root / "prompts" / "doctor_probe.txt").read_text()
        configurations = (
            ("phase1", models.phase1_model, models.phase1_reasoning),
            ("phase2", models.phase2_model, models.phase2_reasoning),
        )
        for phase_name, model_id, reasoning_effort in configurations:
            nonce = f"doctor-{phase_name}-" + uuid.uuid4().hex
            request = WorkerRequest(
                phase="doctor",
                model_id=model_id,
                reasoning_effort=reasoning_effort,
                prompt=prompt_template.replace("{{NONCE}}", nonce),
                schema_path=package_root / "schemas" / "doctor_probe.schema.json",
                timeout_seconds=120,
            )
            try:
                result = probe_worker.run(request)
                live_ok = result.payload == {"ok": True, "nonce": nonce}
                detail = (
                    "synthetic schema-bound probe passed" if live_ok else "probe payload mismatch"
                )
            except Exception as exc:
                live_ok = False
                detail = f"synthetic probe failed ({type(exc).__name__})"
            checks.append(_check(f"live_probe_{phase_name}", live_ok, detail))
        final_forbidden_artifacts = _forbidden_worker_artifacts(runtime)
        checks.append(
            _check(
                "live_ephemeral_state",
                not final_forbidden_artifacts,
                (
                    "live probes left no persisted rollout/history/memory records"
                    if not final_forbidden_artifacts
                    else f"live probes left {len(final_forbidden_artifacts)} forbidden artifact(s)"
                ),
            )
        )
    else:
        for phase_name in ("phase1", "phase2"):
            checks.append(
                DoctorCheck(
                    f"live_probe_{phase_name}",
                    CheckStatus.WARN,
                    "not requested; offline checks cannot certify a live model call",
                    required=False,
                )
            )
        checks.append(
            DoctorCheck(
                "live_ephemeral_state",
                CheckStatus.WARN,
                "not requested; post-probe artifact absence was not tested",
                required=False,
            )
        )

    return DoctorReport(
        checks=tuple(checks),
        live_probe_requested=live_probe,
        codex_version=version if version_observed else None,
    )


def certified_vector_sha256() -> str:
    """Return a stable identifier for the exact worker deny vector under test."""

    vector = "\n".join(config_override_arguments()).encode()
    return hashlib.sha256(vector).hexdigest()


def _write_private_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix(f".tmp-{os.getpid()}-{uuid.uuid4().hex}")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(0o600)
    os.replace(temporary, path)


def write_certification_failure(
    path: Path,
    *,
    report: DoctorReport,
    now: datetime | None = None,
) -> Path:
    """Persist the latest failed live probe without destroying the last good stamp."""

    if report.certified:
        raise ValueError("a failed live doctor report is required")
    failed_at = (now or datetime.now(UTC)).astimezone(UTC)
    failed_checks = [
        {
            "name": check.name,
            "status": check.status.value,
            "detail": check.detail[:500],
        }
        for check in report.checks
        if check.required and check.status is not CheckStatus.PASS
    ]
    _write_private_json(
        path,
        {
            "schema_version": 1,
            "failed_at": failed_at.isoformat(),
            "failed_checks": failed_checks,
        },
    )
    return path


def write_certification_stamp(
    path: Path,
    *,
    report: DoctorReport,
    runtime: WorkerRuntime,
    models: ModelSettings,
    now: datetime | None = None,
    ttl: timedelta = timedelta(days=7),
) -> CertificationStamp:
    """Atomically persist non-secret certification metadata after both live probes pass."""

    if not report.certified:
        raise ValueError("a passing live doctor report is required")
    if report.codex_version is None:
        raise ValueError("a certified report must include the observed Codex version")
    if ttl <= timedelta(0):
        raise ValueError("certification TTL must be positive")
    issued = (now or datetime.now(UTC)).astimezone(UTC)
    stamp = CertificationStamp(
        schema_version=1,
        config_vector_sha256=certified_vector_sha256(),
        codex_version=report.codex_version,
        phase1_model=models.phase1_model,
        phase1_reasoning=models.phase1_reasoning,
        phase2_model=models.phase2_model,
        phase2_reasoning=models.phase2_reasoning,
        certified_at=issued,
        expires_at=issued + ttl,
    )
    payload = {
        "schema_version": stamp.schema_version,
        "config_vector_sha256": stamp.config_vector_sha256,
        "codex_version": stamp.codex_version,
        "phase1_model": stamp.phase1_model,
        "phase1_reasoning": stamp.phase1_reasoning,
        "phase2_model": stamp.phase2_model,
        "phase2_reasoning": stamp.phase2_reasoning,
        "certified_at": stamp.certified_at.isoformat(),
        "expires_at": stamp.expires_at.isoformat(),
    }
    _write_private_json(path, payload)
    return stamp


def verify_certification_stamp(
    path: Path,
    *,
    runtime: WorkerRuntime,
    models: ModelSettings,
    now: datetime | None = None,
) -> bool:
    """Fail closed on expiry or any binary, deny-vector, or model-config drift."""

    try:
        if path.is_symlink() or (path.stat().st_mode & 0o077) != 0:
            return False
        raw = json.loads(path.read_text())
        if not isinstance(raw, dict):
            return False
        expected_keys = {
            "schema_version",
            "config_vector_sha256",
            "codex_version",
            "phase1_model",
            "phase1_reasoning",
            "phase2_model",
            "phase2_reasoning",
            "certified_at",
            "expires_at",
        }
        if set(raw) != expected_keys:
            return False
        certified_at_raw = datetime.fromisoformat(str(raw["certified_at"]))
        expires_at_raw = datetime.fromisoformat(str(raw["expires_at"]))
        if (
            certified_at_raw.tzinfo is None
            or certified_at_raw.utcoffset() is None
            or expires_at_raw.tzinfo is None
            or expires_at_raw.utcoffset() is None
        ):
            return False
        certified_at = certified_at_raw.astimezone(UTC)
        expires_at = expires_at_raw.astimezone(UTC)
        current_time = (now or datetime.now(UTC)).astimezone(UTC)
        failure_path = path.with_name("doctor-last-failure.json")
        if failure_path.exists():
            if failure_path.is_symlink() or (failure_path.stat().st_mode & 0o077) != 0:
                return False
            failure = json.loads(failure_path.read_text(encoding="utf-8"))
            if not isinstance(failure, dict) or set(failure) != {
                "schema_version",
                "failed_at",
                "failed_checks",
            }:
                return False
            failed_at_raw = datetime.fromisoformat(str(failure["failed_at"]))
            if failed_at_raw.tzinfo is None or failed_at_raw.utcoffset() is None:
                return False
            if failed_at_raw.astimezone(UTC) >= certified_at:
                return False
        runtime.prepare()
        version_result = subprocess.run(
            (str(runtime.codex_binary), "--version"),
            input=None,
            capture_output=True,
            cwd=runtime.paths.sterile_cwd,
            env=build_worker_environment(runtime),
            timeout=10,
            check=False,
        )
        current_version = version_result.stdout.decode("utf-8", errors="strict").strip()
        return (
            raw["schema_version"] == 1
            and raw["config_vector_sha256"] == certified_vector_sha256()
            and version_result.returncode == 0
            and bool(current_version)
            and raw["codex_version"] == current_version
            and raw["phase1_model"] == models.phase1_model
            and raw["phase1_reasoning"] == models.phase1_reasoning
            and raw["phase2_model"] == models.phase2_model
            and raw["phase2_reasoning"] == models.phase2_reasoning
            and certified_at <= current_time
            and certified_at < expires_at
            and current_time < expires_at
        )
    except (
        OSError,
        ValueError,
        UnicodeDecodeError,
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
    ):
        return False

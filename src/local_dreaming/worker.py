from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from local_dreaming.config import RuntimePaths
from local_dreaming.errors import ConfigurationError, WorkerProtocolError

REASONING_LEVELS = frozenset({"low", "medium", "high", "xhigh", "max", "ultra"})
_MODEL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
DEFAULT_CODEX_BINARY = Path("/opt/homebrew/bin/codex")

# This vector is deliberately smaller than a dump of every Codex feature flag. It disables every
# currently relevant external-context or effectful surface while the permission profile remains
# the actual OS-enforced boundary. The vector is certified per Codex binary by doctor.py.
CODEX_CONFIG_DENY_VECTOR: tuple[str, ...] = (
    'approval_policy="never"',
    'default_permissions="dream_worker"',
    'permissions.dream_worker.filesystem.:root="deny"',
    'permissions.dream_worker.filesystem.:minimal="read"',
    "permissions.dream_worker.network.enabled=false",
    "mcp_servers={}",
    'model_reasoning_summary="none"',
    'web_search="disabled"',
    "tools.web_search=false",
    "features.apps=false",
    "features.auth_elicitation=false",
    "features.browser_use=false",
    "features.browser_use_external=false",
    "features.browser_use_full_cdp_access=false",
    "features.chronicle=false",
    "features.code_mode=false",
    "features.code_mode_host=false",
    "features.computer_use=false",
    "features.goals=false",
    "features.hooks=false",
    "features.image_generation=false",
    "features.in_app_browser=false",
    "features.memories=false",
    "features.multi_agent=false",
    "features.network_proxy=false",
    "features.plugins=false",
    "features.remote_plugin=false",
    "features.request_permissions_tool=false",
    "features.shell_snapshot=false",
    "features.shell_tool=false",
    "features.skill_mcp_dependency_install=false",
    "features.tool_call_mcp_elicitation=false",
    "features.tool_suggest=false",
    "features.unified_exec=false",
    "features.workspace_dependencies=false",
    "memories.generate_memories=false",
    "memories.use_memories=false",
    'history.persistence="none"',
    "hide_agent_reasoning=true",
    "feedback.enabled=false",
    "analytics.enabled=false",
    'otel.exporter="none"',
    'otel.metrics_exporter="none"',
    'otel.trace_exporter="none"',
    "otel.log_user_prompt=false",
    "check_for_update_on_startup=false",
)

_PASSTHROUGH_ENVIRONMENT = frozenset(
    {
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TERM",
        "TZ",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    }
)

_SAFE_EVENT_TYPES = frozenset(
    {
        "thread.started",
        "turn.started",
        "item.started",
        "item.updated",
        "item.completed",
        "turn.completed",
    }
)
_SAFE_ITEM_TYPES = frozenset({"agent_message", "reasoning"})
_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
)


class ModelCallLog(Protocol):
    """A sink that must persist metadata only, never prompts or model output."""

    def record_model_call(self, record: ModelCallRecord) -> None: ...


@dataclass(frozen=True, slots=True)
class WorkerUsage:
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    reasoning_output_tokens: int

    @property
    def total_output_tokens(self) -> int:
        return self.output_tokens + self.reasoning_output_tokens


@dataclass(frozen=True, slots=True)
class ModelCallRecord:
    """Safe operational telemetry; intentionally has no prompt/output fields."""

    call_id: str
    job_id: str | None
    phase: str
    model_id: str
    reasoning_effort: str
    prompt_sha256: str
    schema_sha256: str
    config_vector_sha256: str
    input_bytes: int
    stdout_bytes: int
    stderr_bytes: int
    duration_ms: int
    status: str
    exit_code: int | None
    usage: WorkerUsage | None = None


@dataclass(frozen=True, slots=True)
class WorkerRequest:
    phase: str
    model_id: str
    reasoning_effort: str
    prompt: str
    schema_path: Path
    timeout_seconds: float = 300.0
    job_id: str | None = None
    call_id: str | None = None


@dataclass(frozen=True, slots=True)
class WorkerResult:
    call_id: str
    thread_id: str
    payload: Any
    usage: WorkerUsage
    record: ModelCallRecord


@dataclass(frozen=True, slots=True)
class ParsedWorkerOutput:
    thread_id: str
    payload: Any
    usage: WorkerUsage


@dataclass(frozen=True, slots=True)
class WorkerRuntime:
    paths: RuntimePaths
    # The effective permission profile is certified; v2.2 intentionally does not pin a binary hash.
    codex_binary: Path = DEFAULT_CODEX_BINARY
    max_stdout_bytes: int = 8 * 1024 * 1024
    max_stderr_bytes: int = 2 * 1024 * 1024
    base_environment: Mapping[str, str] = field(default_factory=lambda: dict(os.environ))

    @property
    def private_tmp(self) -> Path:
        return self.paths.worker / "tmp"

    @property
    def private_os_home(self) -> Path:
        return self.paths.worker / "os-home"

    def prepare(self) -> None:
        self.paths.ensure()
        for private_directory in (self.private_tmp, self.private_os_home):
            private_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            private_directory.chmod(0o700)
        validate_sterile_cwd(self.paths.sterile_cwd)
        default_codex_home = Path.home() / ".codex"
        if self.paths.codex_home.resolve() == default_codex_home.resolve():
            raise ConfigurationError("worker CODEX_HOME must not be the interactive ~/.codex")


def _quoted_toml(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def _validate_request(request: WorkerRequest) -> None:
    if not _MODEL_ID.fullmatch(request.model_id):
        raise ConfigurationError("model_id contains unsupported characters")
    if request.reasoning_effort not in REASONING_LEVELS:
        choices = ", ".join(sorted(REASONING_LEVELS))
        raise ConfigurationError(f"unsupported reasoning effort; expected one of: {choices}")
    if not request.schema_path.is_file():
        raise ConfigurationError(f"output schema does not exist: {request.schema_path}")
    if request.timeout_seconds <= 0:
        raise ConfigurationError("worker timeout must be positive")


def build_codex_command(runtime: WorkerRuntime, request: WorkerRequest) -> tuple[str, ...]:
    """Build a shell-free, binary-pinned argv for an isolated Codex extraction call."""

    _validate_request(request)
    overrides = (
        *CODEX_CONFIG_DENY_VECTOR,
        f"model_reasoning_effort={_quoted_toml(request.reasoning_effort)}",
    )
    command: list[str] = [
        str(runtime.codex_binary),
        "exec",
        "--strict-config",
        "--skip-git-repo-check",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--json",
        "--color",
        "never",
        "--output-schema",
        str(request.schema_path.resolve()),
        "--model",
        request.model_id,
        "-C",
        str(runtime.paths.sterile_cwd.resolve()),
    ]
    for override in overrides:
        command.extend(("-c", override))
    # '-' means the entire prompt comes from stdin. No private text is interpolated into argv.
    command.append("-")
    return tuple(command)


def build_worker_environment(runtime: WorkerRuntime) -> dict[str, str]:
    """Return an allowlisted environment without API keys, proxy credentials, or agent state."""

    environment = {
        key: value
        for key, value in runtime.base_environment.items()
        if key in _PASSTHROUGH_ENVIRONMENT
    }
    environment["PATH"] = "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin"
    environment["HOME"] = str(runtime.private_os_home.resolve())
    environment["USER"] = "dream-worker"
    environment["LOGNAME"] = "dream-worker"
    environment["SHELL"] = "/bin/zsh"
    environment.setdefault("TERM", "dumb")
    environment["CODEX_HOME"] = str(runtime.paths.codex_home.resolve())
    environment["TMPDIR"] = str(runtime.private_tmp.resolve())
    return environment


def validate_sterile_cwd(path: Path) -> None:
    if not path.is_dir():
        raise ConfigurationError(f"sterile cwd does not exist: {path}")
    forbidden = (".git", "AGENTS.md", "AGENTS.override.md", "CLAUDE.md")
    present = [name for name in forbidden if (path / name).exists()]
    if present:
        raise ConfigurationError(f"sterile cwd contains instruction/repository markers: {present}")
    unexpected = [entry.name for entry in path.iterdir()]
    if unexpected:
        raise ConfigurationError("sterile cwd must be empty")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _parse_usage(raw: Any) -> WorkerUsage:
    if not isinstance(raw, dict):
        raise WorkerProtocolError("turn.completed is missing usage")
    values: dict[str, int] = {}
    for field_name in _USAGE_FIELDS:
        # Codex 0.144.x does not always report reasoning_output_tokens separately.
        value = raw.get(field_name, 0 if field_name == "reasoning_output_tokens" else None)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise WorkerProtocolError(f"usage.{field_name} must be a non-negative integer")
        values[field_name] = value
    return WorkerUsage(**values)


def _item_text(item: Mapping[str, Any]) -> str | None:
    text = item.get("text")
    if isinstance(text, str):
        return text
    content = item.get("content")
    if isinstance(content, str):
        return content
    return None


def parse_codex_jsonl(
    stdout: bytes | str,
    schema: Mapping[str, Any],
    *,
    max_bytes: int = 8 * 1024 * 1024,
) -> ParsedWorkerOutput:
    """Parse Codex JSONL with an allowlist; any tool/unknown event invalidates the call."""

    raw_bytes = stdout if isinstance(stdout, bytes) else stdout.encode("utf-8")
    if len(raw_bytes) > max_bytes:
        raise WorkerProtocolError("Codex JSONL exceeded the configured in-memory limit")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkerProtocolError("Codex JSONL is not UTF-8") from exc

    thread_id: str | None = None
    final_messages: list[str] = []
    usage: WorkerUsage | None = None
    thread_started_count = 0
    turn_started_count = 0
    turn_completed_count = 0

    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise WorkerProtocolError(f"invalid JSONL at line {line_number}") from exc
        if not isinstance(event, dict):
            raise WorkerProtocolError(f"JSONL event at line {line_number} is not an object")
        event_type = event.get("type")
        if event_type not in _SAFE_EVENT_TYPES:
            raise WorkerProtocolError(f"unsafe or unknown Codex event: {event_type!r}")
        if turn_completed_count:
            raise WorkerProtocolError("Codex emitted an event after turn.completed")

        if event_type == "thread.started":
            thread_started_count += 1
            if thread_started_count > 1 or turn_started_count:
                raise WorkerProtocolError("thread.started must be the first and only thread event")
            candidate = event.get("thread_id")
            if not isinstance(candidate, str) or not candidate:
                raise WorkerProtocolError("thread.started is missing thread_id")
            if thread_id is not None and thread_id != candidate:
                raise WorkerProtocolError("multiple Codex thread IDs were emitted")
            thread_id = candidate
            continue

        if event_type == "turn.started":
            turn_started_count += 1
            if thread_started_count != 1 or turn_started_count > 1:
                raise WorkerProtocolError("turn.started must follow exactly one thread.started")
            continue

        if event_type.startswith("item."):
            if turn_started_count != 1:
                raise WorkerProtocolError(f"{event_type} was emitted before turn.started")
            item = event.get("item")
            if not isinstance(item, dict):
                raise WorkerProtocolError(f"{event_type} is missing item")
            item_type = item.get("type")
            if item_type not in _SAFE_ITEM_TYPES:
                raise WorkerProtocolError(f"Codex attempted a forbidden tool/item: {item_type!r}")
            if event_type == "item.completed" and item_type == "agent_message":
                message = _item_text(item)
                if message is None:
                    raise WorkerProtocolError("completed agent_message is missing text")
                final_messages.append(message)
            continue

        if event_type == "turn.completed":
            if turn_started_count != 1:
                raise WorkerProtocolError("turn.completed was emitted before turn.started")
            turn_completed_count += 1
            if turn_completed_count > 1:
                raise WorkerProtocolError("multiple turn.completed events were emitted")
            usage = _parse_usage(event.get("usage"))

    if thread_id is None:
        raise WorkerProtocolError("Codex JSONL did not emit thread.started")
    if usage is None:
        raise WorkerProtocolError("Codex JSONL did not emit turn.completed usage")
    if not final_messages:
        raise WorkerProtocolError("Codex JSONL did not emit a completed agent_message")

    final_message = final_messages[-1]
    try:
        payload = json.loads(final_message)
    except json.JSONDecodeError as exc:
        raise WorkerProtocolError("final agent_message is not JSON") from exc
    validate_json_schema(payload, schema)
    return ParsedWorkerOutput(thread_id=thread_id, payload=payload, usage=usage)


def _matches_type(instance: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(instance, dict)
    if expected == "array":
        return isinstance(instance, list)
    if expected == "string":
        return isinstance(instance, str)
    if expected == "integer":
        return isinstance(instance, int) and not isinstance(instance, bool)
    if expected == "number":
        return isinstance(instance, (int, float)) and not isinstance(instance, bool)
    if expected == "boolean":
        return isinstance(instance, bool)
    if expected == "null":
        return instance is None
    raise WorkerProtocolError(f"unsupported JSON Schema type: {expected}")


def validate_json_schema(instance: Any, schema: Mapping[str, Any], path: str = "$") -> None:
    """Validate the strict JSON-Schema subset used by bundled worker schemas."""

    if "const" in schema and instance != schema["const"]:
        raise WorkerProtocolError(f"{path} does not match schema const")
    if "enum" in schema:
        enum = schema["enum"]
        if not isinstance(enum, list) or instance not in enum:
            raise WorkerProtocolError(f"{path} is not in the schema enum")

    if "oneOf" in schema:
        variants = schema["oneOf"]
        if not isinstance(variants, list):
            raise WorkerProtocolError("schema oneOf must be an array")
        matches = 0
        for variant in variants:
            try:
                validate_json_schema(instance, variant, path)
            except WorkerProtocolError:
                continue
            matches += 1
        if matches != 1:
            raise WorkerProtocolError(f"{path} must match exactly one oneOf branch")
        return

    if "anyOf" in schema:
        variants = schema["anyOf"]
        if not isinstance(variants, list):
            raise WorkerProtocolError("schema anyOf must be an array")
        for variant in variants:
            try:
                validate_json_schema(instance, variant, path)
                return
            except WorkerProtocolError:
                continue
        raise WorkerProtocolError(f"{path} does not match any anyOf branch")

    expected_type = schema.get("type")
    if isinstance(expected_type, list):
        if not any(_matches_type(instance, item) for item in expected_type):
            raise WorkerProtocolError(f"{path} has the wrong JSON type")
    elif isinstance(expected_type, str) and not _matches_type(instance, expected_type):
        raise WorkerProtocolError(f"{path} must be {expected_type}")

    if isinstance(instance, dict):
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        if not isinstance(properties, dict) or not isinstance(required, list):
            raise WorkerProtocolError("schema object properties/required are malformed")
        missing = [key for key in required if key not in instance]
        if missing:
            raise WorkerProtocolError(f"{path} is missing required keys: {missing}")
        additional = schema.get("additionalProperties", True)
        if additional is False:
            unknown = sorted(set(instance) - set(properties))
            if unknown:
                raise WorkerProtocolError(f"{path} contains unknown keys: {unknown}")
        for key, value in instance.items():
            child_schema = properties.get(key)
            if isinstance(child_schema, dict):
                validate_json_schema(value, child_schema, f"{path}.{key}")

    if isinstance(instance, list):
        min_items = schema.get("minItems")
        max_items = schema.get("maxItems")
        if isinstance(min_items, int) and len(instance) < min_items:
            raise WorkerProtocolError(f"{path} has fewer than minItems")
        if isinstance(max_items, int) and len(instance) > max_items:
            raise WorkerProtocolError(f"{path} has more than maxItems")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, value in enumerate(instance):
                validate_json_schema(value, item_schema, f"{path}[{index}]")

    if isinstance(instance, str):
        min_length = schema.get("minLength")
        max_length = schema.get("maxLength")
        if isinstance(min_length, int) and len(instance) < min_length:
            raise WorkerProtocolError(f"{path} is shorter than minLength")
        if isinstance(max_length, int) and len(instance) > max_length:
            raise WorkerProtocolError(f"{path} is longer than maxLength")
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.search(pattern, instance) is None:
            raise WorkerProtocolError(f"{path} does not match schema pattern")

    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if isinstance(minimum, (int, float)) and instance < minimum:
            raise WorkerProtocolError(f"{path} is below schema minimum")
        if isinstance(maximum, (int, float)) and instance > maximum:
            raise WorkerProtocolError(f"{path} is above schema maximum")


class CodexWorker:
    def __init__(
        self,
        runtime: WorkerRuntime,
        *,
        call_log: ModelCallLog | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.runtime = runtime
        self.call_log = call_log
        self.clock = clock

    def run(self, request: WorkerRequest) -> WorkerResult:
        self.runtime.prepare()
        command = build_codex_command(self.runtime, request)
        environment = build_worker_environment(self.runtime)
        schema_bytes = request.schema_path.read_bytes()
        try:
            schema = json.loads(schema_bytes)
        except json.JSONDecodeError as exc:
            raise ConfigurationError("worker output schema is not valid JSON") from exc
        if not isinstance(schema, dict):
            raise ConfigurationError("worker output schema must be a JSON object")

        call_id = request.call_id or str(uuid.uuid4())
        prompt_bytes = request.prompt.encode("utf-8")
        vector_hash = _sha256_text("\n".join(command))
        started = self.clock()
        exit_code: int | None = None
        stdout = b""
        stderr = b""
        status = "failed"
        parsed: ParsedWorkerOutput | None = None
        try:
            completed = subprocess.run(
                command,
                input=prompt_bytes,
                capture_output=True,
                cwd=self.runtime.paths.sterile_cwd,
                env=environment,
                timeout=request.timeout_seconds,
                check=False,
            )
            exit_code = completed.returncode
            stdout = completed.stdout
            stderr = completed.stderr
            if len(stderr) > self.runtime.max_stderr_bytes:
                raise WorkerProtocolError("Codex stderr exceeded the in-memory limit")
            if exit_code != 0:
                raise WorkerProtocolError(
                    f"Codex worker exited non-zero (code={exit_code}, stderr_bytes={len(stderr)})"
                )
            parsed = parse_codex_jsonl(
                stdout,
                schema,
                max_bytes=self.runtime.max_stdout_bytes,
            )
            status = "completed"
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout or b""
            stderr = exc.stderr or b""
            raise WorkerProtocolError("Codex worker timed out") from exc
        finally:
            duration_ms = max(0, int((self.clock() - started) * 1000))
            record = ModelCallRecord(
                call_id=call_id,
                job_id=request.job_id,
                phase=request.phase,
                model_id=request.model_id,
                reasoning_effort=request.reasoning_effort,
                prompt_sha256=hashlib.sha256(prompt_bytes).hexdigest(),
                schema_sha256=hashlib.sha256(schema_bytes).hexdigest(),
                config_vector_sha256=vector_hash,
                input_bytes=len(prompt_bytes),
                stdout_bytes=len(stdout),
                stderr_bytes=len(stderr),
                duration_ms=duration_ms,
                status=status,
                exit_code=exit_code,
                usage=parsed.usage if parsed is not None else None,
            )
            if self.call_log is not None:
                self.call_log.record_model_call(record)

        if parsed is None:  # pragma: no cover - all failure paths raise before this point.
            raise WorkerProtocolError("Codex worker returned no parsed result")
        return WorkerResult(
            call_id=call_id,
            thread_id=parsed.thread_id,
            payload=parsed.payload,
            usage=parsed.usage,
            record=record,
        )


def config_override_arguments(vector: Sequence[str] = CODEX_CONFIG_DENY_VECTOR) -> tuple[str, ...]:
    """Expose deterministic -c arguments for doctor without duplicating command policy."""

    arguments: list[str] = []
    for value in vector:
        arguments.extend(("-c", value))
    return tuple(arguments)

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from enum import Enum, StrEnum
from pathlib import Path
from typing import Annotated, Any

import typer

from local_dreaming.application import CanonicalApplicationService
from local_dreaming.artifacts import current_bundle
from local_dreaming.compiler import compile_memory
from local_dreaming.config import RuntimePaths, Settings
from local_dreaming.database import check_integrity, connect_memory, connect_operations
from local_dreaming.doctor import (
    run_doctor,
    verify_certification_stamp,
    write_certification_failure,
    write_certification_stamp,
)
from local_dreaming.evals import load_cases, run_evals
from local_dreaming.forgetting import (
    ForgottenLedger,
    apply_forgotten_ledger,
    forget_claim,
    forget_source,
)
from local_dreaming.ingest import IngestService
from local_dreaming.installation_provenance import build_installation_provenance
from local_dreaming.mcp_server import serve_stdio
from local_dreaming.models import IngestEventInput, Sensitivity, SourceKind, SourcePolicy
from local_dreaming.nightly import NightlyStatus
from local_dreaming.orchestration import (
    build_nightly_runner,
    build_pilot_runner,
    enqueue_phase1_jobs,
    enqueue_phase2_jobs,
    plan_pilot_scope,
    segment_pending_events,
)
from local_dreaming.queue_reconciliation import (
    reactivate_phase1_barriers,
    reconcile_phase1_queue,
)
from local_dreaming.retrieval import RetrievalService
from local_dreaming.review import ReviewService, fingerprint_head_set, validate_proposal_binding
from local_dreaming.scheduler import (
    DEFAULT_HOUR,
    DEFAULT_MINUTE,
    DOCTOR_HOUR,
    DOCTOR_LABEL,
    DOCTOR_MINUTE,
    ScheduleScanError,
    bootstrap_launch_agent,
    find_openclaw_schedule_conflicts,
    find_schedule_conflicts,
    render_doctor_launch_agent,
    render_launch_agent,
    write_launch_agent,
)
from local_dreaming.scheduler import (
    LABEL as NIGHTLY_LAUNCHD_LABEL,
)
from local_dreaming.source_adapters import (
    AdvisoryMarkdownAdapter,
    ChronicleSummaryAdapter,
    CodexSessionAdapter,
    SourceScanResult,
    build_automation_snapshot,
    capture_health_snapshot,
    capture_workspace_snapshot,
)
from local_dreaming.storage import (
    ClaimVersionInput,
    MemoryStore,
    OperationsStore,
    SnapshotManager,
    fingerprint,
    identity_fingerprint,
    memory_maintenance_lock,
    operations_maintenance_lock,
    restore_database,
)
from local_dreaming.temporal import propose_expired_plan_outcomes
from local_dreaming.worker import DEFAULT_CODEX_BINARY, WorkerRuntime

app = typer.Typer(
    name="dream",
    help="Practical local memory for Codex with explicit human approval.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)
review_app = typer.Typer(help="Inspect and decide proposed memory changes.")
snapshot_app = typer.Typer(help="Create or restore managed local snapshots.")
mcp_app = typer.Typer(help="Serve the read-only MCP interface.")
schedule_app = typer.Typer(help="Preview or install the gated local nightly worker.")
app.add_typer(review_app, name="review")
app.add_typer(snapshot_app, name="snapshot")
app.add_typer(mcp_app, name="mcp")
app.add_typer(schedule_app, name="schedule")


class IngestAdapter(StrEnum):
    MANUAL = "manual"
    CODEX_SESSION = "codex-session"
    CHRONICLE = "chronicle"
    CODEX_MEMORY = "codex-memory"
    OPERATIONS = "operations"


class ScheduleJob(StrEnum):
    NIGHTLY = "nightly"
    DOCTOR = "doctor"


def _settings() -> Settings:
    os.umask(0o077)
    return Settings(paths=RuntimePaths())


def _pilot_worker_paths(settings: Settings, override: Path | None) -> RuntimePaths:
    if override is None:
        return settings.paths
    production_home = (Path.home() / "Library" / "Application Support" / "Local-Dreaming").resolve()
    if settings.paths.home.resolve() == production_home:
        raise typer.BadParameter("worker runtime override is only permitted for an isolated clone")
    selected = override.expanduser().resolve()
    if selected != production_home:
        raise typer.BadParameter("clone pilot worker runtime must be the production worker home")
    return RuntimePaths(home=selected)


def _json_default(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, (Path, datetime)):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple | set):
        return list(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _emit(payload: object, *, json_output: bool = False) -> None:
    if json_output:
        typer.echo(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=_json_default,
            )
        )
        return
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))


def _stores(settings: Settings) -> tuple[MemoryStore, OperationsStore]:
    return MemoryStore(settings.paths.memory_db), OperationsStore(settings.paths.operations_db)


def _require_valid_proposal_binding(proposal: dict[str, Any]) -> None:
    try:
        validate_proposal_binding(proposal)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _verified_manual_run_count(operations: OperationsStore) -> int:
    with operations.connection() as connection:
        return int(
            connection.execute(
                """
                SELECT COUNT(*) FROM nightly_runs
                WHERE status = 'completed'
                  AND episode_count > 0
                  AND model_calls > 0
                  AND input_tokens > 0
                """
            ).fetchone()[0]
        )


def _certification_expiry(path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return None
    value = payload.get("expires_at") if isinstance(payload, dict) else None
    return str(value) if isinstance(value, str) else None


def _doctor_failure_summary(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("failed_at"), str):
        return None
    checks = payload.get("failed_checks")
    names = (
        [str(item.get("name")) for item in checks if isinstance(item, dict)]
        if isinstance(checks, list)
        else []
    )
    return {"failed_at": payload["failed_at"], "failed_checks": names}


def _schedule_scan(
    *,
    hour: int,
    minute: int,
    strict: bool,
    exclude_labels: set[str] | None = None,
) -> tuple[list[Any], list[Any], list[str]]:
    errors: list[str] = []
    try:
        launchd = find_schedule_conflicts(
            Path.home() / "Library" / "LaunchAgents",
            hour=hour,
            minute=minute,
            strict=strict,
            exclude_labels=exclude_labels or {NIGHTLY_LAUNCHD_LABEL, DOCTOR_LABEL},
        )
    except ScheduleScanError as exc:
        launchd = []
        errors.append(str(exc))
    try:
        openclaw = find_openclaw_schedule_conflicts(
            Path.home() / ".openclaw" / "state" / "openclaw.sqlite",
            hour=hour,
            minute=minute,
            strict=strict,
        )
    except ScheduleScanError as exc:
        openclaw = []
        errors.append(str(exc))
    return launchd, openclaw, errors


def _retrieval(settings: Settings) -> RetrievalService:
    return RetrievalService(settings.paths.memory_db, settings.paths.operations_db)


def _parse_timestamp(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise typer.BadParameter("timestamp must include a timezone")
    return parsed


def _parse_json_value(value: str) -> object:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _read_ingest_records(path: Path) -> list[dict[str, Any]]:
    if path.is_symlink():
        raise typer.BadParameter("manual ingest does not follow symlinks")
    if not path.is_file():
        raise typer.BadParameter(f"input file does not exist: {path}")
    maximum_bytes = 8 * 1024 * 1024
    with path.open("rb") as handle:
        data_bytes = handle.read(maximum_bytes + 1)
    if len(data_bytes) > maximum_bytes:
        raise typer.BadParameter("manual ingest file exceeds the 8 MiB bound")
    try:
        text = data_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise typer.BadParameter("manual ingest input must be UTF-8") from exc
    if path.suffix.casefold() != ".jsonl":
        if len(text) > 64_000:
            raise typer.BadParameter("manual ingest content exceeds 64,000 characters")
        return [{"content": text}]
    records: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        if line_number > 20_000:
            raise typer.BadParameter("manual ingest JSONL exceeds 20,000 records")
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"invalid JSONL at line {line_number}") from exc
        if not isinstance(value, dict) or not isinstance(value.get("content"), str):
            raise typer.BadParameter(f"line {line_number} requires a string content field")
        if len(value["content"]) > 64_000:
            raise typer.BadParameter(f"line {line_number} content exceeds 64,000 characters")
        records.append(value)
    return records


def _source_trust(kind: SourceKind) -> str:
    if kind is SourceKind.USER_DIRECT:
        return "user_direct"
    if kind is SourceKind.MANUAL:
        return "manual"
    if kind is SourceKind.TOOL_RESULT:
        return "tool_verified"
    if kind in {SourceKind.CHRONICLE, SourceKind.CODEX_MEMORY}:
        return "advisory"
    return "conversation"


def _scalar_metadata(value: object) -> dict[str, str | int | float | bool | None]:
    if not isinstance(value, dict):
        return {}
    return {
        str(key): item
        for key, item in value.items()
        if isinstance(item, str | int | float | bool) or item is None
    }


def _latest_operations_snapshot_summaries(
    connection: sqlite3.Connection,
) -> dict[str, dict[str, Any] | None]:
    workspace = connection.execute(
        """
        SELECT captured_at, git_branch, git_dirty_count, payload_json
        FROM workspace_snapshots
        ORDER BY captured_at DESC, snapshot_id DESC LIMIT 1
        """
    ).fetchone()
    automation = connection.execute(
        """
        SELECT automation_type, status, captured_at, payload_json
        FROM automation_snapshots
        ORDER BY captured_at DESC, snapshot_id DESC LIMIT 1
        """
    ).fetchone()
    health = connection.execute(
        """
        SELECT status, captured_at, payload_json
        FROM health_snapshots
        ORDER BY captured_at DESC, snapshot_id DESC LIMIT 1
        """
    ).fetchone()

    workspace_summary: dict[str, Any] | None = None
    if workspace is not None:
        payload = _scalar_metadata(json.loads(str(workspace["payload_json"])))
        workspace_summary = {
            "captured_at": workspace["captured_at"],
            "status": (
                "available"
                if payload.get("exists") is True and payload.get("is_symlink") is not True
                else "unavailable"
            ),
            "git_branch": workspace["git_branch"],
            "git_dirty_count": workspace["git_dirty_count"],
        }

    automation_summary: dict[str, Any] | None = None
    if automation is not None:
        payload = _scalar_metadata(json.loads(str(automation["payload_json"])))
        automation_summary = {
            "captured_at": automation["captured_at"],
            "status": automation["status"],
            "automation_type": automation["automation_type"],
            "installed": payload.get("installed"),
        }

    health_summary: dict[str, Any] | None = None
    if health is not None:
        payload = _scalar_metadata(json.loads(str(health["payload_json"])))
        health_summary = {
            "captured_at": health["captured_at"],
            "status": health["status"],
            "available_count": payload.get("available_count"),
            "check_count": payload.get("check_count"),
        }

    return {
        "workspace": workspace_summary,
        "automation": automation_summary,
        "health": health_summary,
    }


def _persist_scan_result(
    result: SourceScanResult,
    *,
    settings: Settings,
    dry_run: bool,
) -> dict[str, Any]:
    needs_memory = bool(result.sources or result.events or result.collapsed_observations)
    memory = None if dry_run or not needs_memory else MemoryStore(settings.paths.memory_db)
    service = IngestService(memory)
    policies: dict[str, SourcePolicy] = {}
    ledger = (
        ForgottenLedger(settings.paths.forgotten_log)
        if settings.paths.forgotten_log.exists() or not dry_run
        else None
    )
    suppressed_sources: set[str] = set()
    registered_source_ids: list[str] = []
    registered_partition_ids: list[str] = []
    registered_sources: set[str] = set()
    registered_partitions: set[str] = set()
    for descriptor in result.sources:
        if ledger is not None and ledger.is_source_forgotten(descriptor.source_fingerprint):
            suppressed_sources.add(descriptor.source_id)
            continue
        descriptor_policy = descriptor.policy()
        existing_policy = policies.get(descriptor.source_id)
        if existing_policy is not None and existing_policy != descriptor_policy:
            raise ValueError("adapter partitions disagree on their source policy")
        policies[descriptor.source_id] = descriptor_policy
        if descriptor.source_id not in registered_sources:
            registered_sources.add(descriptor.source_id)
            registered_source_ids.append(descriptor.source_id)
        if descriptor.partition_id not in registered_partitions:
            registered_partitions.add(descriptor.partition_id)
            registered_partition_ids.append(descriptor.partition_id)
        if memory is None:
            continue
        source_metadata = {
            **descriptor.metadata,
            "advisory": descriptor.advisory,
            "allow_model_egress": descriptor.model_egress_allowed,
            "allow_private_model_egress": descriptor.allow_private_model_egress,
        }
        model_egress_allowed = (
            descriptor.model_egress_allowed or descriptor.allow_private_model_egress
        )
        stored_source_id = memory.create_source(
            source_type=descriptor.source_kind.value,
            source_fingerprint=descriptor.source_fingerprint,
            trust_level=descriptor.trust_level,
            sensitivity=descriptor.sensitivity.value,
            model_egress_allowed=model_egress_allowed,
            display_name=descriptor.display_name,
            metadata=source_metadata,
            source_id=descriptor.source_id,
        )
        if stored_source_id != descriptor.source_id:
            raise ValueError("source fingerprint resolved to an unexpected source ID")
        memory.set_source_policy(
            stored_source_id,
            sensitivity=descriptor.sensitivity.value,
            model_egress_allowed=model_egress_allowed,
            policy_version="v1",
            metadata=source_metadata,
        )
        stored_partition_id = memory.create_partition(
            source_id=descriptor.source_id,
            partition_fingerprint=descriptor.partition_fingerprint,
            external_partition_id=descriptor.partition_id,
            display_name=descriptor.display_name,
            opted_in=descriptor.opted_in,
            sensitivity=descriptor.sensitivity.value,
            partition_id=descriptor.partition_id,
        )
        if stored_partition_id != descriptor.partition_id:
            raise ValueError("partition fingerprint resolved to an unexpected partition ID")
        memory.set_partition_policy(
            stored_partition_id,
            opted_in=descriptor.opted_in,
            sensitivity=descriptor.sensitivity.value,
        )
    event_ids: list[str] = []
    redactions = 0
    for event in result.events:
        if event.source_id in suppressed_sources:
            continue
        event_policy = policies.get(event.source_id)
        if event_policy is None:
            raise ValueError("adapter event has no matching source descriptor")
        ingested = service.ingest_event(event, event_policy, dry_run=dry_run)
        event_ids.append(ingested.event.event_id)
        redactions += ingested.event.redaction_count
    collapsed_observation_ids: list[str] = []
    for observation in result.collapsed_observations:
        if observation.source_id in suppressed_sources:
            continue
        if observation.source_id not in policies:
            raise ValueError("collapsed observation has no matching source descriptor")
        if memory is not None:
            collapsed_observation_ids.append(memory.record_collapsed_observation(observation))
    operations_store = (
        OperationsStore(settings.paths.operations_db)
        if not dry_run and (result.operations or result.cursor_updates)
        else None
    )
    for snapshot in result.operations:
        if operations_store is not None:
            operations_store.record_operational_snapshot(snapshot)
    for cursor_update in result.cursor_updates:
        if operations_store is not None:
            operations_store.set_cursor(cursor_update.name, cursor_update.value)
    return {
        "sources": len(registered_source_ids),
        "partitions": len(registered_partition_ids),
        "source_ids": registered_source_ids,
        "partition_ids": registered_partition_ids,
        "events": len(event_ids),
        "event_ids": event_ids,
        "collapsed_observations": len(collapsed_observation_ids),
        "collapsed_observation_ids": collapsed_observation_ids,
        "operations_snapshots": len(result.operations),
        "files_scanned": result.files_scanned,
        "bytes_scanned": result.bytes_scanned,
        "records_scanned": result.records_scanned,
        "extracted_records": result.extracted_records,
        "extracted_chars": result.extracted_chars,
        "cursor_updates": len(result.cursor_updates),
        "redaction_count": redactions,
        "truncated": result.truncated,
        "diagnostics": [asdict(item) for item in result.diagnostics],
        "suppressed_sources": sorted(suppressed_sources),
        "dry_run": dry_run,
    }


@app.command("init")
def init_command(
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Preview without writing.")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Create private runtime directories, databases, ledger, and empty artifacts."""

    settings = _settings()
    payload = {
        "runtime_home": str(settings.paths.home),
        "memory_db": str(settings.paths.memory_db),
        "operations_db": str(settings.paths.operations_db),
        "dry_run": dry_run,
    }
    if not dry_run:
        settings.paths.ensure()
        _stores(settings)
        ForgottenLedger(settings.paths.forgotten_log)
        payload["artifact_bundle"] = str(
            compile_memory(settings.paths.memory_db, settings.paths.artifacts)
        )
        payload["initialized"] = True
    else:
        payload["initialized"] = False
    _emit(payload, json_output=json_output)


@app.command("doctor")
def doctor_command(
    live: Annotated[
        bool,
        typer.Option("--live", help="Run synthetic model/schema probes and certify on success."),
    ] = False,
    codex_binary: Annotated[Path, typer.Option("--codex-binary")] = DEFAULT_CODEX_BINARY,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Check the sterile worker boundary without reading private credentials."""

    settings = _settings()
    runtime = WorkerRuntime(settings.paths, codex_binary=codex_binary)
    report = run_doctor(runtime, models=settings.models, live_probe=live and not dry_run)
    stamp_path = settings.paths.worker / "doctor-certification.json"
    failure_path = settings.paths.worker / "doctor-last-failure.json"
    stamped = False
    failure_written = False
    if live and not dry_run and report.certified:
        write_certification_stamp(
            stamp_path,
            report=report,
            runtime=runtime,
            models=settings.models,
        )
        stamped = True
    elif live and not dry_run:
        write_certification_failure(failure_path, report=report)
        failure_written = True
    _emit(
        {
            "offline_ready": report.offline_ready,
            "certified": report.certified,
            "certification_written": stamped,
            "failure_written": failure_written,
            "dry_run": dry_run,
            "checks": [asdict(check) for check in report.checks],
        },
        json_output=json_output,
    )
    if not dry_run and ((live and not report.certified) or (not live and not report.offline_ready)):
        raise typer.Exit(code=1)


@app.command("ingest")
def ingest_command(
    input_path: Annotated[Path, typer.Argument(help="Explicit file or directory to ingest.")],
    adapter: Annotated[IngestAdapter, typer.Option("--adapter")] = IngestAdapter.MANUAL,
    source_id: Annotated[str | None, typer.Option("--source-id")] = None,
    partition_id: Annotated[str, typer.Option("--partition-id")] = "default",
    source_kind: Annotated[SourceKind, typer.Option("--source-kind")] = SourceKind.MANUAL,
    sensitivity: Annotated[Sensitivity, typer.Option("--sensitivity")] = Sensitivity.NORMAL,
    allow_model_egress: Annotated[bool, typer.Option("--allow-model-egress")] = False,
    allow_private_model_egress: Annotated[
        bool, typer.Option("--allow-private-model-egress")
    ] = False,
    occurred_at: Annotated[str | None, typer.Option("--occurred-at")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Ingest opted-in local text after deterministic secret redaction."""

    settings = _settings()
    if adapter is not IngestAdapter.MANUAL:
        if adapter is IngestAdapter.OPERATIONS:
            if sensitivity is not Sensitivity.NORMAL or (
                allow_model_egress or allow_private_model_egress
            ):
                raise typer.BadParameter(
                    "operations adapter does not accept sensitivity or model-egress options"
                )
            captured_at = _parse_timestamp(occurred_at)
            launch_agent = (
                Path.home() / "Library" / "LaunchAgents" / f"{NIGHTLY_LAUNCHD_LABEL}.plist"
            )
            installed = launch_agent.is_file() and not launch_agent.is_symlink()
            scan = SourceScanResult(
                operations=(
                    capture_workspace_snapshot(input_path, captured_at=captured_at),
                    build_automation_snapshot(
                        automation_type="launchd",
                        automation_id=NIGHTLY_LAUNCHD_LABEL,
                        status="installed" if installed else "not_installed",
                        captured_at=captured_at,
                        payload={"installed": installed},
                    ),
                    capture_health_snapshot(
                        {
                            "workspace": input_path,
                            "runtime_home": settings.paths.home,
                        },
                        captured_at=captured_at,
                    ),
                )
            )
            _emit(
                {
                    "adapter": adapter.value,
                    **_persist_scan_result(scan, settings=settings, dry_run=dry_run),
                },
                json_output=json_output,
            )
            return
        if sensitivity is Sensitivity.SECRET and (allow_model_egress or allow_private_model_egress):
            raise typer.BadParameter("secret adapter sources cannot enable model egress")
        if allow_private_model_egress and sensitivity is not Sensitivity.PRIVATE:
            raise typer.BadParameter("--allow-private-model-egress requires --sensitivity private")
        if adapter is IngestAdapter.CODEX_SESSION:
            cursor_store = (
                OperationsStore(settings.paths.operations_db, initialize=False)
                if settings.paths.operations_db.exists()
                else None
            )
            scan = CodexSessionAdapter(
                [input_path],
                sensitivity=sensitivity,
                allow_model_egress=allow_model_egress,
                allow_private_model_egress=allow_private_model_egress,
            ).scan(cursor_loader=None if cursor_store is None else cursor_store.get_cursor)
        elif adapter is IngestAdapter.CHRONICLE:
            scan = ChronicleSummaryAdapter(
                [input_path],
                sensitivity=sensitivity,
                allow_model_egress=allow_model_egress,
                allow_private_model_egress=allow_private_model_egress,
            ).scan()
        else:
            scan = AdvisoryMarkdownAdapter(
                [input_path],
                source_kind=SourceKind.CODEX_MEMORY,
                namespace="codex-built-in-memory",
                default_occurred_at=_parse_timestamp(occurred_at) if occurred_at else None,
                sensitivity=sensitivity,
                allow_model_egress=allow_model_egress,
                allow_private_model_egress=allow_private_model_egress,
            ).scan()
        _emit(
            {
                "adapter": adapter.value,
                **_persist_scan_result(scan, settings=settings, dry_run=dry_run),
            },
            json_output=json_output,
        )
        return
    if source_id is None:
        raise typer.BadParameter("--source-id is required for the manual adapter")
    records = _read_ingest_records(input_path)
    source_fingerprint = fingerprint("source-v1", source_id)
    ledger = (
        ForgottenLedger(settings.paths.forgotten_log)
        if settings.paths.forgotten_log.exists() or not dry_run
        else None
    )
    if ledger is not None and ledger.is_source_forgotten(source_fingerprint):
        _emit(
            {
                "source_id": source_id,
                "events": 0,
                "suppressed_sources": [source_id],
                "dry_run": dry_run,
            },
            json_output=json_output,
        )
        return
    default_time = (
        _parse_timestamp(occurred_at)
        if occurred_at is not None
        else datetime.fromtimestamp(input_path.stat().st_mtime, UTC)
    )
    policy = SourcePolicy(
        source_id=source_id,
        source_kind=source_kind,
        opted_in=True,
        sensitivity=sensitivity,
        allow_model_egress=allow_model_egress,
        allow_private_model_egress=allow_private_model_egress,
    )
    if dry_run:
        service = IngestService()
        stored_source_id = source_id
        stored_partition_id = partition_id
    else:
        memory, _ = _stores(settings)
        source_metadata = {
            "allow_model_egress": allow_model_egress,
            "allow_private_model_egress": allow_private_model_egress,
        }
        model_egress_allowed = allow_model_egress or allow_private_model_egress
        stored_source_id = memory.create_source(
            source_type=source_kind.value,
            source_fingerprint=source_fingerprint,
            trust_level=_source_trust(source_kind),
            sensitivity=sensitivity.value,
            model_egress_allowed=model_egress_allowed,
            display_name=source_id,
            metadata=source_metadata,
            source_id=source_id,
        )
        memory.set_source_policy(
            stored_source_id,
            sensitivity=sensitivity.value,
            model_egress_allowed=model_egress_allowed,
            policy_version="v1",
            metadata=source_metadata,
        )
        stored_partition_id = memory.create_partition(
            source_id=stored_source_id,
            partition_fingerprint=fingerprint("partition-v1", source_id, partition_id),
            external_partition_id=partition_id,
            display_name=partition_id,
            opted_in=True,
            sensitivity=sensitivity.value,
            partition_id=partition_id,
        )
        memory.set_partition_policy(
            stored_partition_id,
            opted_in=True,
            sensitivity=sensitivity.value,
        )
        service = IngestService(memory)
    event_ids: list[str] = []
    redactions = 0
    for index, record in enumerate(records, 1):
        record_time = (
            _parse_timestamp(str(record["occurred_at"]))
            if record.get("occurred_at")
            else default_time
        )
        event = IngestEventInput(
            source_id=stored_source_id,
            partition_id=stored_partition_id,
            source_kind=source_kind,
            external_event_id=str(
                record.get("external_event_id")
                or hashlib.sha256(
                    f"{input_path.resolve()}:{index}:{record['content']}".encode()
                ).hexdigest()
            ),
            occurred_at=record_time,
            content=str(record["content"]),
            sensitivity=Sensitivity(str(record.get("sensitivity", sensitivity.value))),
            source_locator=str(record.get("source_locator") or input_path.resolve()),
            metadata=_scalar_metadata(record.get("metadata")),
        )
        result = service.ingest_event(event, policy, dry_run=dry_run)
        event_ids.append(result.event.event_id)
        redactions += result.event.redaction_count
    _emit(
        {
            "source_id": stored_source_id,
            "partition_id": stored_partition_id,
            "events": len(event_ids),
            "event_ids": event_ids,
            "redaction_count": redactions,
            "dry_run": dry_run,
        },
        json_output=json_output,
    )


@app.command("segment")
def segment_command(
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 40,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Build stable episodes from unsegmented redacted events."""

    settings = _settings()
    memory, _ = _stores(settings)
    _emit(segment_pending_events(memory, dry_run=dry_run, limit=limit), json_output=json_output)


@app.command("extract")
def extract_command(
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 40,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Queue Phase 1 candidate extraction; it never writes canonical claims."""

    settings = _settings()
    memory, operations = _stores(settings)
    _emit(
        enqueue_phase1_jobs(
            memory, operations, limit=limit, dry_run=dry_run, models=settings.models
        ),
        json_output=json_output,
    )


@app.command("consolidate")
def consolidate_command(
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 40,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Queue Phase 2 review proposals; it never approves memory."""

    settings = _settings()
    memory, operations = _stores(settings)
    phase2 = enqueue_phase2_jobs(
        memory, operations, limit=limit, dry_run=dry_run, models=settings.models
    )
    temporal = propose_expired_plan_outcomes(memory, dry_run=dry_run)
    _emit(
        {"phase2": asdict(phase2), "temporal": asdict(temporal)},
        json_output=json_output,
    )


@review_app.command("list")
def review_list_command(
    status: Annotated[str, typer.Option("--status")] = "pending",
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    memory, _ = _stores(settings)
    with memory.connection() as connection:
        rows = connection.execute(
            """
            SELECT rb.batch_id, rb.status, rb.base_memory_revision, rb.created_at,
                   COUNT(rp.proposal_id) AS proposal_count
            FROM review_batches AS rb
            LEFT JOIN review_proposals AS rp ON rp.batch_id = rb.batch_id
            WHERE ? = 'all' OR rb.status = ?
            GROUP BY rb.batch_id
            ORDER BY rb.created_at DESC, rb.batch_id
            """,
            (status, status),
        ).fetchall()
    _emit({"batches": [dict(row) for row in rows]}, json_output=json_output)


@review_app.command("show")
def review_show_command(
    batch_id: Annotated[str, typer.Argument()],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    memory, _ = _stores(settings)
    _emit(memory.load_review_batch(batch_id), json_output=json_output)


@review_app.command("approve")
def review_approve_command(
    batch_id: Annotated[str, typer.Argument()],
    note: Annotated[str | None, typer.Option("--note")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Approve an entire fresh batch, then apply it in one canonical transaction."""

    settings = _settings()
    memory, _ = _stores(settings)
    review = ReviewService(memory)
    batch = memory.load_review_batch(batch_id)
    for proposal in batch["proposals"]:
        _require_valid_proposal_binding(proposal)
    if batch["status"] == "pending":
        validation = review.validate_batch(batch_id)
        stale = [asdict(item) for item in validation.validations if not item.fresh]
        proposal_count = len(validation.proposals)
    elif batch["status"] == "approved":
        stale = []
        for proposal in batch["proposals"]:
            current = fingerprint_head_set(
                memory.load_heads(str(proposal["target_slot_fingerprint"]))
            )
            if current != proposal["expected_head_set_hash"]:
                stale.append(
                    {
                        "proposal_id": proposal["proposal_id"],
                        "reason": "target_head_set_changed",
                    }
                )
        proposal_count = len(batch["proposals"])
    else:
        raise typer.BadParameter(f"review batch is {batch['status']}, not pending or approved")
    if dry_run:
        _emit(
            {
                "batch_id": batch_id,
                "fresh": not stale,
                "stale": stale,
                "would_apply": proposal_count,
                "resume_approved_batch": batch["status"] == "approved",
                "dry_run": True,
            },
            json_output=json_output,
        )
        return
    if stale:
        if batch["status"] == "pending":
            memory.mark_review_stale(
                batch_id,
                note="approve --all rejected because the batch contains stale proposals",
            )
        raise typer.BadParameter("review batch is stale; rerun Phase 2")
    if batch["status"] == "pending":
        review.record_approve_all(batch_id, note=note)
    revision, versions = CanonicalApplicationService(settings.paths.memory_db).apply_batch(batch_id)
    bundle = compile_memory(settings.paths.memory_db, settings.paths.artifacts)
    _emit(
        {
            "batch_id": batch_id,
            "memory_revision": revision,
            "claim_version_ids": versions,
            "artifact_bundle": str(bundle),
            "dry_run": False,
        },
        json_output=json_output,
    )


@review_app.command("reject")
def review_reject_command(
    batch_id: Annotated[str, typer.Argument()],
    note: Annotated[str | None, typer.Option("--note")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    memory, _ = _stores(settings)
    batch = memory.load_review_batch(batch_id)
    proposals = [row for row in batch["proposals"] if row["status"] == "pending"]
    if not dry_run:
        memory.record_review_decisions(
            batch_id,
            {str(row["proposal_id"]): "rejected" for row in proposals},
            note=note,
        )
    _emit(
        {
            "batch_id": batch_id,
            "rejected": len(proposals),
            "dry_run": dry_run,
        },
        json_output=json_output,
    )


@review_app.command("correct")
def review_correct_command(
    batch_id: Annotated[str, typer.Argument()],
    proposal_id: Annotated[str, typer.Argument()],
    value: Annotated[str, typer.Option("--value", help="JSON value or plain string.")],
    summary: Annotated[str, typer.Option("--summary")],
    mcp_safe_summary: Annotated[str | None, typer.Option("--mcp-safe-summary")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Record Leo's explicit canonical correction and stale the old proposal."""

    settings = _settings()
    memory, _ = _stores(settings)
    batch = memory.load_review_batch(batch_id)
    rows = [row for row in batch["proposals"] if row["proposal_id"] == proposal_id]
    if len(rows) != 1:
        raise typer.BadParameter("proposal does not belong to this batch")
    row = rows[0]
    if row["status"] != "pending":
        raise typer.BadParameter("proposal is not pending")
    _require_valid_proposal_binding(row)
    payload = row["payload"]
    identity = identity_fingerprint(
        str(payload["subject_text"]), str(payload["predicate"]), str(payload["scope"])
    )
    heads = memory.load_heads(identity)
    claim = ClaimVersionInput(
        subject_text=str(payload["subject_text"]),
        predicate=str(payload["predicate"]),
        scope=str(payload["scope"]),
        value=_parse_json_value(value),
        summary=summary,
        confidence=1.0,
        epistemic_status="user_confirmed",
        sensitivity=str(payload.get("sensitivity", "normal")),
        provenance_kind="manual",
        mcp_safe_summary=mcp_safe_summary,
        supersedes=tuple(head.claim_version_id for head in heads),
    )
    if dry_run:
        _emit(
            {
                "batch_id": batch_id,
                "proposal_id": proposal_id,
                "target_slot_fingerprint": identity,
                "would_supersede": [head.claim_version_id for head in heads],
                "dry_run": True,
            },
            json_output=json_output,
        )
        return
    claim_id, version_id, revision = memory.add_claim_version(
        claim, actor="leo", reason="Leo corrected review proposal"
    )
    memory.mark_review_stale(
        batch_id,
        proposal_ids=(proposal_id,),
        note=f"Leo correction created {version_id}",
    )
    bundle = compile_memory(settings.paths.memory_db, settings.paths.artifacts)
    _emit(
        {
            "claim_id": claim_id,
            "claim_version_id": version_id,
            "memory_revision": revision,
            "artifact_bundle": str(bundle),
            "dry_run": False,
        },
        json_output=json_output,
    )


@app.command("search")
def search_command(
    query: Annotated[str, typer.Argument()],
    limit: Annotated[int, typer.Option("--limit", min=1, max=100)] = 10,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    _emit(_retrieval(settings).search(query, limit=limit), json_output=json_output)


@app.command("context")
def context_command(
    query: Annotated[str, typer.Argument()],
    limit: Annotated[int, typer.Option("--limit", min=1, max=100)] = 12,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    _emit(_retrieval(settings).context(query, limit=limit), json_output=json_output)


@app.command("timeline")
def timeline_command(
    limit: Annotated[int, typer.Option("--limit", min=1, max=200)] = 30,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    _emit(_retrieval(settings).timeline(limit=limit), json_output=json_output)


@app.command("explain")
def explain_command(
    claim_id: Annotated[str, typer.Argument()],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    _emit(_retrieval(settings).explain(claim_id), json_output=json_output)


@app.command("history")
def history_command(
    claim_id: Annotated[str, typer.Argument()],
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    _emit(_retrieval(settings).history(claim_id), json_output=json_output)


@app.command("status")
def status_command(
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    memory, operations = _stores(settings)
    with memory.connection() as connection:
        memory_counts = {
            "revision": memory.current_revision(),
            "sources": int(connection.execute("SELECT COUNT(*) FROM sources").fetchone()[0]),
            "events": int(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]),
            "episodes": int(connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0]),
            "candidates": int(
                connection.execute("SELECT COUNT(*) FROM candidate_claims").fetchone()[0]
            ),
            "pending_review_batches": int(
                connection.execute(
                    "SELECT COUNT(*) FROM review_batches WHERE status = 'pending'"
                ).fetchone()[0]
            ),
        }
    with operations.connection() as connection:
        operations_counts: dict[str, Any] = {
            "queued_jobs": int(
                connection.execute("SELECT COUNT(*) FROM jobs WHERE status = 'queued'").fetchone()[
                    0
                ]
            ),
            "verified_manual_runs": _verified_manual_run_count(operations),
            "last_nightly": (
                dict(row)
                if (
                    row := connection.execute(
                        "SELECT * FROM nightly_runs ORDER BY started_at DESC LIMIT 1"
                    ).fetchone()
                )
                else None
            ),
            "latest_snapshots": _latest_operations_snapshot_summaries(connection),
        }
    runtime = WorkerRuntime(settings.paths)
    stamp = settings.paths.worker / "doctor-certification.json"
    worker_certified = verify_certification_stamp(
        stamp,
        runtime=runtime,
        models=settings.models,
    )
    launchd_conflicts, openclaw_conflicts, scan_errors = _schedule_scan(
        hour=DEFAULT_HOUR,
        minute=DEFAULT_MINUTE,
        strict=True,
    )
    doctor_launchd_conflicts, doctor_openclaw_conflicts, doctor_scan_errors = _schedule_scan(
        hour=DOCTOR_HOUR,
        minute=DOCTOR_MINUTE,
        strict=True,
    )
    conflicts = [*launchd_conflicts, *openclaw_conflicts]
    doctor_conflicts = [*doctor_launchd_conflicts, *doctor_openclaw_conflicts]
    agents = Path.home() / "Library" / "LaunchAgents"
    nightly_destination = agents / f"{NIGHTLY_LAUNCHD_LABEL}.plist"
    doctor_destination = agents / f"{DOCTOR_LABEL}.plist"
    _emit(
        {
            "runtime_home": str(settings.paths.home),
            "memory": memory_counts,
            "operations": operations_counts,
            "worker_certified": worker_certified,
            "worker_certification_expires_at": _certification_expiry(stamp),
            "last_doctor_failure": _doctor_failure_summary(
                settings.paths.worker / "doctor-last-failure.json"
            ),
            "artifact_bundle": (
                str(bundle) if (bundle := current_bundle(settings.paths.artifacts)) else None
            ),
            "launchd_conflicts": [asdict(conflict) for conflict in launchd_conflicts],
            "openclaw_conflicts": [asdict(conflict) for conflict in openclaw_conflicts],
            "launchd_scan_errors": scan_errors,
            "launchd_installed": nightly_destination.is_file()
            and not nightly_destination.is_symlink(),
            "schedule_target": {
                "hour": DEFAULT_HOUR,
                "minute": DEFAULT_MINUTE,
                "timezone": "Asia/Taipei",
            },
            "launchd_install_gate_passed": (
                worker_certified
                and operations_counts["verified_manual_runs"] >= 7
                and not conflicts
                and not scan_errors
            ),
            "doctor_schedule": {
                "conflicts": [asdict(conflict) for conflict in doctor_conflicts],
                "scan_errors": doctor_scan_errors,
                "installed": doctor_destination.is_file() and not doctor_destination.is_symlink(),
                "target": {
                    "weekday": "Sunday",
                    "hour": DOCTOR_HOUR,
                    "minute": DOCTOR_MINUTE,
                    "timezone": "Asia/Taipei",
                },
                "install_gate_passed": (
                    worker_certified
                    and operations_counts["verified_manual_runs"] >= 7
                    and not doctor_conflicts
                    and not doctor_scan_errors
                ),
            },
        },
        json_output=json_output,
    )


@schedule_app.command("install")
def schedule_install_command(
    job: Annotated[ScheduleJob, typer.Option("--job")] = ScheduleJob.NIGHTLY,
    hour: Annotated[int | None, typer.Option("--hour")] = None,
    minute: Annotated[int | None, typer.Option("--minute")] = None,
    dream_executable: Annotated[Path | None, typer.Option("--dream-executable")] = None,
    destination: Annotated[Path | None, typer.Option("--destination")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Install one fail-closed LaunchAgent after recomputing every gate."""

    settings = _settings()
    _, operations = _stores(settings)
    selected_hour = (
        hour if hour is not None else (DEFAULT_HOUR if job is ScheduleJob.NIGHTLY else DOCTOR_HOUR)
    )
    selected_minute = (
        minute
        if minute is not None
        else (DEFAULT_MINUTE if job is ScheduleJob.NIGHTLY else DOCTOR_MINUTE)
    )
    executable_input = dream_executable or settings.paths.home / "venv" / "bin" / "dream"
    if executable_input.is_symlink():
        raise typer.BadParameter("dream executable must not be a symlink")
    executable = executable_input.expanduser().resolve()
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise typer.BadParameter("dream executable must be an executable regular file")

    label = NIGHTLY_LAUNCHD_LABEL if job is ScheduleJob.NIGHTLY else DOCTOR_LABEL
    selected_destination = (
        destination.expanduser().resolve()
        if destination is not None
        else Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
    )
    try:
        payload = (
            render_launch_agent(
                dream_executable=executable,
                runtime_home=settings.paths.home,
                hour=selected_hour,
                minute=selected_minute,
            )
            if job is ScheduleJob.NIGHTLY
            else render_doctor_launch_agent(
                dream_executable=executable,
                runtime_home=settings.paths.home,
                hour=selected_hour,
                minute=selected_minute,
            )
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    runtime = WorkerRuntime(settings.paths)
    stamp = settings.paths.worker / "doctor-certification.json"
    certified = verify_certification_stamp(stamp, runtime=runtime, models=settings.models)
    verified_runs = _verified_manual_run_count(operations)
    launchd_conflicts, openclaw_conflicts, scan_errors = _schedule_scan(
        hour=selected_hour,
        minute=selected_minute,
        strict=True,
    )
    conflicts = [*launchd_conflicts, *openclaw_conflicts]
    existing_matches = False
    if selected_destination.exists():
        if selected_destination.is_symlink():
            scan_errors.append("existing LaunchAgent destination is a symlink")
        else:
            try:
                existing_matches = selected_destination.read_bytes() == payload
            except OSError:
                scan_errors.append("existing LaunchAgent cannot be read")
            if not existing_matches:
                scan_errors.append("existing LaunchAgent configuration differs")
    gate_passed = certified and verified_runs >= 7 and not conflicts and not scan_errors
    result: dict[str, Any] = {
        "job": job.value,
        "label": label,
        "destination": str(selected_destination),
        "target": {
            "hour": selected_hour,
            "minute": selected_minute,
            "timezone": "Asia/Taipei",
            **({"weekday": "Sunday"} if job is ScheduleJob.DOCTOR else {}),
        },
        "worker_certified": certified,
        "certification_expires_at": _certification_expiry(stamp),
        "verified_manual_runs": verified_runs,
        "conflicts": [asdict(conflict) for conflict in conflicts],
        "scan_errors": scan_errors,
        "gate_passed": gate_passed,
        "already_configured": existing_matches,
        "dry_run": dry_run,
    }
    if not gate_passed:
        _emit(result, json_output=json_output)
        raise typer.Exit(code=1)
    if dry_run:
        _emit(result, json_output=json_output)
        return

    if not existing_matches:
        write_launch_agent(
            selected_destination,
            payload,
            verified_manual_runs=verified_runs,
            conflicts=conflicts,
            worker_certified=certified,
        )
    try:
        bootstrapped = bootstrap_launch_agent(selected_destination, label=label)
    except RuntimeError as exc:
        result.update({"installed": False, "error": str(exc)})
        _emit(result, json_output=json_output)
        raise typer.Exit(code=1) from exc
    result.update(
        {
            "installed": selected_destination.is_file() and not selected_destination.is_symlink(),
            "bootstrapped": bootstrapped,
        }
    )
    _emit(result, json_output=json_output)


@app.command("pin")
def pin_command(
    claim_id: Annotated[str, typer.Argument()],
    unpin: Annotated[bool, typer.Option("--unpin")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    memory, _ = _stores(settings)
    with memory.connection() as connection:
        exists = connection.execute(
            "SELECT 1 FROM claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()
    if exists is None:
        raise typer.BadParameter(f"unknown claim: {claim_id}")
    revision = None
    changed = False
    bundle = None
    if not dry_run:
        changed, revision = memory.set_claim_pin(claim_id, pinned=not unpin, actor="leo")
        bundle = compile_memory(settings.paths.memory_db, settings.paths.artifacts)
    _emit(
        {
            "claim_id": claim_id,
            "pinned": not unpin,
            "changed": changed,
            "memory_revision": revision,
            "artifact_bundle": str(bundle) if bundle else None,
            "dry_run": dry_run,
        },
        json_output=json_output,
    )


@app.command("forget-claim")
def forget_claim_command(
    claim_id: Annotated[str, typer.Argument()],
    identity: Annotated[bool, typer.Option("--identity")] = False,
    reason: Annotated[str | None, typer.Option("--reason")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    memory, _ = _stores(settings)
    ledger = (
        ForgottenLedger(settings.paths.forgotten_log)
        if not dry_run
        else object.__new__(ForgottenLedger)
    )
    report = forget_claim(
        memory,
        ledger,
        claim_id,
        identity_wide=identity,
        reason=reason,
        dry_run=dry_run,
        snapshot_manager=SnapshotManager(settings.paths.snapshots) if not dry_run else None,
        operations_path=settings.paths.operations_db,
        artifacts_dir=settings.paths.artifacts,
    )
    bundle = None
    if not dry_run:
        bundle = compile_memory(settings.paths.memory_db, settings.paths.artifacts)
    _emit(
        {**asdict(report), "artifact_bundle": str(bundle) if bundle else None},
        json_output=json_output,
    )


@app.command("forget-source")
def forget_source_command(
    source_id: Annotated[str, typer.Argument()],
    reason: Annotated[str | None, typer.Option("--reason")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    memory, _ = _stores(settings)
    ledger = (
        ForgottenLedger(settings.paths.forgotten_log)
        if not dry_run
        else object.__new__(ForgottenLedger)
    )
    report = forget_source(
        memory,
        ledger,
        source_id,
        reason=reason,
        dry_run=dry_run,
        snapshot_manager=SnapshotManager(settings.paths.snapshots) if not dry_run else None,
        operations_path=settings.paths.operations_db,
        artifacts_dir=settings.paths.artifacts,
    )
    bundle = None
    if not dry_run:
        bundle = compile_memory(settings.paths.memory_db, settings.paths.artifacts)
    _emit(
        {**asdict(report), "artifact_bundle": str(bundle) if bundle else None},
        json_output=json_output,
    )


@snapshot_app.command("create")
def snapshot_create_command(
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    _stores(settings)
    path = None
    if not dry_run:
        path = SnapshotManager(settings.paths.snapshots).create(
            settings.paths.memory_db, settings.paths.operations_db
        )
    _emit(
        {"snapshot": str(path) if path else None, "dry_run": dry_run},
        json_output=json_output,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_snapshot_database(path: Path, *, expected_role: str) -> None:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    try:
        check_integrity(connection)
        row = connection.execute(
            "SELECT value FROM metadata WHERE key = 'database_role'"
        ).fetchone()
        if row is None or row[0] != expected_role:
            raise typer.BadParameter(f"snapshot database role mismatch: expected {expected_role}")
    finally:
        connection.close()


@snapshot_app.command("restore")
def snapshot_restore_command(
    snapshot: Annotated[Path, typer.Argument()],
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    managed_root = settings.paths.snapshots.resolve()
    selected = snapshot.expanduser().resolve()
    if not selected.is_relative_to(managed_root) or selected.parent != managed_root:
        raise typer.BadParameter("snapshot must be a direct child of the managed snapshot root")
    manifest_path = selected / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for name in ("memory.sqlite3", "operations.sqlite3"):
        expected = manifest["files"][name]
        if _sha256(selected / name) != expected:
            raise typer.BadParameter(f"snapshot checksum mismatch: {name}")
    _validate_snapshot_database(selected / "memory.sqlite3", expected_role="memory")
    _validate_snapshot_database(selected / "operations.sqlite3", expected_role="operations")
    if not dry_run:
        # All multi-database maintenance acquires canonical memory first, then
        # operations, so writers cannot deadlock by choosing opposite orders.
        with (
            memory_maintenance_lock(settings.paths.memory_db),
            operations_maintenance_lock(settings.paths.operations_db),
        ):
            _stores(settings)
            ledger = ForgottenLedger(settings.paths.forgotten_log)
            ledger.entries()
            baseline = SnapshotManager(settings.paths.snapshots).create(
                settings.paths.memory_db, settings.paths.operations_db
            )
            try:
                restore_database(
                    selected / "operations.sqlite3",
                    settings.paths.operations_db,
                    expected_role="operations",
                )
                restore_database(
                    selected / "memory.sqlite3",
                    settings.paths.memory_db,
                    expected_role="memory",
                )
                changed = apply_forgotten_ledger(
                    MemoryStore(settings.paths.memory_db),
                    ledger,
                )
                bundle = compile_memory(settings.paths.memory_db, settings.paths.artifacts)
            except BaseException:
                restore_database(
                    baseline / "operations.sqlite3",
                    settings.paths.operations_db,
                    expected_role="operations",
                )
                restore_database(
                    baseline / "memory.sqlite3",
                    settings.paths.memory_db,
                    expected_role="memory",
                )
                compile_memory(settings.paths.memory_db, settings.paths.artifacts)
                raise
    else:
        changed = 0
        bundle = None
    _emit(
        {
            "snapshot": str(selected),
            "memory_revision": manifest["memory_revision"],
            "forgotten_records_reapplied": changed,
            "artifact_bundle": str(bundle) if bundle else None,
            "dry_run": dry_run,
        },
        json_output=json_output,
    )


@app.command("audit")
def audit_command(
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    checks: list[dict[str, Any]] = []
    for role, path, connector in (
        ("memory", settings.paths.memory_db, connect_memory),
        ("operations", settings.paths.operations_db, connect_operations),
    ):
        if not path.is_file():
            checks.append({"name": f"{role}_integrity", "status": "fail", "error": "missing"})
            continue
        try:
            with connector(path) as connection:
                check_integrity(connection)
            checks.append({"name": f"{role}_integrity", "status": "pass"})
        except (OSError, sqlite3.Error, ValueError) as exc:
            checks.append(
                {"name": f"{role}_integrity", "status": "fail", "error": type(exc).__name__}
            )
    try:
        if not settings.paths.forgotten_log.is_file():
            raise FileNotFoundError(settings.paths.forgotten_log)
        entries = ForgottenLedger(settings.paths.forgotten_log).entries()
        checks.append({"name": "forgotten_ledger", "status": "pass", "records": len(entries)})
    except (OSError, ValueError) as exc:
        checks.append({"name": "forgotten_ledger", "status": "fail", "error": type(exc).__name__})
    bundle = current_bundle(settings.paths.artifacts)
    checks.append(
        {
            "name": "artifact_bundle",
            "status": "pass" if bundle else "warn",
            "path": str(bundle) if bundle else None,
        }
    )
    _emit(
        {
            "passed": all(check["status"] != "fail" for check in checks),
            "checks": checks,
        },
        json_output=json_output,
    )


@app.command("eval")
def eval_command(
    cases: Annotated[Path | None, typer.Option("--cases")] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    settings = _settings()
    case_path = cases or Path(__file__).parent / "data" / "synthetic_cases.json"
    results = run_evals(_retrieval(settings), load_cases(case_path))
    _emit(
        {
            "passed": all(result.passed for result in results),
            "results": [result.model_dump(mode="json") for result in results],
        },
        json_output=json_output,
    )


@app.command("run-nightly")
def run_nightly_command(
    codex_binary: Annotated[Path, typer.Option("--codex-binary")] = DEFAULT_CODEX_BINARY,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Run bounded jobs; outputs remain candidates/review batches until Leo approves."""

    settings = _settings()
    memory, operations = _stores(settings)
    phase1 = enqueue_phase1_jobs(
        memory,
        operations,
        limit=settings.nightly.max_episodes,
        dry_run=dry_run,
        models=settings.models,
    )
    phase2 = enqueue_phase2_jobs(
        memory,
        operations,
        limit=settings.nightly.max_episodes,
        dry_run=dry_run,
        models=settings.models,
    )
    if dry_run:
        runtime = WorkerRuntime(settings.paths, codex_binary=codex_binary)
        certified = verify_certification_stamp(
            settings.paths.worker / "doctor-certification.json",
            runtime=runtime,
            models=settings.models,
        )
        report: object = {
            "status": "dry_run",
            "worker_certified": certified,
            "phase1": asdict(phase1),
            "phase2": asdict(phase2),
            "temporal": asdict(propose_expired_plan_outcomes(memory, dry_run=True)),
        }
    else:
        nightly_report = build_nightly_runner(settings, codex_binary=codex_binary).run()
        report = {
            **asdict(nightly_report),
            "temporal": asdict(propose_expired_plan_outcomes(memory)),
        }
    _emit(report, json_output=json_output)
    if not dry_run and nightly_report.status not in {
        NightlyStatus.COMPLETED,
        NightlyStatus.ALREADY_RUNNING,
    }:
        raise typer.Exit(code=1)


@app.command("run-pilot")
def run_pilot_command(
    job_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--job-id",
            help="Exact queued Phase 1 allowlist; repeat for one to three pilot jobs.",
        ),
    ] = None,
    phase2_limit: Annotated[
        int,
        typer.Option(
            "--max-phase2-groups",
            min=1,
            max=5,
            help="Fail closed before Phase 2 when the pilot creates more identity groups.",
        ),
    ] = 5,
    codex_binary: Annotated[Path, typer.Option("--codex-binary")] = DEFAULT_CODEX_BINARY,
    worker_runtime_home: Annotated[
        Path | None,
        typer.Option(
            "--worker-runtime-home",
            help="Clone-only: reuse the certified production worker without copying credentials.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run/--apply",
            help="Validate the exact scope by default; --apply runs the fail-fast pilot.",
        ),
    ] = True,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Run only explicitly selected Phase 1 jobs and their newly created Phase 2 work."""

    settings = _settings()
    allowlist = tuple(dict.fromkeys(job_ids or ()))
    try:
        worker_paths = _pilot_worker_paths(settings, worker_runtime_home)
        if dry_run:
            memory, operations = _stores(settings)
            scope = plan_pilot_scope(
                memory,
                operations,
                phase1_job_allowlist=allowlist,
            )
            runtime = WorkerRuntime(worker_paths, codex_binary=codex_binary)
            certified = verify_certification_stamp(
                worker_paths.worker / "doctor-certification.json",
                runtime=runtime,
                models=settings.models,
            )
            report: object = {
                "status": "dry_run",
                "scope": asdict(scope),
                "max_phase2_groups": phase2_limit,
                "worker_certified": certified,
            }
        else:
            runner, scope = build_pilot_runner(
                settings,
                phase1_job_allowlist=allowlist,
                phase2_limit=phase2_limit,
                codex_binary=codex_binary,
                worker_paths=worker_paths,
            )
            pilot_report = runner.run()
            report = {
                **asdict(pilot_report),
                "scope": asdict(scope),
                "max_phase2_groups": phase2_limit,
            }
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(report, json_output=json_output)
    if not dry_run and pilot_report.status not in {
        NightlyStatus.COMPLETED,
        NightlyStatus.ALREADY_RUNNING,
    }:
        raise typer.Exit(code=1)


@app.command("queue-reconcile")
def queue_reconcile_command(
    job_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--job-id",
            help="Exact Phase 1 job allowlist; repeat for every job in the atomic batch.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run/--apply",
            help="Preview by default; --apply atomically installs terminal barriers.",
        ),
    ] = True,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Reconcile stale Phase 1 jobs without model egress or canonical writes."""

    settings = _settings()
    memory, operations = _stores(settings)
    try:
        if dry_run:
            report = reconcile_phase1_queue(
                memory,
                operations,
                job_allowlist=tuple(job_ids or ()),
                models=settings.models,
                dry_run=True,
            )
        else:
            with operations_maintenance_lock(settings.paths.operations_db):
                report = reconcile_phase1_queue(
                    memory,
                    operations,
                    job_allowlist=tuple(job_ids or ()),
                    models=settings.models,
                    dry_run=False,
                )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(asdict(report), json_output=json_output)


@app.command("queue-reactivate")
def queue_reactivate_command(
    barrier_job_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--barrier-job-id",
            help="Exact reconciler-owned barrier allowlist; repeat for the whole batch.",
        ),
    ] = None,
    codex_binary: Annotated[Path, typer.Option("--codex-binary")] = DEFAULT_CODEX_BINARY,
    worker_runtime_home: Annotated[
        Path | None,
        typer.Option(
            "--worker-runtime-home",
            help="Clone-only: certify with the production worker without copying credentials.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run/--apply",
            help="Preview by default; --apply first requires a live synthetic doctor pass.",
        ),
    ] = True,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Reactivate only explicitly approved deferred barriers after a live doctor gate."""

    settings = _settings()
    _memory, operations = _stores(settings)
    allowlist = tuple(barrier_job_ids or ())
    live_doctor_executed = False
    try:
        worker_paths = _pilot_worker_paths(settings, worker_runtime_home)
        if dry_run:
            reactivated = reactivate_phase1_barriers(
                operations,
                barrier_job_allowlist=allowlist,
                live_doctor_verified=True,
                dry_run=True,
            )
        else:
            live_doctor_executed = True
            report = run_doctor(
                WorkerRuntime(worker_paths, codex_binary=codex_binary),
                models=settings.models,
                live_probe=True,
            )
            if not report.certified:
                raise ValueError("phase1 barrier reactivation live doctor did not certify")
            with operations_maintenance_lock(settings.paths.operations_db):
                reactivated = reactivate_phase1_barriers(
                    operations,
                    barrier_job_allowlist=allowlist,
                    live_doctor_verified=True,
                    dry_run=False,
                )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _emit(
        {
            "barrier_job_ids": reactivated,
            "dry_run": dry_run,
            "live_doctor_executed": live_doctor_executed,
            "live_doctor_required_for_apply": True,
        },
        json_output=json_output,
    )


@app.command("runtime-provenance")
def runtime_provenance_command(
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Hash the installed runtime boundary; package version alone is not sufficient."""

    manifest = build_installation_provenance(
        Path(__file__).resolve().parent,
        package_version=importlib.metadata.version("local-dreaming"),
    )
    _emit(asdict(manifest), json_output=json_output)


@mcp_app.command("serve")
def mcp_serve_command() -> None:
    """Serve only approved policy-filtered summaries over stdio."""

    settings = _settings()
    serve_stdio(_retrieval(settings))


if __name__ == "__main__":
    app()

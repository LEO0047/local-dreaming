"""Deterministic, read-only adapters for Local-Dreaming source material.

Every filesystem root is supplied by the caller.  This module deliberately has
no defaults pointing at ``~/.codex`` and never reads authentication or
credential stores.  It converts bounded source material into ingest-shaped
records; persistence, redaction validation, and model execution remain the
responsibility of later layers.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, tzinfo
from enum import StrEnum
from pathlib import Path
from typing import Any

from local_dreaming.handoff import parse_dreaming_handoff
from local_dreaming.ingest import normalize_content
from local_dreaming.models import (
    IngestEventInput,
    JsonScalar,
    Sensitivity,
    SourceKind,
    SourcePolicy,
)
from local_dreaming.redaction import REDACTED_SECRET, redact_secrets

_PARSER_VERSION = "source-adapters-v4"
_COMPATIBLE_CURSOR_PARSER_VERSIONS = frozenset(
    {_PARSER_VERSION, "source-adapters-v3", "source-adapters-v2"}
)
_CODEX_CURSOR_VERSION = 1
_CODEX_INVENTORY_MULTIPLIER = 64
_ADJACENT_DUPLICATE_SECONDS = 1.0
_CREDENTIAL_BASENAMES = frozenset(
    {
        ".env",
        "auth.json",
        "auth.jsonl",
        "credentials.json",
        "credentials.jsonl",
        "secrets.md",
        "tokens.md",
    }
)
_FRONTMATTER_TIME_KEYS = ("occurred_at", "created_at", "updated_at", "date", "timestamp")


@dataclass(frozen=True, slots=True)
class ScanLimits:
    """Hard limits applied before source contents are returned."""

    max_files: int = 128
    max_file_bytes: int = 8 * 1024 * 1024
    max_total_bytes: int = 250 * 1024 * 1024
    max_records_per_file: int = 20_000
    max_content_chars: int = 64_000
    max_record_bytes: int = 1024 * 1024
    max_extracted_records_per_file: int = 500
    max_extracted_chars_per_file: int = 512_000
    max_wall_seconds: int = 1_200
    max_directory_depth: int = 8

    def __post_init__(self) -> None:
        values = (
            self.max_files,
            self.max_file_bytes,
            self.max_total_bytes,
            self.max_records_per_file,
            self.max_content_chars,
            self.max_record_bytes,
            self.max_extracted_records_per_file,
            self.max_extracted_chars_per_file,
            self.max_wall_seconds,
            self.max_directory_depth,
        )
        if any(value <= 0 for value in values):
            raise ValueError("scan limits must be positive")


@dataclass(frozen=True, slots=True)
class ScanDiagnostic:
    code: str
    message: str
    path: str | None = None
    record_number: int | None = None


@dataclass(frozen=True, slots=True)
class SourceCursorUpdate:
    name: str
    value: str


@dataclass(frozen=True, slots=True)
class CollapsedEventObservation:
    """Opaque audit record for one observation collapsed into a canonical event.

    The duplicate body and source locator are deliberately absent.  Persistence
    resolves the representative through its stable external identity (or the
    frozen occurrence/lineage fallback used by pre-v4 cursors).
    """

    source_id: str
    partition_id: str
    representative_external_event_id: str | None
    representative_occurred_at: datetime
    source_lineage_fingerprint: str
    evidence_family_fingerprint: str
    observation_fingerprint: str
    observed_at: datetime
    parser_version: str = _PARSER_VERSION

    def __post_init__(self) -> None:
        for field_name in (
            "source_id",
            "partition_id",
            "source_lineage_fingerprint",
            "evidence_family_fingerprint",
            "observation_fingerprint",
            "parser_version",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if (
            self.representative_external_event_id is not None
            and not self.representative_external_event_id.strip()
        ):
            raise ValueError("representative_external_event_id must not be blank")
        _require_aware(
            self.representative_occurred_at,
            field_name="representative_occurred_at",
        )
        _require_aware(self.observed_at, field_name="observed_at")


@dataclass(frozen=True, slots=True)
class SourceDescriptor:
    """Storage registration and policy information for one source partition."""

    source_id: str
    partition_id: str
    source_kind: SourceKind
    source_fingerprint: str
    partition_fingerprint: str
    display_name: str
    trust_level: str
    sensitivity: Sensitivity = Sensitivity.NORMAL
    opted_in: bool = True
    model_egress_allowed: bool = False
    allow_private_model_egress: bool = False
    advisory: bool = False
    metadata: dict[str, JsonScalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_adapter_egress(
            sensitivity=self.sensitivity,
            allow_model_egress=self.model_egress_allowed,
            allow_private_model_egress=self.allow_private_model_egress,
        )
        if self.source_kind in {SourceKind.CHRONICLE, SourceKind.CODEX_MEMORY} and (
            self.trust_level != "advisory" or not self.advisory
        ):
            raise ValueError("Chronicle and Codex memory descriptors must be advisory")

    def policy(self) -> SourcePolicy:
        return SourcePolicy(
            source_id=self.source_id,
            source_kind=self.source_kind,
            opted_in=self.opted_in,
            sensitivity=self.sensitivity,
            allow_model_egress=self.model_egress_allowed,
            allow_private_model_egress=self.allow_private_model_egress,
        )


class OperationalSnapshotKind(StrEnum):
    WORKSPACE = "workspace"
    AUTOMATION = "automation"
    HEALTH = "health"


@dataclass(frozen=True, slots=True)
class OperationalSnapshotInput:
    """Operations-only telemetry that must never enter memory ingest."""

    snapshot_id: str
    snapshot_kind: OperationalSnapshotKind
    subject_id: str
    captured_at: datetime
    status: str
    payload: dict[str, JsonScalar]
    operations_only: bool = field(default=True, init=False)

    def __post_init__(self) -> None:
        _require_aware(self.captured_at, field_name="captured_at")
        if not self.subject_id.strip() or not self.status.strip():
            raise ValueError("subject_id and status must not be empty")


@dataclass(frozen=True, slots=True)
class SourceScanResult:
    events: tuple[IngestEventInput, ...] = ()
    collapsed_observations: tuple[CollapsedEventObservation, ...] = ()
    sources: tuple[SourceDescriptor, ...] = ()
    operations: tuple[OperationalSnapshotInput, ...] = ()
    diagnostics: tuple[ScanDiagnostic, ...] = ()
    files_scanned: int = 0
    bytes_scanned: int = 0
    records_scanned: int = 0
    extracted_records: int = 0
    extracted_chars: int = 0
    cursor_updates: tuple[SourceCursorUpdate, ...] = ()
    truncated: bool = False


@dataclass(slots=True)
class _ScanState:
    events: list[IngestEventInput] = field(default_factory=list)
    collapsed_observations: list[CollapsedEventObservation] = field(default_factory=list)
    sources: dict[tuple[str, str], SourceDescriptor] = field(default_factory=dict)
    diagnostics: list[ScanDiagnostic] = field(default_factory=list)
    files_scanned: int = 0
    bytes_scanned: int = 0
    records_scanned: int = 0
    extracted_records: int = 0
    extracted_chars: int = 0
    cursor_updates: list[SourceCursorUpdate] = field(default_factory=list)
    truncated: bool = False

    def result(self) -> SourceScanResult:
        return SourceScanResult(
            events=tuple(self.events),
            collapsed_observations=tuple(self.collapsed_observations),
            sources=tuple(self.sources.values()),
            diagnostics=tuple(self.diagnostics),
            files_scanned=self.files_scanned,
            bytes_scanned=self.bytes_scanned,
            records_scanned=self.records_scanned,
            extracted_records=self.extracted_records,
            extracted_chars=self.extracted_chars,
            cursor_updates=tuple(self.cursor_updates),
            truncated=self.truncated,
        )


class CodexSessionAdapter:
    """Parse explicitly selected Codex JSONL rollouts without reading config/auth."""

    def __init__(
        self,
        paths: Sequence[str | Path],
        *,
        limits: ScanLimits | None = None,
        opted_in: bool = True,
        sensitivity: Sensitivity = Sensitivity.NORMAL,
        allow_model_egress: bool = False,
        allow_private_model_egress: bool = False,
    ) -> None:
        if not paths:
            raise ValueError("at least one explicit Codex session path is required")
        _validate_adapter_egress(
            sensitivity=sensitivity,
            allow_model_egress=allow_model_egress,
            allow_private_model_egress=allow_private_model_egress,
        )
        self.paths = tuple(Path(path).expanduser() for path in paths)
        self.limits = limits or ScanLimits()
        self.opted_in = opted_in
        self.sensitivity = sensitivity
        self.allow_model_egress = allow_model_egress
        self.allow_private_model_egress = allow_private_model_egress

    def scan(
        self,
        *,
        cursor_loader: Callable[[str], str | None] | None = None,
    ) -> SourceScanResult:
        state = _ScanState()
        deadline = time.monotonic() + self.limits.max_wall_seconds
        discovery_limits = replace(
            self.limits,
            max_files=self.limits.max_files * _CODEX_INVENTORY_MULTIPLIER,
        )
        discovered = _discover_files(self.paths, {".jsonl"}, discovery_limits, state)
        unfinished: list[tuple[Path, str]] = []
        for path, logical_name in discovered:
            if self._cursor_at_eof(
                path,
                logical_name=logical_name,
                state=state,
                cursor_loader=cursor_loader,
            ):
                continue
            unfinished.append((path, logical_name))
        if len(unfinished) > self.limits.max_files:
            state.truncated = True
            state.diagnostics.append(
                ScanDiagnostic(
                    "file_limit_reached",
                    "remaining unfinished Codex files were deferred",
                )
            )
        files = unfinished[: self.limits.max_files]
        for path, logical_name in files:
            if time.monotonic() >= deadline:
                state.truncated = True
                state.diagnostics.append(
                    ScanDiagnostic(
                        "wall_time_limit_reached",
                        "remaining Codex files were deferred",
                        logical_name,
                    )
                )
                break
            if state.bytes_scanned >= self.limits.max_total_bytes:
                state.truncated = True
                break
            self._stream_file(
                path,
                logical_name,
                state,
                cursor_loader=cursor_loader,
                deadline=deadline,
            )
        return state.result()

    def _cursor_at_eof(
        self,
        path: Path,
        *,
        logical_name: str,
        state: _ScanState,
        cursor_loader: Callable[[str], str | None] | None,
    ) -> bool:
        if cursor_loader is None:
            return False
        cursor = _decode_codex_cursor(cursor_loader(_codex_cursor_name(path)))
        if cursor is None or bool(cursor.get("discarding_record")):
            return False
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError:
            return False
        try:
            file_stat = os.fstat(descriptor)
            if not stat.S_ISREG(file_stat.st_mode):
                return False
            prefix_length = int(cursor["prefix_length"])
            prefix_hash = hashlib.sha256(os.pread(descriptor, prefix_length, 0)).hexdigest()
            complete = (
                _cursor_matches_file(
                    cursor,
                    device=file_stat.st_dev,
                    inode=file_stat.st_ino,
                    size=file_stat.st_size,
                    prefix_hash=prefix_hash,
                )
                and int(cursor["offset"]) >= file_stat.st_size
            )
            if not complete:
                return False
            session_id = (
                str(cursor["session_id"])
                if cursor.get("session_id")
                else _stable_digest("codex-session-fallback", logical_name, prefix_hash)
            )
            for raw_source_kind in cursor.get("source_kinds", ()):
                source_kind = SourceKind(str(raw_source_kind))
                descriptor_from_cursor = _codex_descriptor(
                    session_id,
                    source_kind,
                    opted_in=self.opted_in,
                    sensitivity=self.sensitivity,
                    allow_model_egress=self.allow_model_egress,
                    allow_private_model_egress=self.allow_private_model_egress,
                )
                state.sources[
                    (descriptor_from_cursor.source_id, descriptor_from_cursor.partition_id)
                ] = descriptor_from_cursor
            return True
        finally:
            os.close(descriptor)

    def _stream_file(
        self,
        path: Path,
        logical_name: str,
        state: _ScanState,
        *,
        cursor_loader: Callable[[str], str | None] | None,
        deadline: float,
    ) -> None:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError:
            state.diagnostics.append(
                ScanDiagnostic("read_error", "source file could not be opened safely", logical_name)
            )
            return
        try:
            file_stat = os.fstat(descriptor)
            if not stat.S_ISREG(file_stat.st_mode):
                return
            cursor_name = _codex_cursor_name(path)
            cursor = _decode_codex_cursor(
                None if cursor_loader is None else cursor_loader(cursor_name)
            )
            prefix_length = (
                min(file_stat.st_size, 4096) if cursor is None else int(cursor["prefix_length"])
            )
            prefix_hash = hashlib.sha256(os.pread(descriptor, prefix_length, 0)).hexdigest()
            if cursor is not None and not _cursor_matches_file(
                cursor,
                device=file_stat.st_dev,
                inode=file_stat.st_ino,
                size=file_stat.st_size,
                prefix_hash=prefix_hash,
            ):
                state.diagnostics.append(
                    ScanDiagnostic(
                        "cursor_reset",
                        "Codex source identity changed; streaming restarted safely",
                        logical_name,
                    )
                )
                cursor = None

            offset = 0 if cursor is None else int(cursor["offset"])
            record_number = 0 if cursor is None else int(cursor["record_number"])
            discarding_record = bool(cursor and cursor["discarding_record"])
            session_id = (
                str(cursor["session_id"])
                if cursor is not None and cursor.get("session_id")
                else _stable_digest("codex-session-fallback", logical_name, prefix_hash)
            )
            session_time = (
                _parse_datetime(cursor.get("session_time")) if cursor is not None else None
            )
            source_kinds = {
                SourceKind(value)
                for value in (() if cursor is None else cursor.get("source_kinds", ()))
            }
            adjacent_primary_state = _decode_adjacent_primary_state(cursor)
            for source_kind in source_kinds:
                descriptor_from_cursor = _codex_descriptor(
                    session_id,
                    source_kind,
                    opted_in=self.opted_in,
                    sensitivity=self.sensitivity,
                    allow_model_egress=self.allow_model_egress,
                    allow_private_model_egress=self.allow_private_model_egress,
                )
                state.sources[
                    (descriptor_from_cursor.source_id, descriptor_from_cursor.partition_id)
                ] = descriptor_from_cursor
            records: list[tuple[int, Mapping[str, Any]]] = []
            extracted_chars = 0
            per_file_bytes = 0
            records_in_batch = 0
            state.files_scanned += 1

            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                stream.seek(offset)
                while stream.tell() < file_stat.st_size:
                    if time.monotonic() >= deadline:
                        state.truncated = True
                        state.diagnostics.append(
                            ScanDiagnostic(
                                "wall_time_limit_reached",
                                "remaining Codex records were deferred",
                                logical_name,
                                record_number + 1,
                            )
                        )
                        break
                    remaining = min(
                        self.limits.max_total_bytes - state.bytes_scanned,
                        self.limits.max_file_bytes - per_file_bytes,
                    )
                    if remaining <= 0:
                        state.truncated = True
                        break
                    if discarding_record:
                        chunk = stream.read(min(64 * 1024, remaining))
                        if not chunk:
                            break
                        newline = chunk.find(b"\n")
                        consumed = len(chunk) if newline < 0 else newline + 1
                        if consumed < len(chunk):
                            stream.seek(stream.tell() - (len(chunk) - consumed))
                        per_file_bytes += consumed
                        state.bytes_scanned += consumed
                        offset = stream.tell()
                        if newline >= 0:
                            discarding_record = False
                        continue

                    if records_in_batch >= self.limits.max_records_per_file:
                        state.truncated = True
                        state.diagnostics.append(
                            ScanDiagnostic(
                                "record_limit_reached",
                                "remaining JSONL records were not parsed",
                                logical_name,
                                record_number + 1,
                            )
                        )
                        break
                    line_start = stream.tell()
                    read_limit = min(self.limits.max_record_bytes + 1, remaining)
                    raw_line = stream.readline(read_limit)
                    if not raw_line:
                        break
                    per_file_bytes += len(raw_line)
                    state.bytes_scanned += len(raw_line)
                    at_eof = stream.tell() >= file_stat.st_size
                    complete_line = raw_line.endswith(b"\n") or at_eof
                    oversized = len(raw_line) > self.limits.max_record_bytes
                    if oversized or not complete_line:
                        if not oversized and read_limit < self.limits.max_record_bytes + 1:
                            stream.seek(line_start)
                            offset = line_start
                            state.truncated = True
                            state.diagnostics.append(
                                ScanDiagnostic(
                                    "raw_byte_limit_reached",
                                    "next JSONL record was deferred without partial parsing",
                                    logical_name,
                                    record_number + 1,
                                )
                            )
                            break
                        record_number += 1
                        records_in_batch += 1
                        state.records_scanned += 1
                        state.diagnostics.append(
                            ScanDiagnostic(
                                "oversized_record_skipped",
                                "oversized JSONL record is being discarded without copying",
                                logical_name,
                                record_number,
                            )
                        )
                        discarding_record = not raw_line.endswith(b"\n") and not at_eof
                        offset = stream.tell()
                        continue

                    record_number += 1
                    records_in_batch += 1
                    state.records_scanned += 1
                    offset = stream.tell()
                    try:
                        record = json.loads(raw_line.decode("utf-8"))
                    except UnicodeDecodeError:
                        state.diagnostics.append(
                            ScanDiagnostic(
                                "invalid_utf8",
                                "JSONL record was not valid UTF-8 and was skipped",
                                logical_name,
                                record_number,
                            )
                        )
                        continue
                    except json.JSONDecodeError:
                        state.diagnostics.append(
                            ScanDiagnostic(
                                "malformed_json",
                                "JSONL record was skipped",
                                logical_name,
                                record_number,
                            )
                        )
                        continue
                    if not isinstance(record, Mapping):
                        state.diagnostics.append(
                            ScanDiagnostic(
                                "malformed_record",
                                "JSONL record must be an object",
                                logical_name,
                                record_number,
                            )
                        )
                        continue
                    observed_identity = _codex_session_meta(record)
                    if observed_identity is not None:
                        session_id, observed_time = observed_identity
                        session_time = observed_time or session_time
                        continue
                    parsed = _parse_codex_record(record)
                    if parsed is None:
                        continue
                    next_chars = len(parsed[2])
                    if (
                        len(records) >= self.limits.max_extracted_records_per_file
                        or extracted_chars + next_chars > self.limits.max_extracted_chars_per_file
                    ):
                        stream.seek(line_start)
                        offset = line_start
                        record_number -= 1
                        records_in_batch -= 1
                        state.records_scanned -= 1
                        state.truncated = True
                        state.diagnostics.append(
                            ScanDiagnostic(
                                "extracted_batch_limit_reached",
                                "relevant Codex record was deferred atomically",
                                logical_name,
                                record_number + 1,
                            )
                        )
                        break
                    records.append((record_number, record))
                    extracted_chars += next_chars

            self._parse_records(
                path,
                logical_name,
                records,
                state,
                session_id=session_id,
                session_time=session_time,
                adjacent_primary_state=adjacent_primary_state,
            )
            for source_kind in (
                SourceKind.CODEX_TASK,
                SourceKind.ASSISTANT_FINAL,
                SourceKind.TOOL_RESULT,
                SourceKind.DREAMING_HANDOFF,
            ):
                expected_descriptor = _codex_descriptor(
                    session_id,
                    source_kind,
                    opted_in=self.opted_in,
                    sensitivity=self.sensitivity,
                    allow_model_egress=self.allow_model_egress,
                    allow_private_model_egress=self.allow_private_model_egress,
                )
                if (
                    expected_descriptor.source_id,
                    expected_descriptor.partition_id,
                ) in state.sources:
                    source_kinds.add(source_kind)
            state.extracted_records += len(records)
            state.extracted_chars += extracted_chars
            if offset < file_stat.st_size:
                state.truncated = True
                state.diagnostics.append(
                    ScanDiagnostic(
                        "stream_batch_limit_reached",
                        "Codex JSONL has more bytes for a later bounded batch",
                        logical_name,
                        record_number + 1,
                    )
                )
            state.cursor_updates.append(
                SourceCursorUpdate(
                    cursor_name,
                    _encode_codex_cursor(
                        offset=offset,
                        record_number=record_number,
                        discarding_record=discarding_record,
                        session_id=session_id,
                        session_time=session_time,
                        device=file_stat.st_dev,
                        inode=file_stat.st_ino,
                        prefix_hash=prefix_hash,
                        prefix_length=prefix_length,
                        source_kinds=source_kinds,
                        adjacent_primary_state=adjacent_primary_state,
                    ),
                )
            )
        finally:
            os.close(descriptor)

    def _parse_records(
        self,
        path: Path,
        logical_name: str,
        records: Sequence[tuple[int, Mapping[str, Any]]],
        state: _ScanState,
        *,
        session_id: str,
        session_time: datetime | None,
        adjacent_primary_state: dict[str, dict[str, str]],
    ) -> None:
        seen_event_fingerprints: set[str] = set()
        for record_number, record in records:
            parsed = _parse_codex_record(record)
            if parsed is None:
                continue
            role, source_kind, content, item_identity, phase = parsed
            if len(content) > self.limits.max_content_chars:
                state.diagnostics.append(
                    ScanDiagnostic(
                        code="content_limit_exceeded",
                        message="oversized Codex record was skipped",
                        path=logical_name,
                        record_number=record_number,
                    )
                )
                continue
            occurred_at = _parse_datetime(record.get("timestamp")) or _payload_timestamp(record)
            occurred_at = occurred_at or session_time
            if occurred_at is None:
                state.diagnostics.append(
                    ScanDiagnostic(
                        code="missing_timestamp",
                        message="record has no stable timestamp and was skipped",
                        path=logical_name,
                        record_number=record_number,
                    )
                )
                continue

            normalized = normalize_content(content)
            if not normalized:
                continue
            handoff = parse_dreaming_handoff(normalized) if role == "assistant" else None
            primary_content = normalized
            if handoff is not None:
                marker_index = normalized.rfind("[DREAMING_HANDOFF]")
                primary_content = normalize_content(normalized[:marker_index])
            if primary_content:
                content_hash = hashlib.sha256(primary_content.encode()).hexdigest()
                lineage = _stable_digest(session_id, source_kind.value, role)
                family = _stable_digest(lineage, role, content_hash)
                descriptor = _codex_descriptor(
                    session_id,
                    source_kind,
                    opted_in=self.opted_in,
                    sensitivity=self.sensitivity,
                    allow_model_egress=self.allow_model_egress,
                    allow_private_model_egress=self.allow_private_model_egress,
                )
                state.sources[(descriptor.source_id, descriptor.partition_id)] = descriptor
                external_id = (
                    "codex_"
                    + _stable_digest(
                        session_id,
                        role,
                        phase,
                        item_identity,
                        occurred_at.isoformat(),
                        primary_content,
                    )[:32]
                )
                previous = adjacent_primary_state.get(lineage)
                duplicate = _is_adjacent_duplicate(previous, family, occurred_at)
                if duplicate:
                    state.diagnostics.append(
                        ScanDiagnostic(
                            code="adjacent_duplicate_collapsed",
                            message=(
                                "adjacent Codex observation was merged into its evidence family"
                            ),
                            path=logical_name,
                            record_number=record_number,
                        )
                    )
                    _record_collapsed_provenance(
                        state,
                        source_id=descriptor.source_id,
                        partition_id=descriptor.partition_id,
                        lineage=lineage,
                        family=family,
                        representative_external_event_id=(
                            None
                            if previous is None
                            else previous.get("representative_external_event_id")
                        ),
                        representative_occurred_at=(
                            occurred_at
                            if previous is None
                            else _parse_datetime(previous.get("representative_occurred_at"))
                            or _parse_datetime(previous.get("occurred_at"))
                            or occurred_at
                        ),
                        observed_at=occurred_at,
                        session_id=session_id,
                        record_number=record_number,
                        record_type=str(record.get("type") or "unknown"),
                        item_identity=item_identity,
                    )
                    if previous is not None:
                        previous["occurred_at"] = occurred_at.isoformat()
                    continue
                adjacent_primary_state[lineage] = {
                    "family": family,
                    "occurred_at": occurred_at.isoformat(),
                    "representative_external_event_id": external_id,
                    "representative_occurred_at": occurred_at.isoformat(),
                }
                event_fingerprint = _stable_digest(
                    session_id,
                    role,
                    source_kind.value,
                    occurred_at.isoformat(),
                    primary_content,
                )
                if event_fingerprint not in seen_event_fingerprints:
                    seen_event_fingerprints.add(event_fingerprint)
                    state.sources[(descriptor.source_id, descriptor.partition_id)] = descriptor
                    safe_content, sensitivity, redaction_count = _sanitize_event_content(
                        primary_content
                    )
                    state.events.append(
                        IngestEventInput(
                            source_id=descriptor.source_id,
                            partition_id=descriptor.partition_id,
                            source_kind=source_kind,
                            external_event_id=external_id,
                            occurred_at=occurred_at,
                            content=safe_content,
                            sensitivity=(
                                sensitivity
                                if sensitivity is Sensitivity.SECRET
                                else self.sensitivity
                            ),
                            source_locator=f"{logical_name}:record={record_number}",
                            metadata={
                                "adapter_parser": _PARSER_VERSION,
                                "canonical_support": _canonical_support(source_kind),
                                "phase": phase,
                                "redaction_count": redaction_count,
                                "role": role,
                                "source_lineage_fingerprint": lineage,
                                "source_sequence": record_number,
                                "collapsed_observation_count": 1,
                                "collapsed_provenance_fingerprint": _stable_digest(
                                    session_id,
                                    record_number,
                                    record.get("type"),
                                    item_identity,
                                ),
                            },
                        )
                    )

            if handoff is None:
                continue
            handoff_content = _format_handoff(handoff)
            handoff_fingerprint = _stable_digest(
                session_id,
                SourceKind.DREAMING_HANDOFF.value,
                occurred_at.isoformat(),
                handoff_content,
            )
            if handoff_fingerprint in seen_event_fingerprints:
                continue
            seen_event_fingerprints.add(handoff_fingerprint)
            handoff_descriptor = _codex_descriptor(
                session_id,
                SourceKind.DREAMING_HANDOFF,
                opted_in=self.opted_in,
                sensitivity=self.sensitivity,
                allow_model_egress=self.allow_model_egress,
                allow_private_model_egress=self.allow_private_model_egress,
            )
            state.sources[(handoff_descriptor.source_id, handoff_descriptor.partition_id)] = (
                handoff_descriptor
            )
            state.events.append(
                IngestEventInput(
                    source_id=handoff_descriptor.source_id,
                    partition_id=handoff_descriptor.partition_id,
                    source_kind=SourceKind.DREAMING_HANDOFF,
                    external_event_id="codex_"
                    + _stable_digest(
                        session_id,
                        SourceKind.DREAMING_HANDOFF.value,
                        item_identity,
                        occurred_at.isoformat(),
                        handoff_content,
                    )[:32],
                    occurred_at=occurred_at,
                    content=handoff_content,
                    sensitivity=(
                        Sensitivity.SECRET if handoff.redaction_count else self.sensitivity
                    ),
                    source_locator=f"{logical_name}:record={record_number}:handoff",
                    metadata={
                        "adapter_parser": _PARSER_VERSION,
                        "canonical_support": "project_state_only",
                        "claim_scope": "project_state",
                        "redaction_count": handoff.redaction_count,
                        "terminal": True,
                    },
                )
            )


class AdvisoryMarkdownAdapter:
    """Read persisted Markdown as hints that cannot independently support claims."""

    def __init__(
        self,
        paths: Sequence[str | Path],
        *,
        source_kind: SourceKind,
        namespace: str,
        limits: ScanLimits | None = None,
        default_occurred_at: datetime | None = None,
        default_timezone: tzinfo = UTC,
        opted_in: bool = True,
        sensitivity: Sensitivity = Sensitivity.NORMAL,
        allow_model_egress: bool = False,
        allow_private_model_egress: bool = False,
    ) -> None:
        if source_kind not in {SourceKind.CHRONICLE, SourceKind.CODEX_MEMORY}:
            raise ValueError("advisory Markdown must be Chronicle or Codex memory")
        if not paths:
            raise ValueError("at least one explicit advisory Markdown path is required")
        _validate_adapter_egress(
            sensitivity=sensitivity,
            allow_model_egress=allow_model_egress,
            allow_private_model_egress=allow_private_model_egress,
        )
        if default_occurred_at is not None:
            _require_aware(default_occurred_at, field_name="default_occurred_at")
        self.paths = tuple(Path(path).expanduser() for path in paths)
        self.source_kind = source_kind
        self.namespace = namespace.strip()
        if not self.namespace:
            raise ValueError("namespace must not be empty")
        self.limits = limits or ScanLimits(max_file_bytes=2 * 1024 * 1024)
        self.default_occurred_at = default_occurred_at
        self.default_timezone = default_timezone
        self.opted_in = opted_in
        self.sensitivity = sensitivity
        self.allow_model_egress = allow_model_egress
        self.allow_private_model_egress = allow_private_model_egress

    def scan(self) -> SourceScanResult:
        state = _ScanState()
        files = _discover_files(self.paths, {".md", ".markdown"}, self.limits, state)
        for path, logical_name in files:
            document = _read_bounded_text(path, logical_name, self.limits, state)
            if document is None:
                continue
            text, byte_count = document
            state.files_scanned += 1
            state.bytes_scanned += byte_count
            normalized = normalize_content(text)
            if not normalized:
                state.diagnostics.append(
                    ScanDiagnostic(
                        code="empty_document",
                        message="empty advisory document was skipped",
                        path=logical_name,
                    )
                )
                continue
            if len(normalized) > self.limits.max_content_chars:
                state.diagnostics.append(
                    ScanDiagnostic(
                        code="content_limit_exceeded",
                        message="oversized advisory document was skipped",
                        path=logical_name,
                    )
                )
                continue
            occurred_at = (
                _markdown_occurred_at(normalized, self.default_timezone) or self.default_occurred_at
            )
            if occurred_at is None:
                state.diagnostics.append(
                    ScanDiagnostic(
                        code="missing_timestamp",
                        message="advisory document has no stable timestamp and was skipped",
                        path=logical_name,
                    )
                )
                continue
            descriptor = _advisory_descriptor(
                namespace=self.namespace,
                logical_name=logical_name,
                source_kind=self.source_kind,
                opted_in=self.opted_in,
                sensitivity=self.sensitivity,
                allow_model_egress=self.allow_model_egress,
                allow_private_model_egress=self.allow_private_model_egress,
            )
            state.sources[(descriptor.source_id, descriptor.partition_id)] = descriptor
            safe_content, sensitivity, redaction_count = _sanitize_event_content(normalized)
            external_id = (
                "advisory_"
                + _stable_digest(
                    self.namespace,
                    logical_name,
                    occurred_at.isoformat(),
                    safe_content,
                )[:32]
            )
            state.events.append(
                IngestEventInput(
                    source_id=descriptor.source_id,
                    partition_id=descriptor.partition_id,
                    source_kind=self.source_kind,
                    external_event_id=external_id,
                    occurred_at=occurred_at,
                    content=safe_content,
                    sensitivity=(
                        sensitivity if sensitivity is Sensitivity.SECRET else self.sensitivity
                    ),
                    source_locator=logical_name,
                    metadata={
                        "adapter_parser": _PARSER_VERSION,
                        "advisory": True,
                        "canonical_support": False,
                        "persisted_summary_only": True,
                        "redaction_count": redaction_count,
                    },
                )
            )
        return state.result()


class ChronicleSummaryAdapter(AdvisoryMarkdownAdapter):
    """Chronicle adapter restricted to explicitly selected persisted summaries."""

    def __init__(self, paths: Sequence[str | Path], **kwargs: Any) -> None:
        super().__init__(
            paths,
            source_kind=SourceKind.CHRONICLE,
            namespace="chronicle:persisted-summary",
            **kwargs,
        )


def extract_terminal_handoff(
    final_text: str,
    *,
    task_id: str,
    occurred_at: datetime,
    opted_in: bool = True,
    sensitivity: Sensitivity = Sensitivity.NORMAL,
    allow_model_egress: bool = False,
    allow_private_model_egress: bool = False,
) -> SourceScanResult:
    """Convert one explicitly supplied terminal handoff into an ingest record."""

    _require_aware(occurred_at, field_name="occurred_at")
    _validate_adapter_egress(
        sensitivity=sensitivity,
        allow_model_egress=allow_model_egress,
        allow_private_model_egress=allow_private_model_egress,
    )
    handoff = parse_dreaming_handoff(final_text)
    if handoff is None:
        return SourceScanResult(
            diagnostics=(
                ScanDiagnostic(
                    code="invalid_terminal_handoff",
                    message="text does not end in one valid DREAMING_HANDOFF block",
                ),
            )
        )
    descriptor = _codex_descriptor(
        task_id,
        SourceKind.DREAMING_HANDOFF,
        opted_in=opted_in,
        sensitivity=sensitivity,
        allow_model_egress=allow_model_egress,
        allow_private_model_egress=allow_private_model_egress,
    )
    safe_block = _format_handoff(handoff)
    external_id = "handoff_" + _stable_digest(task_id, occurred_at.isoformat(), safe_block)[:32]
    event = IngestEventInput(
        source_id=descriptor.source_id,
        partition_id=descriptor.partition_id,
        source_kind=SourceKind.DREAMING_HANDOFF,
        external_event_id=external_id,
        occurred_at=occurred_at,
        content=safe_block,
        sensitivity=Sensitivity.SECRET if handoff.redaction_count else sensitivity,
        metadata={
            "adapter_parser": _PARSER_VERSION,
            "canonical_support": "project_state_only",
            "claim_scope": "project_state",
            "redaction_count": handoff.redaction_count,
            "terminal": True,
        },
    )
    return SourceScanResult(events=(event,), sources=(descriptor,))


def capture_workspace_snapshot(
    workspace_path: str | Path,
    *,
    captured_at: datetime,
    git_dirty_count: int | None = None,
    extra: Mapping[str, JsonScalar] | None = None,
) -> OperationalSnapshotInput:
    """Capture shallow workspace telemetry without walking files or invoking Git."""

    _require_aware(captured_at, field_name="captured_at")
    if git_dirty_count is not None and git_dirty_count < 0:
        raise ValueError("git_dirty_count cannot be negative")
    requested = Path(workspace_path).expanduser()
    exists = requested.exists()
    is_symlink = requested.is_symlink()
    resolved = requested.resolve(strict=False)
    git_branch = _read_git_head(resolved) if exists and not is_symlink else None
    safe_path = redact_secrets(str(resolved)).text
    payload: dict[str, JsonScalar] = {
        "exists": exists,
        "git_branch": git_branch,
        "git_dirty_count": git_dirty_count,
        "is_directory": requested.is_dir() if exists and not is_symlink else False,
        "is_symlink": is_symlink,
        "workspace_path": safe_path,
    }
    payload.update(_sanitize_scalar_mapping(extra or {}))
    status = "available" if exists and not is_symlink else "unavailable"
    return _operational_snapshot(
        OperationalSnapshotKind.WORKSPACE,
        subject_id=_opaque_subject("workspace", safe_path),
        captured_at=captured_at,
        status=status,
        payload=payload,
    )


def build_automation_snapshot(
    *,
    automation_type: str,
    automation_id: str,
    status: str,
    captured_at: datetime,
    payload: Mapping[str, JsonScalar] | None = None,
) -> OperationalSnapshotInput:
    """Normalize caller-observed automation status as operations-only telemetry."""

    _require_aware(captured_at, field_name="captured_at")
    safe_type = redact_secrets(automation_type.strip()).text
    safe_id = redact_secrets(automation_id.strip()).text
    safe_status = redact_secrets(status.strip()).text
    if not safe_type or not safe_id or not safe_status:
        raise ValueError("automation type, id, and status must not be empty")
    values = _sanitize_scalar_mapping(payload or {})
    values.update({"automation_id": safe_id, "automation_type": safe_type})
    return _operational_snapshot(
        OperationalSnapshotKind.AUTOMATION,
        subject_id=_opaque_subject(safe_type, safe_id),
        captured_at=captured_at,
        status=safe_status,
        payload=values,
    )


def capture_health_snapshot(
    checks: Mapping[str, str | Path],
    *,
    captured_at: datetime,
) -> OperationalSnapshotInput:
    """Record only availability booleans for explicit local paths, never contents."""

    _require_aware(captured_at, field_name="captured_at")
    values: dict[str, JsonScalar] = {}
    for label in sorted(checks):
        safe_label = redact_secrets(label).text
        if not safe_label or safe_label == REDACTED_SECRET:
            safe_label = "redacted_check_" + _stable_digest(label)[:12]
        target = Path(checks[label]).expanduser()
        values[safe_label] = target.exists() and not target.is_symlink()
    available = sum(value is True for value in values.values())
    values["available_count"] = available
    values["check_count"] = len(checks)
    status = "healthy" if available == len(checks) else "degraded"
    return _operational_snapshot(
        OperationalSnapshotKind.HEALTH,
        subject_id="local-health",
        captured_at=captured_at,
        status=status,
        payload=values,
    )


def _discover_files(
    inputs: Sequence[Path],
    suffixes: set[str],
    limits: ScanLimits,
    state: _ScanState,
) -> list[tuple[Path, str]]:
    discovered: list[tuple[Path, str]] = []
    seen: set[str] = set()
    for supplied in sorted(inputs, key=lambda item: str(item)):
        if supplied.is_symlink():
            state.diagnostics.append(
                ScanDiagnostic(
                    code="symlink_skipped",
                    message="symbolic-link source was not followed",
                    path=str(supplied),
                )
            )
            continue
        if supplied.is_file():
            _add_discovered_file(supplied, supplied.name, suffixes, discovered, seen, state)
            continue
        if not supplied.is_dir():
            state.diagnostics.append(
                ScanDiagnostic(
                    code="path_unavailable",
                    message="explicit source path does not exist or is not a regular file",
                    path=str(supplied),
                )
            )
            continue
        root = supplied.resolve()
        for current, directory_names, file_names in os.walk(root, followlinks=False):
            current_path = Path(current)
            depth = len(current_path.relative_to(root).parts)
            directory_names[:] = sorted(
                name
                for name in directory_names
                if depth < limits.max_directory_depth and not (current_path / name).is_symlink()
            )
            for name in sorted(file_names):
                candidate = current_path / name
                logical_name = str(candidate.relative_to(root))
                if candidate.is_symlink():
                    state.diagnostics.append(
                        ScanDiagnostic(
                            code="symlink_skipped",
                            message="symbolic-link source was not followed",
                            path=logical_name,
                        )
                    )
                    continue
                _add_discovered_file(candidate, logical_name, suffixes, discovered, seen, state)

    discovered.sort(key=lambda item: (item[1], str(item[0])))
    if len(discovered) > limits.max_files:
        state.truncated = True
        state.diagnostics.append(
            ScanDiagnostic(
                code="file_limit_reached",
                message="remaining source files were not scanned",
            )
        )
        return discovered[: limits.max_files]
    return discovered


def _add_discovered_file(
    candidate: Path,
    logical_name: str,
    suffixes: set[str],
    discovered: list[tuple[Path, str]],
    seen: set[str],
    state: _ScanState,
) -> None:
    if candidate.suffix.casefold() not in suffixes:
        return
    if candidate.name.casefold() in _CREDENTIAL_BASENAMES:
        state.diagnostics.append(
            ScanDiagnostic(
                code="credential_path_skipped",
                message="credential-looking source file was not read",
                path=logical_name,
            )
        )
        return
    resolved = str(candidate.resolve())
    if resolved not in seen:
        seen.add(resolved)
        discovered.append((candidate, logical_name))


def _read_bounded_text(
    path: Path,
    logical_name: str,
    limits: ScanLimits,
    state: _ScanState,
) -> tuple[str, int] | None:
    try:
        file_stat = path.stat(follow_symlinks=False)
    except OSError:
        state.diagnostics.append(
            ScanDiagnostic(
                code="read_error",
                message="source file metadata could not be read",
                path=logical_name,
            )
        )
        return None
    if not stat.S_ISREG(file_stat.st_mode):
        return None
    if file_stat.st_size > limits.max_file_bytes:
        state.diagnostics.append(
            ScanDiagnostic(
                code="file_too_large",
                message="source file exceeds the per-file byte limit",
                path=logical_name,
            )
        )
        return None
    if state.bytes_scanned + file_stat.st_size > limits.max_total_bytes:
        state.truncated = True
        state.diagnostics.append(
            ScanDiagnostic(
                code="total_byte_limit_reached",
                message="remaining source files were not read",
                path=logical_name,
            )
        )
        return None
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            raw = os.read(descriptor, limits.max_file_bytes + 1)
        finally:
            os.close(descriptor)
    except OSError:
        state.diagnostics.append(
            ScanDiagnostic(
                code="read_error",
                message="source file could not be read safely",
                path=logical_name,
            )
        )
        return None
    if len(raw) > limits.max_file_bytes:
        state.diagnostics.append(
            ScanDiagnostic(
                code="file_too_large",
                message="source file changed beyond the per-file byte limit",
                path=logical_name,
            )
        )
        return None
    try:
        return raw.decode("utf-8"), len(raw)
    except UnicodeDecodeError:
        state.diagnostics.append(
            ScanDiagnostic(
                code="invalid_utf8",
                message="source file was not valid UTF-8 and was skipped",
                path=logical_name,
            )
        )
        return None


def _codex_cursor_name(path: Path) -> str:
    resolved_fingerprint = hashlib.sha256(str(path.resolve()).encode()).hexdigest()
    return f"codex-jsonl:{resolved_fingerprint}"


def _decode_codex_cursor(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            return None
        if decoded.get("version") != _CODEX_CURSOR_VERSION:
            return None
        if decoded.get("parser_version") not in _COMPATIBLE_CURSOR_PARSER_VERSIONS:
            return None
        for field_name in ("offset", "record_number", "device", "inode", "prefix_length"):
            if not isinstance(decoded.get(field_name), int) or decoded[field_name] < 0:
                return None
        if not isinstance(decoded.get("prefix_hash"), str):
            return None
        if not isinstance(decoded.get("discarding_record"), bool):
            return None
        adjacent_state = decoded.get("adjacent_primary_state", {})
        if not isinstance(adjacent_state, dict) or any(
            not isinstance(key, str)
            or not isinstance(item, dict)
            or not isinstance(item.get("family"), str)
            or not isinstance(item.get("occurred_at"), str)
            or (
                item.get("representative_external_event_id") is not None
                and not isinstance(item.get("representative_external_event_id"), str)
            )
            or (
                item.get("representative_occurred_at") is not None
                and not isinstance(item.get("representative_occurred_at"), str)
            )
            for key, item in adjacent_state.items()
        ):
            return None
        source_kinds = decoded.get("source_kinds")
        allowed_source_kinds = {item.value for item in SourceKind}
        if not isinstance(source_kinds, list) or any(
            not isinstance(value, str) or value not in allowed_source_kinds
            for value in source_kinds
        ):
            return None
        return decoded
    except json.JSONDecodeError, TypeError:
        return None


def _cursor_matches_file(
    cursor: Mapping[str, Any],
    *,
    device: int,
    inode: int,
    size: int,
    prefix_hash: str,
) -> bool:
    return (
        int(cursor["device"]) == device
        and int(cursor["inode"]) == inode
        and int(cursor["offset"]) <= size
        and str(cursor["prefix_hash"]) == prefix_hash
    )


def _encode_codex_cursor(
    *,
    offset: int,
    record_number: int,
    discarding_record: bool,
    session_id: str,
    session_time: datetime | None,
    device: int,
    inode: int,
    prefix_hash: str,
    prefix_length: int,
    source_kinds: set[SourceKind],
    adjacent_primary_state: Mapping[str, Mapping[str, str]],
) -> str:
    return json.dumps(
        {
            "device": device,
            "discarding_record": discarding_record,
            "inode": inode,
            "offset": offset,
            "parser_version": _PARSER_VERSION,
            "prefix_hash": prefix_hash,
            "prefix_length": prefix_length,
            "record_number": record_number,
            "session_id": session_id,
            "session_time": None if session_time is None else session_time.isoformat(),
            "source_kinds": sorted(source_kind.value for source_kind in source_kinds),
            "adjacent_primary_state": adjacent_primary_state,
            "version": _CODEX_CURSOR_VERSION,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_adjacent_primary_state(
    cursor: Mapping[str, Any] | None,
) -> dict[str, dict[str, str]]:
    if cursor is None:
        return {}
    raw = cursor.get("adjacent_primary_state", {})
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, dict[str, str]] = {}
    for key, item in raw.items():
        if not isinstance(key, str) or not isinstance(item, Mapping):
            continue
        family = item.get("family")
        occurred_at = item.get("occurred_at")
        if isinstance(family, str) and isinstance(occurred_at, str):
            decoded = {"family": family, "occurred_at": occurred_at}
            representative_external_event_id = item.get("representative_external_event_id")
            representative_occurred_at = item.get("representative_occurred_at")
            if isinstance(representative_external_event_id, str):
                decoded["representative_external_event_id"] = representative_external_event_id
            if isinstance(representative_occurred_at, str):
                decoded["representative_occurred_at"] = representative_occurred_at
            result[key] = decoded
    return result


def _is_adjacent_duplicate(
    previous: Mapping[str, str] | None,
    family: str,
    occurred_at: datetime,
) -> bool:
    if previous is None or previous.get("family") != family:
        return False
    previous_time = _parse_datetime(previous.get("occurred_at"))
    if previous_time is None:
        return False
    delta = (occurred_at - previous_time).total_seconds()
    return 0.0 <= delta <= _ADJACENT_DUPLICATE_SECONDS


def _record_collapsed_provenance(
    state: _ScanState,
    *,
    source_id: str,
    partition_id: str,
    lineage: str,
    family: str,
    representative_external_event_id: str | None,
    representative_occurred_at: datetime,
    observed_at: datetime,
    session_id: str,
    record_number: int,
    record_type: str,
    item_identity: str,
) -> None:
    observation_fingerprint = _stable_digest(
        "collapsed-observation-v1",
        session_id,
        record_number,
        record_type,
        item_identity,
        observed_at.isoformat(),
    )
    state.collapsed_observations.append(
        CollapsedEventObservation(
            source_id=source_id,
            partition_id=partition_id,
            representative_external_event_id=representative_external_event_id,
            representative_occurred_at=representative_occurred_at,
            source_lineage_fingerprint=lineage,
            evidence_family_fingerprint=family,
            observation_fingerprint=observation_fingerprint,
            observed_at=observed_at,
        )
    )
    for index in range(len(state.events) - 1, -1, -1):
        event = state.events[index]
        if (
            event.source_id != source_id
            or event.partition_id != partition_id
            or event.metadata.get("source_lineage_fingerprint") != lineage
        ):
            continue
        metadata = dict(event.metadata)
        prior = str(metadata.get("collapsed_provenance_fingerprint") or "")
        metadata["collapsed_observation_count"] = (
            int(metadata.get("collapsed_observation_count") or 1) + 1
        )
        metadata["collapsed_provenance_fingerprint"] = _stable_digest(
            prior,
            observation_fingerprint,
        )
        state.events[index] = replace(event, metadata=metadata)
        return


def _codex_session_meta(record: Mapping[str, Any]) -> tuple[str, datetime | None] | None:
    if record.get("type") != "session_meta":
        return None
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    identifier = payload.get("id")
    if not isinstance(identifier, str) or not identifier.strip():
        return None
    timestamp = _parse_datetime(record.get("timestamp")) or _parse_datetime(
        payload.get("timestamp")
    )
    return identifier.strip(), timestamp


def _parse_codex_record(
    record: Mapping[str, Any],
) -> tuple[str, SourceKind, str, str, str] | None:
    record_type = record.get("type")
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    if record_type == "event_msg" and payload.get("type") == "user_message":
        content = payload.get("message") or payload.get("text")
        if isinstance(content, str):
            return "user", SourceKind.CODEX_TASK, content, "event-user", "input"
        return None
    if record_type != "response_item":
        return None

    payload_type = payload.get("type")
    if payload_type == "message":
        role = payload.get("role")
        if role not in {"user", "assistant"}:
            return None
        content = _message_text(payload.get("content"))
        if not content:
            return None
        phase = str(payload.get("phase") or ("input" if role == "user" else "unknown"))
        if role == "assistant" and phase != "final_answer":
            if parse_dreaming_handoff(content) is None:
                return None
            phase = "final_answer"
        source_kind = SourceKind.CODEX_TASK if role == "user" else SourceKind.ASSISTANT_FINAL
        identity = str(payload.get("id") or payload.get("item_id") or payload_type)
        return str(role), source_kind, content, identity, phase

    if payload_type in {"function_call_output", "custom_tool_call_output", "tool_result"}:
        output = payload.get("output", payload.get("content"))
        content = _tool_output_text(output)
        if not content:
            return None
        identity = str(payload.get("call_id") or payload.get("id") or payload_type)
        return "tool", SourceKind.TOOL_RESULT, content, identity, "tool_result"
    return None


def _message_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        return ""
    chunks: list[str] = []
    for item in value:
        if isinstance(item, str):
            chunks.append(item)
        elif isinstance(item, Mapping):
            text = item.get("text")
            if isinstance(text, str):
                chunks.append(text)
    return "\n".join(chunks)


def _tool_output_text(value: object) -> str:
    """Keep bounded textual verification while refusing image/blob payload trees."""

    if isinstance(value, str):
        return value
    sequence_text = _message_text(value)
    if sequence_text:
        return sequence_text
    if not isinstance(value, Mapping):
        return ""
    chunks: list[str] = []
    for key in ("text", "output", "content", "result", "error", "status", "exit_code"):
        item = value.get(key)
        if isinstance(item, str):
            chunks.append(item)
        elif isinstance(item, int | float | bool):
            chunks.append(f"{key}: {item}")
        else:
            nested_text = _message_text(item)
            if nested_text:
                chunks.append(nested_text)
    return "\n".join(chunks)


def _payload_timestamp(record: Mapping[str, Any]) -> datetime | None:
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return None
    return _parse_datetime(payload.get("timestamp"))


def _parse_datetime(value: object, default_timezone: tzinfo | None = None) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip().strip("\"'")
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        if default_timezone is None:
            return None
        parsed = parsed.replace(tzinfo=default_timezone)
    return parsed


def _validate_adapter_egress(
    *,
    sensitivity: Sensitivity,
    allow_model_egress: bool,
    allow_private_model_egress: bool,
) -> None:
    if sensitivity is Sensitivity.SECRET and (allow_model_egress or allow_private_model_egress):
        raise ValueError("secret adapter sources cannot enable model egress")
    if allow_private_model_egress and sensitivity is not Sensitivity.PRIVATE:
        raise ValueError("private model egress requires private source sensitivity")


def _markdown_occurred_at(text: str, default_timezone: tzinfo) -> datetime | None:
    lines = text.splitlines()
    if len(lines) < 3 or lines[0].strip() != "---":
        return None
    for line in lines[1:]:
        if line.strip() == "---":
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key.strip().casefold() not in _FRONTMATTER_TIME_KEYS:
            continue
        return _parse_datetime(value, default_timezone)
    return None


def _codex_descriptor(
    session_id: str,
    source_kind: SourceKind,
    *,
    opted_in: bool,
    sensitivity: Sensitivity,
    allow_model_egress: bool,
    allow_private_model_egress: bool,
) -> SourceDescriptor:
    identity = _stable_digest("codex", session_id, source_kind.value)
    source_id = f"src_{source_kind.value}_{identity[:20]}"
    partition_id = f"part_{source_kind.value}_{identity[20:40]}"
    trust_level = "tool_verified" if source_kind is SourceKind.TOOL_RESULT else "conversation"
    return SourceDescriptor(
        source_id=source_id,
        partition_id=partition_id,
        source_kind=source_kind,
        source_fingerprint=_stable_digest("source", source_kind.value, session_id),
        partition_fingerprint=_stable_digest("partition", source_kind.value, session_id),
        display_name=f"Codex {source_kind.value}",
        trust_level=trust_level,
        sensitivity=sensitivity,
        opted_in=opted_in,
        model_egress_allowed=allow_model_egress,
        allow_private_model_egress=allow_private_model_egress,
        metadata={
            "adapter_parser": _PARSER_VERSION,
            "advisory": False,
            "canonical_support": _canonical_support(source_kind),
        },
    )


def _advisory_descriptor(
    *,
    namespace: str,
    logical_name: str,
    source_kind: SourceKind,
    opted_in: bool,
    sensitivity: Sensitivity,
    allow_model_egress: bool,
    allow_private_model_egress: bool,
) -> SourceDescriptor:
    source_identity = _stable_digest("advisory-source", namespace, source_kind.value)
    partition_identity = _stable_digest(
        "advisory-partition", namespace, source_kind.value, logical_name
    )
    return SourceDescriptor(
        source_id=f"src_{source_kind.value}_{source_identity[:20]}",
        partition_id=f"part_{source_kind.value}_{partition_identity[:20]}",
        source_kind=source_kind,
        source_fingerprint=source_identity,
        partition_fingerprint=partition_identity,
        display_name=f"{source_kind.value} persisted advisory",
        trust_level="advisory",
        sensitivity=sensitivity,
        opted_in=opted_in,
        model_egress_allowed=allow_model_egress,
        allow_private_model_egress=allow_private_model_egress,
        advisory=True,
        metadata={
            "adapter_parser": _PARSER_VERSION,
            "advisory": True,
            "canonical_support": False,
            "persisted_summary_only": True,
        },
    )


def _canonical_support(source_kind: SourceKind) -> str | bool:
    if source_kind is SourceKind.CODEX_TASK:
        return "candidate_only"
    if source_kind in {
        SourceKind.ASSISTANT_FINAL,
        SourceKind.TOOL_RESULT,
        SourceKind.DREAMING_HANDOFF,
    }:
        return "project_state_only"
    return False


def _sanitize_event_content(content: str) -> tuple[str, Sensitivity, int]:
    redaction = redact_secrets(content)
    if redaction.redaction_count:
        return REDACTED_SECRET, Sensitivity.SECRET, redaction.redaction_count
    return redaction.text, Sensitivity.NORMAL, 0


def _format_handoff(handoff: object) -> str:
    fields = (
        "workspace",
        "state",
        "completed",
        "verified",
        "leo_corrections",
        "pending",
    )
    lines = ["[DREAMING_HANDOFF]"]
    lines.extend(f"{name}: {getattr(handoff, name)}" for name in fields)
    lines.append("[/DREAMING_HANDOFF]")
    return "\n".join(lines)


def _read_git_head(workspace: Path) -> str | None:
    head = workspace / ".git" / "HEAD"
    if head.is_symlink() or not head.is_file():
        return None
    try:
        if head.stat().st_size > 4_096:
            return None
        value = head.read_text(encoding="utf-8").strip()
    except OSError, UnicodeDecodeError:
        return None
    redacted = redact_secrets(value).text
    prefix = "ref: refs/heads/"
    return redacted[len(prefix) :] if redacted.startswith(prefix) else redacted[:64]


def _sanitize_scalar_mapping(values: Mapping[str, JsonScalar]) -> dict[str, JsonScalar]:
    sanitized: dict[str, JsonScalar] = {}
    for index, key in enumerate(sorted(values)):
        safe_key_result = redact_secrets(key)
        safe_key = safe_key_result.text
        if safe_key_result.redaction_count or not safe_key:
            safe_key = f"redacted_key_{index}"
        value = values[key]
        sanitized[safe_key] = redact_secrets(value).text if isinstance(value, str) else value
    return sanitized


def _operational_snapshot(
    kind: OperationalSnapshotKind,
    *,
    subject_id: str,
    captured_at: datetime,
    status: str,
    payload: Mapping[str, JsonScalar],
) -> OperationalSnapshotInput:
    safe_payload = _sanitize_scalar_mapping(payload)
    snapshot_id = (
        "ops_"
        + _stable_digest(
            kind.value,
            subject_id,
            captured_at.isoformat(),
            status,
            safe_payload,
        )[:32]
    )
    return OperationalSnapshotInput(
        snapshot_id=snapshot_id,
        snapshot_kind=kind,
        subject_id=subject_id,
        captured_at=captured_at,
        status=status,
        payload=safe_payload,
    )


def _opaque_subject(*parts: object) -> str:
    return "subject_" + _stable_digest(*parts)[:24]


def _stable_digest(*parts: object) -> str:
    encoded = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _require_aware(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")

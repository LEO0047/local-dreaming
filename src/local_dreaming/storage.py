"""Repository-style storage services for Local-Dreaming.

This module deliberately depends only on the Python standard library.  Model,
CLI, and MCP adapters can use these services without leaking their own request
objects into canonical storage.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import threading
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .database import (
    DatabaseError,
    bump_memory_revision,
    canonical_json,
    check_integrity,
    connect_memory,
    connect_operations,
    get_memory_revision,
    initialize_memory,
    initialize_operations,
    transaction,
    utc_now,
)
from .episode_identity import episode_id_for, event_sequence_fingerprint
from .models import ClaimHead
from .redaction import has_secret_material
from .review import fingerprint_head_set

_MAINTENANCE_LOCKS_GUARD = threading.Lock()
_MAINTENANCE_LOCKS: dict[Path, threading.RLock] = {}
_MAINTENANCE_LOCAL = threading.local()
_SENSITIVITIES = frozenset({"normal", "private", "secret"})


def memory_maintenance_lock_path(database_path: str | Path) -> Path:
    """Return the stable cross-process lock path for one canonical memory DB."""

    path = Path(database_path).expanduser().resolve()
    return path.parent / f".{path.name}.maintenance.lock"


def operations_maintenance_lock_path(database_path: str | Path) -> Path:
    """Return the stable cross-process lock path for one operations DB."""

    path = Path(database_path).expanduser().resolve()
    return path.parent / f".{path.name}.maintenance.lock"


def _thread_lock_for(path: Path) -> threading.RLock:
    with _MAINTENANCE_LOCKS_GUARD:
        return _MAINTENANCE_LOCKS.setdefault(path, threading.RLock())


@contextmanager
def _maintenance_lock(lock_path: Path, *, target: str) -> Iterator[None]:
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(lock_path.parent, 0o700)
    thread_lock = _thread_lock_for(lock_path)
    with thread_lock:
        held = getattr(_MAINTENANCE_LOCAL, "held", None)
        if held is None:
            held = {}
            _MAINTENANCE_LOCAL.held = held
        state = held.get(lock_path)
        if state is not None:
            descriptor, depth = state
            held[lock_path] = (descriptor, depth + 1)
            try:
                yield
            finally:
                held[lock_path] = (descriptor, depth)
            return

        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError(f"{target} maintenance lock target is not a regular file")
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            held[lock_path] = (descriptor, 1)
            try:
                yield
            finally:
                held.pop(lock_path, None)
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


@contextmanager
def memory_maintenance_lock(database_path: str | Path) -> Iterator[None]:
    """Serialize canonical writers across threads and local processes.

    The lock is re-entrant within one thread so a higher-level workflow can
    cover multiple repository operations without deadlocking.  The SQLite
    transaction remains the atomic data boundary; this file lock extends the
    ordering boundary to the append-only forget ledger.
    """

    with _maintenance_lock(memory_maintenance_lock_path(database_path), target="memory"):
        yield


@contextmanager
def operations_maintenance_lock(database_path: str | Path) -> Iterator[None]:
    """Serialize operations writers across threads and local processes."""

    with _maintenance_lock(
        operations_maintenance_lock_path(database_path),
        target="operations",
    ):
        yield


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def fingerprint(*parts: object) -> str:
    payload = canonical_json(["" if value is None else value for value in parts])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_text(value: str) -> str:
    return " ".join(value.casefold().split())


def identity_fingerprint(subject: str, predicate: str, scope: str) -> str:
    return fingerprint(
        "claim-identity-v1",
        normalize_text(subject),
        normalize_text(predicate),
        normalize_text(scope),
    )


def value_fingerprint(value: object) -> str:
    return fingerprint("claim-value-v1", value)


def _insert_candidate_disposition(
    connection: sqlite3.Connection,
    *,
    candidate_id: str,
    disposition: str,
    reason_code: str,
) -> None:
    if disposition not in {"eligible", "proposed", "suppressed"}:
        raise ValueError("unsupported candidate disposition")
    if (
        connection.execute(
            "SELECT 1 FROM candidate_claims WHERE candidate_id = ?", (candidate_id,)
        ).fetchone()
        is None
    ):
        raise KeyError(f"unknown candidate: {candidate_id}")
    connection.execute(
        """
        INSERT INTO candidate_disposition_events(
            disposition_event_id, candidate_id, disposition, reason_code, recorded_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (new_id("canddisp"), candidate_id, disposition, reason_code, utc_now()),
    )


def _validate_source_policy(
    *, sensitivity: str, model_egress_allowed: bool, policy_version: str
) -> None:
    if sensitivity not in _SENSITIVITIES:
        raise ValueError(f"unsupported sensitivity: {sensitivity}")
    if not policy_version.strip():
        raise ValueError("policy_version must not be empty")
    if sensitivity == "secret" and model_egress_allowed:
        raise ValueError("secret sources cannot allow model egress")


def _validate_partition_policy(*, sensitivity: str) -> None:
    if sensitivity not in _SENSITIVITIES:
        raise ValueError(f"unsupported sensitivity: {sensitivity}")


def _source_policy_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    existing_json: str | None,
    sensitivity: str,
    model_egress_allowed: bool,
) -> str:
    if metadata is not None:
        values = dict(metadata)
    elif existing_json is not None:
        decoded = json.loads(existing_json)
        if not isinstance(decoded, Mapping):
            raise DatabaseError("source policy metadata must be a JSON object")
        values = dict(decoded)
    else:
        values = {}
    if not model_egress_allowed or sensitivity != "private":
        values["allow_private_model_egress"] = False
    return canonical_json(values)


def _to_mapping(value: object) -> dict[str, Any]:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    names = getattr(value, "__dataclass_fields__", None)
    if names:
        return {name: getattr(value, name) for name in names}
    raise TypeError("expected a dataclass or mapping")


def _observation_timestamp(value: object, *, field_name: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    else:
        raise TypeError(f"{field_name} must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed.isoformat()


def _opaque_fingerprint(value: object, *, field_name: str) -> str:
    encoded = str(value)
    if len(encoded) != 64 or any(character not in "0123456789abcdef" for character in encoded):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 fingerprint")
    return encoded


@dataclass(frozen=True, slots=True)
class EventInput:
    source_id: str
    event_type: str
    content_text: str | None
    content_fingerprint: str
    parser_version: str
    redactor_version: str
    partition_id: str | None = None
    external_event_id: str | None = None
    source_locator: str | None = None
    sensitivity: str = "normal"
    occurred_at: str | None = None
    captured_at: str | None = None
    metadata: Mapping[str, Any] | None = None
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class CollapsedObservationInput:
    source_id: str
    partition_id: str
    representative_external_event_id: str | None
    representative_occurred_at: str
    source_lineage_fingerprint: str
    evidence_family_fingerprint: str
    observation_fingerprint: str
    observed_at: str
    parser_version: str


@dataclass(frozen=True, slots=True)
class EventObservationAudit:
    event_id: str
    collapsed_observation_count: int
    total_observation_count: int
    collapsed_provenance_fingerprint: str


@dataclass(frozen=True, slots=True)
class EpisodeInput:
    source_id: str
    episode_type: str
    content_text: str
    content_fingerprint: str
    segmenter_version: str
    segmentation_reason: str
    event_ids: Sequence[str]
    partition_id: str | None = None
    parent_episode_id: str | None = None
    title: str | None = None
    sensitivity: str = "normal"
    occurred_from: str | None = None
    occurred_to: str | None = None
    episode_id: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceInput:
    event_id: str
    evidence_type: str
    evidence_fingerprint: str
    bounded_excerpt: str | None = None
    source_locator: str | None = None
    captured_at: str | None = None
    parser_version: str = "unknown"
    redactor_version: str = "unknown"
    evidence_id: str | None = None


@dataclass(frozen=True, slots=True)
class ClaimVersionInput:
    subject_text: str
    predicate: str
    scope: str
    value: object
    summary: str
    status: str = "approved"
    subject_entity_id: str | None = None
    claim_id: str | None = None
    claim_version_id: str | None = None
    valid_from: str | None = None
    valid_to: str | None = None
    confidence: float = 1.0
    epistemic_status: str = "user_confirmed"
    sensitivity: str = "normal"
    provenance_kind: str = "manual"
    mcp_safe_summary: str | None = None
    supersedes: Sequence[str] = ()
    contradicts: Sequence[str] = ()
    narrows: Sequence[str] = ()
    evidence: Sequence[EvidenceInput] = ()


@dataclass(frozen=True, slots=True)
class ReviewProposalInput:
    proposal_type: str
    target_slot_fingerprint: str
    expected_head_set_hash: str
    precondition_hash: str
    proposal_payload_fingerprint: str
    evidence_set_fingerprint: str
    payload: object
    candidate_id: str | None = None
    candidate_ids: Sequence[str] = ()
    target_claim_id: str | None = None
    proposal_id: str | None = None


class MemoryStore:
    """Canonical memory repository with short, explicit transactions."""

    def __init__(self, path: str | Path, *, initialize: bool = True) -> None:
        self.path = Path(path).expanduser().resolve()
        if initialize:
            with self.maintenance_lock():
                initialize_memory(self.path)

    @contextmanager
    def maintenance_lock(self) -> Iterator[None]:
        """Hold the canonical writer lock across one or more operations."""

        with memory_maintenance_lock(self.path):
            yield

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = connect_memory(self.path)
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with (
            self.maintenance_lock(),
            self.connection() as connection,
            transaction(connection),
        ):
            yield connection

    def current_revision(self) -> int:
        with self.connection() as connection:
            return get_memory_revision(connection)

    def create_source(
        self,
        *,
        source_type: str,
        source_fingerprint: str,
        trust_level: str = "conversation",
        sensitivity: str = "normal",
        model_egress_allowed: bool = False,
        display_name: str | None = None,
        policy_version: str = "v1",
        metadata: Mapping[str, Any] | None = None,
        source_id: str | None = None,
    ) -> str:
        _validate_source_policy(
            sensitivity=sensitivity,
            model_egress_allowed=model_egress_allowed,
            policy_version=policy_version,
        )
        requested_source_id = source_id
        source_id = source_id or new_id("src")
        with self.transaction() as connection:
            existing = connection.execute(
                """
                SELECT source_id, source_type, metadata_json FROM sources
                WHERE source_fingerprint = ?
                """,
                (source_fingerprint,),
            ).fetchone()
            if existing is not None:
                if requested_source_id is not None and existing["source_id"] != requested_source_id:
                    raise DatabaseError(
                        "source fingerprint belongs to a different explicit source_id"
                    )
                if existing["source_type"] != source_type:
                    raise DatabaseError("source fingerprint belongs to a different source_type")
            metadata_json = _source_policy_metadata(
                metadata,
                existing_json=None if existing is None else str(existing["metadata_json"]),
                sensitivity=sensitivity,
                model_egress_allowed=model_egress_allowed,
            )
            connection.execute(
                """
                INSERT INTO sources(
                    source_id, source_type, display_name, trust_level, sensitivity,
                    model_egress_allowed, source_fingerprint, policy_version,
                    metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_fingerprint) DO UPDATE SET
                    sensitivity = excluded.sensitivity,
                    model_egress_allowed = excluded.model_egress_allowed,
                    policy_version = excluded.policy_version,
                    metadata_json = excluded.metadata_json
                """,
                (
                    source_id,
                    source_type,
                    display_name,
                    trust_level,
                    sensitivity,
                    int(model_egress_allowed),
                    source_fingerprint,
                    policy_version,
                    metadata_json,
                    utc_now(),
                ),
            )
            row = connection.execute(
                "SELECT source_id, source_type FROM sources WHERE source_fingerprint = ?",
                (source_fingerprint,),
            ).fetchone()
            assert row is not None
            return str(row[0])

    def set_source_policy(
        self,
        source_id: str,
        *,
        sensitivity: str,
        model_egress_allowed: bool,
        policy_version: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        """Replace mutable source policy fields without changing memory revision."""

        _validate_source_policy(
            sensitivity=sensitivity,
            model_egress_allowed=model_egress_allowed,
            policy_version=policy_version,
        )
        with self.transaction() as connection:
            row = connection.execute(
                """
                SELECT sensitivity, model_egress_allowed, policy_version, metadata_json
                FROM sources WHERE source_id = ?
                """,
                (source_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown source: {source_id}")
            metadata_json = _source_policy_metadata(
                metadata,
                existing_json=str(row["metadata_json"]),
                sensitivity=sensitivity,
                model_egress_allowed=model_egress_allowed,
            )
            changed = (
                str(row["sensitivity"]) != sensitivity
                or bool(row["model_egress_allowed"]) is not model_egress_allowed
                or str(row["policy_version"]) != policy_version
                or str(row["metadata_json"]) != metadata_json
            )
            if not changed:
                return False
            connection.execute(
                """
                UPDATE sources
                SET sensitivity = ?, model_egress_allowed = ?, policy_version = ?,
                    metadata_json = ?
                WHERE source_id = ?
                """,
                (
                    sensitivity,
                    int(model_egress_allowed),
                    policy_version,
                    metadata_json,
                    source_id,
                ),
            )
            return True

    def create_partition(
        self,
        *,
        source_id: str,
        partition_fingerprint: str,
        external_partition_id: str | None = None,
        display_name: str | None = None,
        opted_in: bool = True,
        sensitivity: str = "normal",
        metadata: Mapping[str, Any] | None = None,
        partition_id: str | None = None,
    ) -> str:
        _validate_partition_policy(sensitivity=sensitivity)
        requested_partition_id = partition_id
        partition_id = partition_id or new_id("part")
        with self.transaction() as connection:
            existing = connection.execute(
                """
                SELECT partition_id, source_id, metadata_json FROM source_partitions
                WHERE partition_fingerprint = ?
                """,
                (partition_fingerprint,),
            ).fetchone()
            if existing is not None:
                if (
                    requested_partition_id is not None
                    and existing["partition_id"] != requested_partition_id
                ):
                    raise DatabaseError(
                        "partition fingerprint belongs to a different explicit partition_id"
                    )
                if existing["source_id"] != source_id:
                    raise DatabaseError("partition fingerprint belongs to a different source")
            metadata_json = (
                canonical_json(metadata)
                if metadata is not None
                else (
                    str(existing["metadata_json"]) if existing is not None else canonical_json({})
                )
            )
            connection.execute(
                """
                INSERT INTO source_partitions(
                    partition_id, source_id, external_partition_id, display_name,
                    opted_in, sensitivity, partition_fingerprint, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(partition_fingerprint) DO UPDATE SET
                    opted_in = excluded.opted_in,
                    sensitivity = excluded.sensitivity,
                    metadata_json = excluded.metadata_json
                """,
                (
                    partition_id,
                    source_id,
                    external_partition_id,
                    display_name,
                    int(opted_in),
                    sensitivity,
                    partition_fingerprint,
                    metadata_json,
                    utc_now(),
                ),
            )
            row = connection.execute(
                """
                SELECT partition_id, source_id FROM source_partitions
                WHERE partition_fingerprint = ?
                """,
                (partition_fingerprint,),
            ).fetchone()
            assert row is not None
            return str(row[0])

    def set_partition_policy(
        self,
        partition_id: str,
        *,
        opted_in: bool,
        sensitivity: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        """Replace mutable partition policy fields without changing memory revision."""

        _validate_partition_policy(sensitivity=sensitivity)
        with self.transaction() as connection:
            row = connection.execute(
                """
                SELECT opted_in, sensitivity, metadata_json
                FROM source_partitions WHERE partition_id = ?
                """,
                (partition_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown partition: {partition_id}")
            metadata_json = (
                str(row["metadata_json"]) if metadata is None else canonical_json(metadata)
            )
            changed = (
                bool(row["opted_in"]) is not opted_in
                or str(row["sensitivity"]) != sensitivity
                or str(row["metadata_json"]) != metadata_json
            )
            if not changed:
                return False
            connection.execute(
                """
                UPDATE source_partitions
                SET opted_in = ?, sensitivity = ?, metadata_json = ?
                WHERE partition_id = ?
                """,
                (int(opted_in), sensitivity, metadata_json, partition_id),
            )
            return True

    def create_event(self, event: EventInput | Mapping[str, Any] | object) -> str:
        """Insert an immutable redacted event, returning its stable ID.

        The method intentionally accepts dataclass-shaped objects so ingest can
        define its own ``PreparedEvent`` without coupling that type to storage.
        """

        data = _to_mapping(event)
        event_id = data.get("event_id") or new_id("evt")
        captured_at = data.get("captured_at") or utc_now()
        sensitivity = data.get("sensitivity", "normal")
        content_text = data.get("content_text")
        if (
            sensitivity == "secret"
            and content_text is not None
            and "[REDACTED_SECRET]" not in content_text
        ):
            raise ValueError("secret event context must contain [REDACTED_SECRET]")
        metadata = data.get("metadata") or data.get("metadata_json") or {}
        metadata_json = metadata if isinstance(metadata, str) else canonical_json(metadata)

        with self.transaction() as connection:
            partition_id = data.get("partition_id")
            if partition_id is not None:
                partition = connection.execute(
                    "SELECT source_id FROM source_partitions WHERE partition_id = ?",
                    (partition_id,),
                ).fetchone()
                if partition is None or partition[0] != data["source_id"]:
                    raise ValueError("partition does not belong to event source")
            connection.execute(
                """
                INSERT INTO events(
                    event_id, source_id, partition_id, external_event_id, event_type,
                    content_text, content_fingerprint, source_locator, sensitivity,
                    occurred_at, captured_at, parser_version, redactor_version,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    event_id,
                    data["source_id"],
                    partition_id,
                    data.get("external_event_id"),
                    data["event_type"],
                    content_text,
                    data["content_fingerprint"],
                    data.get("source_locator"),
                    sensitivity,
                    data.get("occurred_at"),
                    captured_at,
                    data["parser_version"],
                    data["redactor_version"],
                    metadata_json,
                ),
            )
            if data.get("external_event_id") is not None:
                row = connection.execute(
                    "SELECT event_id FROM events WHERE source_id = ? AND external_event_id = ?",
                    (data["source_id"], data["external_event_id"]),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT event_id FROM events
                    WHERE source_id = ? AND content_fingerprint = ?
                      AND occurred_at IS ?
                    """,
                    (data["source_id"], data["content_fingerprint"], data.get("occurred_at")),
                ).fetchone()
            if row is None:
                raise DatabaseError("event insert conflicted with a different stable identity")
            return str(row[0])

    def record_collapsed_observation(
        self,
        observation: CollapsedObservationInput | Mapping[str, Any] | object,
    ) -> str:
        """Append one opaque duplicate observation linked to an immutable event.

        Replaying the same source record returns the same durable identity.  A
        pre-v4 cursor may omit the representative external ID; in that case the
        frozen source/partition/occurrence/lineage tuple resolves the event.
        """

        data = _to_mapping(observation)
        source_id = str(data["source_id"])
        partition_id = str(data["partition_id"])
        external_raw = data.get("representative_external_event_id")
        representative_external_event_id = None if external_raw is None else str(external_raw)
        representative_occurred_at = _observation_timestamp(
            data["representative_occurred_at"],
            field_name="representative_occurred_at",
        )
        observed_at = _observation_timestamp(data["observed_at"], field_name="observed_at")
        lineage = _opaque_fingerprint(
            data["source_lineage_fingerprint"],
            field_name="source_lineage_fingerprint",
        )
        family = _opaque_fingerprint(
            data["evidence_family_fingerprint"],
            field_name="evidence_family_fingerprint",
        )
        observation_fingerprint = _opaque_fingerprint(
            data["observation_fingerprint"],
            field_name="observation_fingerprint",
        )
        parser_version = str(data["parser_version"])
        if not parser_version or len(parser_version) > 128:
            raise ValueError("parser_version must contain 1-128 characters")
        if representative_external_event_id is not None and (
            not representative_external_event_id or len(representative_external_event_id) > 256
        ):
            raise ValueError("representative_external_event_id must contain 1-256 characters")

        observation_id = (
            "colobs_"
            + fingerprint(
                "collapsed-observation-v1",
                source_id,
                partition_id,
                observation_fingerprint,
            )[:32]
        )
        with self.transaction() as connection:
            if representative_external_event_id is not None:
                candidates = connection.execute(
                    """
                    SELECT event_id, source_id, partition_id, metadata_json
                    FROM events
                    WHERE source_id = ? AND external_event_id = ?
                    """,
                    (source_id, representative_external_event_id),
                ).fetchall()
            else:
                candidates = connection.execute(
                    """
                    SELECT event_id, source_id, partition_id, metadata_json
                    FROM events
                    WHERE source_id = ? AND partition_id = ? AND occurred_at = ?
                    ORDER BY event_id
                    """,
                    (source_id, partition_id, representative_occurred_at),
                ).fetchall()
            matching = []
            for row in candidates:
                metadata = json.loads(str(row["metadata_json"]))
                if (
                    row["source_id"] == source_id
                    and row["partition_id"] == partition_id
                    and metadata.get("source_lineage_fingerprint") == lineage
                ):
                    matching.append(row)
            if len(matching) != 1:
                raise DatabaseError("collapsed observation representative did not resolve uniquely")
            event_id = str(matching[0]["event_id"])
            connection.execute(
                """
                INSERT INTO event_collapsed_observations(
                    collapsed_observation_id, event_id, source_id, partition_id,
                    source_lineage_fingerprint, evidence_family_fingerprint,
                    observation_fingerprint, observed_at, parser_version, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, partition_id, observation_fingerprint) DO NOTHING
                """,
                (
                    observation_id,
                    event_id,
                    source_id,
                    partition_id,
                    lineage,
                    family,
                    observation_fingerprint,
                    observed_at,
                    parser_version,
                    utc_now(),
                ),
            )
            persisted = connection.execute(
                """
                SELECT collapsed_observation_id, event_id,
                       source_lineage_fingerprint, evidence_family_fingerprint
                FROM event_collapsed_observations
                WHERE source_id = ? AND partition_id = ?
                  AND observation_fingerprint = ?
                """,
                (source_id, partition_id, observation_fingerprint),
            ).fetchone()
            if persisted is None:
                raise DatabaseError("collapsed observation insert did not persist")
            if (
                persisted["event_id"] != event_id
                or persisted["source_lineage_fingerprint"] != lineage
                or persisted["evidence_family_fingerprint"] != family
            ):
                raise DatabaseError(
                    "collapsed observation identity belongs to different provenance"
                )
            return str(persisted["collapsed_observation_id"])

    def get_event_observation_audit(self, event_id: str) -> EventObservationAudit:
        """Return a deterministic aggregate without mutating event metadata."""

        with self.connection() as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                is None
            ):
                raise KeyError(f"unknown event: {event_id}")
            rows = connection.execute(
                """
                SELECT observation_fingerprint
                FROM event_collapsed_observations
                WHERE event_id = ?
                ORDER BY observation_fingerprint
                """,
                (event_id,),
            ).fetchall()
        observation_fingerprints = tuple(str(row["observation_fingerprint"]) for row in rows)
        return EventObservationAudit(
            event_id=event_id,
            collapsed_observation_count=len(observation_fingerprints),
            total_observation_count=1 + len(observation_fingerprints),
            collapsed_provenance_fingerprint=fingerprint(
                "event-observation-audit-v1",
                event_id,
                observation_fingerprints,
            ),
        )

    def create_episode(self, episode: EpisodeInput | Mapping[str, Any] | object) -> str:
        data = _to_mapping(episode)
        event_ids = tuple(str(value) for value in data.pop("event_ids", ()))
        partition_raw = data.get("partition_id")
        if partition_raw is None or not str(partition_raw).strip():
            raise ValueError("episode identity requires partition_id")
        partition_id = str(partition_raw)
        expected_episode_id = episode_id_for(
            source_id=str(data["source_id"]),
            partition_id=partition_id,
            event_ids=event_ids,
            segmenter_version=str(data["segmenter_version"]),
        )
        proposed_episode_id = data.get("episode_id")
        if proposed_episode_id is not None and str(proposed_episode_id) != expected_episode_id:
            raise DatabaseError("episode_id does not match the frozen logical identity")
        episode_id = expected_episode_id
        sequence_fingerprint = event_sequence_fingerprint(event_ids)
        with self.transaction() as connection:
            for event_id in event_ids:
                event_row = connection.execute(
                    "SELECT source_id FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                if event_row is None or event_row[0] != data["source_id"]:
                    raise ValueError("episode event does not belong to episode source")
            inserted = connection.execute(
                """
                INSERT INTO episodes(
                    episode_id, source_id, partition_id, parent_episode_id,
                    episode_type, title, content_text, content_fingerprint,
                    event_sequence_fingerprint, segmenter_version,
                    segmentation_reason, sensitivity,
                    occurred_from, occurred_to, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    source_id, partition_id, event_sequence_fingerprint,
                    segmenter_version
                ) DO NOTHING
                """,
                (
                    episode_id,
                    data["source_id"],
                    partition_id,
                    data.get("parent_episode_id"),
                    data["episode_type"],
                    data.get("title"),
                    data["content_text"],
                    data["content_fingerprint"],
                    sequence_fingerprint,
                    data["segmenter_version"],
                    data["segmentation_reason"],
                    data.get("sensitivity", "normal"),
                    data.get("occurred_from"),
                    data.get("occurred_to"),
                    utc_now(),
                ),
            )
            row = connection.execute(
                """
                SELECT * FROM episodes
                WHERE source_id = ? AND partition_id = ?
                  AND event_sequence_fingerprint = ? AND segmenter_version = ?
                """,
                (
                    data["source_id"],
                    partition_id,
                    sequence_fingerprint,
                    data["segmenter_version"],
                ),
            ).fetchone()
            if row is None:
                raise DatabaseError("episode insert did not resolve its logical identity")
            stable_id = str(row["episode_id"])
            if stable_id != episode_id:
                raise DatabaseError("episode identity resolved to an unexpected stable ID")
            immutable_payload = {
                "content_fingerprint": str(row["content_fingerprint"]),
                "content_text": str(row["content_text"]),
                "occurred_from": row["occurred_from"],
                "occurred_to": row["occurred_to"],
                "partition_id": str(row["partition_id"]),
            }
            expected_payload = {
                "content_fingerprint": str(data["content_fingerprint"]),
                "content_text": str(data["content_text"]),
                "occurred_from": data.get("occurred_from"),
                "occurred_to": data.get("occurred_to"),
                "partition_id": partition_id,
            }
            if immutable_payload != expected_payload:
                raise DatabaseError("episode replay changed immutable episode content")
            if inserted.rowcount == 1:
                for ordinal, event_id in enumerate(event_ids):
                    connection.execute(
                        """
                        INSERT INTO episode_events(episode_id, event_id, ordinal)
                        VALUES (?, ?, ?)
                        """,
                        (stable_id, event_id, ordinal),
                    )
            else:
                persisted_event_ids = tuple(
                    str(item["event_id"])
                    for item in connection.execute(
                        """
                        SELECT event_id FROM episode_events
                        WHERE episode_id = ? ORDER BY ordinal
                        """,
                        (stable_id,),
                    ).fetchall()
                )
                if persisted_event_ids != event_ids:
                    raise DatabaseError("episode replay changed the ordered event sequence")
            return stable_id

    def create_candidate(
        self,
        *,
        episode_id: str,
        subject_text: str,
        predicate: str,
        scope: str,
        value: object,
        proposal_type: str,
        confidence: float,
        extraction_fingerprint: str,
        extractor_version: str,
        prompt_hash: str,
        schema_version: str,
        model_id: str,
        reasoning_effort: str,
        subject_entity_id: str | None = None,
        sensitivity: str = "normal",
        candidate_id: str | None = None,
        evidence_event_ids: Sequence[str] = (),
        epistemic_status: str = "provisional",
        valid_from: str | None = None,
        valid_to: str | None = None,
        possible_supersedes_claim_id: str | None = None,
        initial_disposition: str = "eligible",
        initial_reason_code: str = "phase1_candidate_created",
    ) -> str:
        candidate_id = candidate_id or new_id("cand")
        identity = identity_fingerprint(subject_text, predicate, scope)
        value_fp = value_fingerprint(value)
        with self.transaction() as connection:
            inserted = connection.execute(
                """
                INSERT INTO candidate_claims(
                    candidate_id, episode_id, subject_entity_id, subject_text,
                    predicate, scope, value_json, normalized_identity_fingerprint,
                    value_fingerprint, proposal_type, epistemic_status,
                    valid_from, valid_to, possible_supersedes_claim_id,
                    confidence, sensitivity,
                    extraction_fingerprint, extractor_version, prompt_hash,
                    schema_version, model_id, reasoning_effort, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(extraction_fingerprint) DO NOTHING
                """,
                (
                    candidate_id,
                    episode_id,
                    subject_entity_id,
                    subject_text,
                    predicate,
                    scope,
                    canonical_json(value),
                    identity,
                    value_fp,
                    proposal_type,
                    epistemic_status,
                    valid_from,
                    valid_to,
                    possible_supersedes_claim_id,
                    confidence,
                    sensitivity,
                    extraction_fingerprint,
                    extractor_version,
                    prompt_hash,
                    schema_version,
                    model_id,
                    reasoning_effort,
                    utc_now(),
                ),
            )
            row = connection.execute(
                "SELECT candidate_id FROM candidate_claims WHERE extraction_fingerprint = ?",
                (extraction_fingerprint,),
            ).fetchone()
            assert row is not None
            stable_id = str(row[0])
            if inserted.rowcount == 1:
                _insert_candidate_disposition(
                    connection,
                    candidate_id=stable_id,
                    disposition=initial_disposition,
                    reason_code=initial_reason_code,
                )
            for ordinal, event_id in enumerate(evidence_event_ids):
                evidence = connection.execute(
                    """
                    SELECT 1 FROM episode_events
                    WHERE episode_id = ? AND event_id = ?
                    """,
                    (episode_id, event_id),
                ).fetchone()
                if evidence is None:
                    raise ValueError("candidate evidence is not part of its episode")
                connection.execute(
                    """
                    INSERT INTO candidate_evidence(candidate_id, event_id, ordinal)
                    VALUES (?, ?, ?)
                    ON CONFLICT(candidate_id, event_id) DO NOTHING
                    """,
                    (stable_id, event_id, ordinal),
                )
            return stable_id

    def set_candidate_disposition(
        self,
        candidate_id: str,
        *,
        disposition: str,
        reason_code: str,
    ) -> None:
        with self.transaction() as connection:
            _insert_candidate_disposition(
                connection,
                candidate_id=candidate_id,
                disposition=disposition,
                reason_code=reason_code,
            )

    def disposition_candidates(
        self,
        candidate_ids: Sequence[str],
        *,
        proposed_candidate_ids: Sequence[str] = (),
        reason_code: str = "phase2_evaluated",
        suppressed_reason_codes: Mapping[str, str] | None = None,
    ) -> None:
        evaluated = tuple(dict.fromkeys(candidate_ids))
        proposed = set(proposed_candidate_ids)
        if not proposed.issubset(evaluated):
            raise ValueError("proposed candidates must be part of the evaluated set")
        reason_codes = dict(suppressed_reason_codes or {})
        if not set(reason_codes).issubset(set(evaluated) - proposed):
            raise ValueError("suppression reasons must target suppressed evaluated candidates")
        with self.transaction() as connection:
            for candidate_id in evaluated:
                _insert_candidate_disposition(
                    connection,
                    candidate_id=candidate_id,
                    disposition="proposed" if candidate_id in proposed else "suppressed",
                    reason_code=(
                        "phase2_proposed"
                        if candidate_id in proposed
                        else reason_codes.get(candidate_id, reason_code)
                    ),
                )

    def create_entity(
        self,
        *,
        entity_type: str,
        canonical_name: str,
        entity_id: str | None = None,
    ) -> str:
        entity_id = entity_id or new_id("ent")
        normalized = normalize_text(canonical_name)
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO entities(
                    entity_id, entity_type, canonical_name, normalized_name, created_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(entity_type, normalized_name) DO NOTHING
                """,
                (entity_id, entity_type, canonical_name, normalized, utc_now()),
            )
            row = connection.execute(
                "SELECT entity_id FROM entities WHERE entity_type = ? AND normalized_name = ?",
                (entity_type, normalized),
            ).fetchone()
            assert row is not None
            return str(row[0])

    def add_claim_version(
        self,
        claim: ClaimVersionInput,
        *,
        actor: str = "leo",
        reason: str = "claim approved",
    ) -> tuple[str, str, int]:
        """Append a canonical claim version and update FTS in one revision."""

        canonical_value = canonical_json(claim.value)
        if (
            claim.sensitivity == "secret"
            or has_secret_material(claim.value)
            or has_secret_material(claim.summary)
            or has_secret_material(claim.mcp_safe_summary)
        ):
            raise ValueError("secret content cannot become a canonical claim")
        identity = identity_fingerprint(claim.subject_text, claim.predicate, claim.scope)
        value_fp = value_fingerprint(claim.value)
        with self.transaction() as connection:
            revision = bump_memory_revision(
                connection,
                actor=actor,
                reason=reason,
                details={"identity_fingerprint": identity},
            )
            existing = connection.execute(
                "SELECT claim_id FROM claims WHERE identity_fingerprint = ?", (identity,)
            ).fetchone()
            claim_id = str(existing[0]) if existing else (claim.claim_id or new_id("claim"))
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO claims(
                        claim_id, subject_entity_id, subject_text, predicate, scope,
                        identity_fingerprint, created_revision, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        claim_id,
                        claim.subject_entity_id,
                        claim.subject_text,
                        claim.predicate,
                        claim.scope,
                        identity,
                        revision,
                        utc_now(),
                    ),
                )
            version_number = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(version_number), 0) + 1
                    FROM claim_versions WHERE claim_id = ?
                    """,
                    (claim_id,),
                ).fetchone()[0]
            )
            version_id = claim.claim_version_id or new_id("cv")
            recorded_at = utc_now()
            connection.execute(
                """
                INSERT INTO claim_versions(
                    claim_version_id, claim_id, version_number, value_json,
                    value_fingerprint, summary, mcp_safe_summary, status,
                    valid_from, valid_to, recorded_revision, recorded_at,
                    superseded_at, confidence, epistemic_status, sensitivity,
                    provenance_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    claim_id,
                    version_number,
                    canonical_value,
                    value_fp,
                    claim.summary,
                    claim.mcp_safe_summary,
                    claim.status,
                    claim.valid_from,
                    claim.valid_to,
                    revision,
                    recorded_at,
                    claim.confidence,
                    claim.epistemic_status,
                    claim.sensitivity,
                    claim.provenance_kind,
                ),
            )
            relation_sets = (
                ("supersedes", claim.supersedes),
                ("contradicts", claim.contradicts),
                ("narrows", claim.narrows),
            )
            for relation_type, targets in relation_sets:
                for target in targets:
                    connection.execute(
                        """
                        INSERT INTO claim_relations(
                            relation_id, from_claim_version_id, to_claim_version_id,
                            relation_type, created_revision, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (new_id("rel"), version_id, target, relation_type, revision, recorded_at),
                    )
            for evidence in claim.evidence:
                connection.execute(
                    """
                    INSERT INTO claim_evidence(
                        evidence_id, claim_version_id, event_id, evidence_type,
                        bounded_excerpt, source_locator, captured_at, parser_version,
                        redactor_version, evidence_fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        evidence.evidence_id or new_id("evd"),
                        version_id,
                        evidence.event_id,
                        evidence.evidence_type,
                        evidence.bounded_excerpt,
                        evidence.source_locator,
                        evidence.captured_at or recorded_at,
                        evidence.parser_version,
                        evidence.redactor_version,
                        evidence.evidence_fingerprint,
                    ),
                )
            if claim.sensitivity != "secret":
                connection.execute(
                    """
                    INSERT INTO claim_fts(
                        claim_version_id, claim_id, subject, predicate, scope, summary
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        version_id,
                        claim_id,
                        claim.subject_text,
                        claim.predicate,
                        claim.scope,
                        claim.summary,
                    ),
                )
            return claim_id, version_id, revision

    def head_version_ids(self, claim_id: str) -> list[str]:
        with self.connection() as connection:
            return [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT claim_version_id FROM current_claim_versions
                    WHERE claim_id = ? ORDER BY claim_version_id
                    """,
                    (claim_id,),
                )
            ]

    def set_claim_pin(
        self,
        claim_id: str,
        *,
        pinned: bool = True,
        actor: str = "leo",
    ) -> tuple[bool, int]:
        """Append a pin decision and return ``(changed, memory_revision)``."""

        with self.transaction() as connection:
            claim = connection.execute(
                "SELECT 1 FROM claims WHERE claim_id = ?", (claim_id,)
            ).fetchone()
            if claim is None:
                raise KeyError(f"unknown claim: {claim_id}")
            current = connection.execute(
                "SELECT 1 FROM current_claim_pins WHERE claim_id = ?", (claim_id,)
            ).fetchone()
            if (current is not None) is pinned:
                return False, get_memory_revision(connection)
            revision = bump_memory_revision(
                connection,
                actor=actor,
                reason="claim pinned" if pinned else "claim unpinned",
                details={"claim_id": claim_id},
            )
            connection.execute(
                """
                INSERT INTO claim_pin_events(
                    pin_event_id, claim_id, pinned, recorded_revision, recorded_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (new_id("pin"), claim_id, int(pinned), revision, utc_now()),
            )
            return True, revision

    def pinned_claim_ids(self) -> tuple[str, ...]:
        with self.connection() as connection:
            return tuple(
                str(row[0])
                for row in connection.execute(
                    "SELECT claim_id FROM current_claim_pins ORDER BY claim_id"
                )
            )

    def head_set_hash(self, claim_id: str) -> str:
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT claim_version_id, value_fingerprint, status, valid_from, valid_to
                FROM current_claim_versions
                WHERE claim_id = ? ORDER BY claim_version_id
                """,
                (claim_id,),
            ).fetchall()
        return fingerprint_head_set(
            ClaimHead(
                claim_version_id=str(row["claim_version_id"]),
                value_fingerprint=str(row["value_fingerprint"]),
                status=str(row["status"]),
                valid_from=row["valid_from"],
                valid_to=row["valid_to"],
            )
            for row in rows
        )

    def load_heads(self, identity: str) -> list[ClaimHead]:
        """Load immutable current heads used by review precondition checks."""

        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT cv.claim_version_id, cv.claim_id, cv.value_fingerprint,
                       cv.valid_from, cv.valid_to, cv.recorded_revision, cv.status
                FROM current_claim_versions AS cv
                JOIN claims AS c ON c.claim_id = cv.claim_id
                WHERE c.identity_fingerprint = ?
                ORDER BY cv.claim_version_id
                """,
                (identity,),
            ).fetchall()
            return [
                ClaimHead(
                    claim_version_id=str(row["claim_version_id"]),
                    value_fingerprint=str(row["value_fingerprint"]),
                    status=str(row["status"]),
                    valid_from=row["valid_from"],
                    valid_to=row["valid_to"],
                )
                for row in rows
            ]

    def semantic_context_hash(self, candidate_ids: Sequence[str]) -> str:
        """Recompute the bounded cross-slot context used by Phase 2."""

        from .semantic_neighbors import load_semantic_neighbor_context

        with self.connection() as connection:
            return load_semantic_neighbor_context(connection, candidate_ids).context_hash

    def create_review_batch(
        self,
        proposals: Sequence[ReviewProposalInput] = (),
        *,
        base_memory_revision: int | None = None,
        extractor_run_id: str | None = None,
        consolidator_run_id: str | None = None,
        batch_id: str | None = None,
        evaluated_candidate_ids: Sequence[str] = (),
        suppressed_reason_codes: Mapping[str, str] | None = None,
    ) -> str:
        """Persist a pending batch and all proposals atomically."""

        slots = [proposal.target_slot_fingerprint for proposal in proposals]
        if len(set(slots)) != len(slots):
            raise ValueError("a review batch cannot modify the same target slot twice")
        batch_id = batch_id or new_id("review")
        with self.transaction() as connection:
            current_revision = get_memory_revision(connection)
            base_revision = (
                current_revision if base_memory_revision is None else base_memory_revision
            )
            if base_revision > current_revision:
                raise ValueError("review base revision cannot be in the future")
            now = utc_now()
            connection.execute(
                """
                INSERT INTO review_batches(
                    batch_id, status, base_memory_revision, extractor_run_id,
                    consolidator_run_id, created_at
                ) VALUES (?, 'pending', ?, ?, ?, ?)
                """,
                (batch_id, base_revision, extractor_run_id, consolidator_run_id, now),
            )
            for proposal in proposals:
                proposal_id = proposal.proposal_id or new_id("proposal")
                connection.execute(
                    """
                    INSERT INTO review_proposals(
                        proposal_id, batch_id, candidate_id, target_claim_id,
                        proposal_type, status, base_memory_revision,
                        target_slot_fingerprint, expected_head_set_hash,
                        precondition_hash, proposal_payload_fingerprint,
                        evidence_set_fingerprint, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        proposal_id,
                        batch_id,
                        proposal.candidate_id,
                        proposal.target_claim_id,
                        proposal.proposal_type,
                        base_revision,
                        proposal.target_slot_fingerprint,
                        proposal.expected_head_set_hash,
                        proposal.precondition_hash,
                        proposal.proposal_payload_fingerprint,
                        proposal.evidence_set_fingerprint,
                        canonical_json(proposal.payload),
                        now,
                    ),
                )
                candidate_ids = tuple(
                    dict.fromkeys(
                        (
                            *proposal.candidate_ids,
                            *((proposal.candidate_id,) if proposal.candidate_id else ()),
                        )
                    )
                )
                for ordinal, candidate_id in enumerate(candidate_ids):
                    connection.execute(
                        """
                        INSERT INTO review_proposal_candidates(
                            proposal_id, candidate_id, ordinal
                        ) VALUES (?, ?, ?)
                        """,
                        (proposal_id, candidate_id, ordinal),
                    )
            proposed_candidate_ids = {
                candidate_id
                for proposal in proposals
                for candidate_id in (
                    *proposal.candidate_ids,
                    *((proposal.candidate_id,) if proposal.candidate_id else ()),
                )
            }
            evaluated = tuple(dict.fromkeys(evaluated_candidate_ids))
            if evaluated and not proposed_candidate_ids.issubset(evaluated):
                raise ValueError("review candidates must be part of the evaluated set")
            reason_codes = dict(suppressed_reason_codes or {})
            if not set(reason_codes).issubset(set(evaluated) - proposed_candidate_ids):
                raise ValueError("suppression reasons must target suppressed evaluated candidates")
            for candidate_id in evaluated:
                _insert_candidate_disposition(
                    connection,
                    candidate_id=candidate_id,
                    disposition=(
                        "proposed" if candidate_id in proposed_candidate_ids else "suppressed"
                    ),
                    reason_code=(
                        "phase2_proposed"
                        if candidate_id in proposed_candidate_ids
                        else reason_codes.get(candidate_id, "phase2_evaluated")
                    ),
                )
        return batch_id

    def load_review_batch(self, batch_id: str) -> dict[str, Any]:
        with self.connection() as connection:
            batch = connection.execute(
                "SELECT * FROM review_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise KeyError(f"unknown review batch: {batch_id}")
            result = dict(batch)
            proposals: list[dict[str, Any]] = []
            for row in connection.execute(
                """
                SELECT * FROM review_proposals
                WHERE batch_id = ? ORDER BY created_at, proposal_id
                """,
                (batch_id,),
            ):
                proposal = dict(row)
                proposal["payload"] = json.loads(proposal.pop("payload_json"))
                proposals.append(proposal)
            result["proposals"] = proposals
            return result

    def mark_review_stale(
        self,
        batch_id: str,
        *,
        proposal_ids: Sequence[str] | None = None,
        note: str = "review precondition changed",
    ) -> int:
        """Mark all or selected pending proposals stale without changing memory."""

        with self.transaction() as connection:
            params: list[object] = [utc_now(), note, batch_id]
            selected = ""
            if proposal_ids:
                placeholders = ",".join("?" for _ in proposal_ids)
                selected = f" AND proposal_id IN ({placeholders})"
                params.extend(proposal_ids)
            cursor = connection.execute(
                f"""
                UPDATE review_proposals
                SET status = 'stale', decided_at = ?, decision_note = ?
                WHERE batch_id = ? AND status = 'pending'{selected}
                """,
                params,
            )
            remaining = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM review_proposals
                    WHERE batch_id = ? AND status = 'pending'
                    """,
                    (batch_id,),
                ).fetchone()[0]
            )
            if remaining == 0:
                connection.execute(
                    "UPDATE review_batches SET status = 'stale', decided_at = ? WHERE batch_id = ?",
                    (utc_now(), batch_id),
                )
            return cursor.rowcount

    def record_review_decisions(
        self,
        batch_id: str,
        decisions: Mapping[str, str],
        *,
        note: str | None = None,
    ) -> None:
        """Atomically record review decisions after the pure freshness guard passes.

        This records intent only.  Canonical application is a separate transaction
        so an adapter cannot accidentally equate ``approved`` with ``applied``.
        """

        allowed = {"approved", "rejected", "corrected"}
        if not decisions or any(status not in allowed for status in decisions.values()):
            raise ValueError("review decisions must be approved, rejected, or corrected")
        with self.transaction() as connection:
            batch = connection.execute(
                "SELECT status FROM review_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise KeyError(f"unknown review batch: {batch_id}")
            if batch[0] != "pending":
                raise ValueError(f"review batch is {batch[0]}, not pending")
            now = utc_now()
            for proposal_id, status in decisions.items():
                cursor = connection.execute(
                    """
                    UPDATE review_proposals
                    SET status = ?, decided_at = ?, decision_note = ?
                    WHERE proposal_id = ? AND batch_id = ? AND status = 'pending'
                    """,
                    (status, now, note, proposal_id, batch_id),
                )
                if cursor.rowcount != 1:
                    raise ValueError(f"proposal is missing or not pending: {proposal_id}")
            remaining = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM review_proposals
                    WHERE batch_id = ? AND status = 'pending'
                    """,
                    (batch_id,),
                ).fetchone()[0]
            )
            if remaining == 0:
                statuses = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT status FROM review_proposals WHERE batch_id = ?", (batch_id,)
                    )
                }
                batch_status = "approved" if statuses <= {"approved", "corrected"} else "rejected"
                connection.execute(
                    "UPDATE review_batches SET status = ?, decided_at = ? WHERE batch_id = ?",
                    (batch_status, now, batch_id),
                )

    def search(
        self,
        query: str,
        *,
        include_private: bool = True,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        sensitivity_clause = "('normal', 'private')" if include_private else "('normal')"
        with self.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT cv.claim_version_id, cv.claim_id, c.subject_text, c.predicate,
                       c.scope, cv.summary, cv.valid_from, cv.valid_to,
                       cv.recorded_revision, cv.epistemic_status, cv.sensitivity,
                       bm25(claim_fts) AS rank
                FROM claim_fts
                JOIN current_claim_versions AS cv
                  ON cv.claim_version_id = claim_fts.claim_version_id
                JOIN claims AS c ON c.claim_id = cv.claim_id
                WHERE claim_fts MATCH ?
                  AND cv.sensitivity IN {sensitivity_clause}
                ORDER BY rank, cv.recorded_revision DESC
                LIMIT ?
                """,
                (query, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def check_integrity(self) -> None:
        with self.connection() as connection:
            check_integrity(connection)


class OperationsStore:
    """Queue and telemetry repository; it has no memory revision API."""

    def __init__(self, path: str | Path, *, initialize: bool = True) -> None:
        self.path = Path(path).expanduser().resolve()
        if initialize:
            with self.maintenance_lock():
                initialize_operations(self.path)

    @contextmanager
    def maintenance_lock(self) -> Iterator[None]:
        """Hold the operations writer lock across one or more operations."""

        with operations_maintenance_lock(self.path):
            yield

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = connect_operations(self.path)
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with (
            self.maintenance_lock(),
            self.connection() as connection,
            transaction(connection),
        ):
            yield connection

    def enqueue_job(
        self,
        *,
        job_type: str,
        dedupe_key: str,
        payload: object | None = None,
        priority: int = 0,
        max_attempts: int = 5,
        available_at: str | None = None,
        job_id: str | None = None,
    ) -> str:
        job_id = job_id or new_id("job")
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO jobs(
                    job_id, job_type, dedupe_key, payload_json, status,
                    priority, attempts, max_attempts, available_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO NOTHING
                """,
                (
                    job_id,
                    job_type,
                    dedupe_key,
                    canonical_json(payload or {}),
                    priority,
                    max_attempts,
                    available_at or now,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT job_id FROM jobs WHERE dedupe_key = ?", (dedupe_key,)
            ).fetchone()
            assert row is not None
            return str(row[0])

    def lease_job(
        self,
        *,
        owner: str,
        lease_seconds: int = 300,
        job_types: Sequence[str] | None = None,
        job_ids: Sequence[str] | None = None,
    ) -> dict[str, Any] | None:
        now_dt = datetime.now(UTC).replace(microsecond=0)
        now = now_dt.isoformat().replace("+00:00", "Z")
        expires = (now_dt + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
        stable_job_ids: tuple[str, ...] | None = None
        recovery_id_clause = ""
        recovery_id_params: tuple[object, ...] = ()
        if job_ids is not None:
            stable_job_ids = tuple(dict.fromkeys(str(item) for item in job_ids))
            if not stable_job_ids:
                return None
            placeholders = ",".join("?" for _ in stable_job_ids)
            recovery_id_clause = f" AND job_id IN ({placeholders})"
            recovery_id_params = stable_job_ids
        with self.transaction() as connection:
            connection.execute(
                f"""
                UPDATE jobs
                SET status = 'failed', lease_owner = NULL, lease_expires_at = NULL,
                    last_error = COALESCE(
                        last_error, 'lease expired after final permitted attempt'
                    ),
                    updated_at = ?
                WHERE status = 'leased' AND lease_expires_at <= ?
                  AND attempts >= max_attempts
                  {recovery_id_clause}
                """,
                (now, now, *recovery_id_params),
            )
            connection.execute(
                f"""
                UPDATE jobs
                SET status = 'queued', lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = ?
                WHERE status = 'leased' AND lease_expires_at <= ?
                  AND attempts < max_attempts
                  {recovery_id_clause}
                """,
                (now, now, *recovery_id_params),
            )
            params: list[object] = [now]
            type_clause = ""
            if job_types:
                placeholders = ",".join("?" for _ in job_types)
                type_clause = f" AND job_type IN ({placeholders})"
                params.extend(job_types)
            id_clause = ""
            if stable_job_ids is not None:
                placeholders = ",".join("?" for _ in stable_job_ids)
                id_clause = f" AND job_id IN ({placeholders})"
                params.extend(stable_job_ids)
            row = connection.execute(
                f"""
                SELECT job_id FROM jobs
                WHERE status = 'queued' AND available_at <= ? AND attempts < max_attempts
                {type_clause}
                {id_clause}
                ORDER BY priority DESC, created_at, job_id
                LIMIT 1
                """,
                params,
            ).fetchone()
            if row is None:
                return None
            job_id = str(row[0])
            connection.execute(
                """
                UPDATE jobs
                SET status = 'leased', lease_owner = ?, lease_expires_at = ?,
                    attempts = attempts + 1, updated_at = ?
                WHERE job_id = ?
                """,
                (owner, expires, now, job_id),
            )
            leased = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            assert leased is not None
            result = dict(leased)
            result["payload"] = json.loads(result.pop("payload_json"))
            return result

    def complete_job(self, job_id: str, *, owner: str) -> bool:
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs SET status = 'completed', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE job_id = ? AND status = 'leased' AND lease_owner = ?
                """,
                (utc_now(), job_id, owner),
            )
            return cursor.rowcount == 1

    def fail_job(
        self,
        job_id: str,
        *,
        owner: str,
        error: str,
        retry_at: str | None = None,
    ) -> bool:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT attempts, max_attempts FROM jobs WHERE job_id = ? AND lease_owner = ?",
                (job_id, owner),
            ).fetchone()
            if row is None:
                return False
            retry = int(row[0]) < int(row[1])
            status = "queued" if retry else "failed"
            cursor = connection.execute(
                """
                UPDATE jobs SET status = ?, available_at = ?, lease_owner = NULL,
                    lease_expires_at = NULL, last_error = ?, updated_at = ?
                WHERE job_id = ? AND status = 'leased' AND lease_owner = ?
                """,
                (status, retry_at or utc_now(), error[:2000], utc_now(), job_id, owner),
            )
            return cursor.rowcount == 1

    def set_cursor(self, name: str, value: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO cursors(cursor_name, cursor_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cursor_name) DO UPDATE SET
                    cursor_value = excluded.cursor_value,
                    updated_at = excluded.updated_at
                """,
                (name, value, utc_now()),
            )

    def get_cursor(self, name: str) -> str | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT cursor_value FROM cursors WHERE cursor_name = ?", (name,)
            ).fetchone()
            return None if row is None else str(row[0])

    def create_model_call(
        self,
        *,
        phase: str,
        source_ids: Sequence[str],
        maximum_sensitivity: str,
        input_bytes: int,
        redaction_count: int,
        model_id: str,
        reasoning_effort: str,
        prompt_hash: str,
        schema_version: str,
        call_id: str | None = None,
    ) -> str:
        """Record non-content model metadata before invoking Codex."""

        call_id = call_id or new_id("call")
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO model_calls(
                    call_id, phase, source_ids_json, maximum_sensitivity,
                    input_bytes, redaction_count, model_id, reasoning_effort,
                    prompt_hash, schema_version, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?)
                """,
                (
                    call_id,
                    phase,
                    canonical_json(sorted(set(source_ids))),
                    maximum_sensitivity,
                    input_bytes,
                    redaction_count,
                    model_id,
                    reasoning_effort,
                    prompt_hash,
                    schema_version,
                    utc_now(),
                ),
            )
        return call_id

    def finish_model_call(
        self,
        call_id: str,
        *,
        status: str,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        error_summary: str | None = None,
    ) -> None:
        if status not in {"completed", "failed", "quarantined"}:
            raise ValueError("model call terminal status is invalid")
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE model_calls
                SET status = ?, input_tokens = ?, output_tokens = ?,
                    completed_at = ?, error_summary = ?
                WHERE call_id = ? AND status = 'prepared'
                """,
                (
                    status,
                    input_tokens,
                    output_tokens,
                    utc_now(),
                    None if error_summary is None else error_summary[:2000],
                    call_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"model call is missing or already terminal: {call_id}")

    def start_nightly_run(self, *, run_id: str | None = None) -> str:
        run_id = run_id or new_id("nightly")
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO nightly_runs(run_id, status, started_at)
                VALUES (?, 'running', ?)
                """,
                (run_id, utc_now()),
            )
        return run_id

    def add_nightly_usage(
        self,
        run_id: str,
        *,
        scan_bytes: int = 0,
        episode_count: int = 0,
        model_calls: int = 0,
        input_tokens: int = 0,
        output_tokens: int = 0,
    ) -> None:
        increments = (scan_bytes, episode_count, model_calls, input_tokens, output_tokens)
        if any(value < 0 for value in increments):
            raise ValueError("nightly usage increments cannot be negative")
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE nightly_runs SET
                    scan_bytes = scan_bytes + ?,
                    episode_count = episode_count + ?,
                    model_calls = model_calls + ?,
                    input_tokens = input_tokens + ?,
                    output_tokens = output_tokens + ?
                WHERE run_id = ? AND status = 'running'
                """,
                (*increments, run_id),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"nightly run is missing or terminal: {run_id}")

    def finish_nightly_run(
        self,
        run_id: str,
        *,
        status: str,
        error_summary: str | None = None,
    ) -> None:
        if status not in {"completed", "failed", "budget_exhausted"}:
            raise ValueError("nightly terminal status is invalid")
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE nightly_runs
                SET status = ?, finished_at = ?, error_summary = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (
                    status,
                    utc_now(),
                    None if error_summary is None else error_summary[:2000],
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"nightly run is missing or terminal: {run_id}")

    def record_retrieval_usage(
        self,
        *,
        tool_name: str,
        memory_revision: int,
        result_count: int,
        returned_bytes: int,
        maximum_sensitivity: str,
        usage_id: str | None = None,
    ) -> str:
        usage_id = usage_id or new_id("retrieval")
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO retrieval_usage(
                    usage_id, tool_name, memory_revision, result_count,
                    returned_bytes, maximum_sensitivity, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    usage_id,
                    tool_name,
                    memory_revision,
                    result_count,
                    returned_bytes,
                    maximum_sensitivity,
                    utc_now(),
                ),
            )
        return usage_id

    def record_operational_snapshot(self, snapshot: object) -> str:
        """Persist an operations-only adapter snapshot in its isolated table."""

        data = _to_mapping(snapshot)
        if data.get("operations_only") is not True:
            raise ValueError("operational snapshot must be explicitly operations-only")
        snapshot_id = str(data["snapshot_id"])
        kind_value = data["snapshot_kind"]
        kind = str(getattr(kind_value, "value", kind_value))
        captured_value = data["captured_at"]
        captured_at = (
            captured_value.isoformat()
            if isinstance(captured_value, datetime)
            else str(captured_value)
        )
        payload = data.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("operational snapshot payload must be a mapping")
        status = str(data["status"])
        subject_id = str(data["subject_id"])
        with self.transaction() as connection:
            if kind == "workspace":
                connection.execute(
                    """
                    INSERT OR IGNORE INTO workspace_snapshots(
                        snapshot_id, workspace_path, captured_at, git_branch,
                        git_dirty_count, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        str(payload.get("workspace_path", subject_id)),
                        captured_at,
                        payload.get("git_branch"),
                        payload.get("git_dirty_count"),
                        canonical_json(payload),
                    ),
                )
            elif kind == "automation":
                connection.execute(
                    """
                    INSERT OR IGNORE INTO automation_snapshots(
                        snapshot_id, automation_type, automation_id, status,
                        captured_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        str(payload.get("automation_type", "unknown")),
                        str(payload.get("automation_id", subject_id)),
                        status,
                        captured_at,
                        canonical_json(payload),
                    ),
                )
            elif kind == "health":
                connection.execute(
                    """
                    INSERT OR IGNORE INTO health_snapshots(
                        snapshot_id, subject_id, status, captured_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (snapshot_id, subject_id, status, captured_at, canonical_json(payload)),
                )
            else:
                raise ValueError(f"unsupported operational snapshot kind: {kind}")
        return snapshot_id


def online_backup(source_path: str | Path, destination_path: str | Path) -> Path:
    """Create a consistent standalone SQLite backup using the Online Backup API."""

    source = Path(source_path).expanduser().resolve()
    destination = Path(destination_path).expanduser().resolve()
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(destination.parent, 0o700)
    if destination.exists():
        raise FileExistsError(destination)
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=30.0)
    destination_connection = sqlite3.connect(destination, timeout=30.0)
    try:
        destination_connection.execute("PRAGMA journal_mode = DELETE")
        source_connection.backup(destination_connection)
        destination_connection.commit()
        check_integrity(destination_connection)
    except BaseException:
        destination_connection.close()
        destination.unlink(missing_ok=True)
        raise
    finally:
        source_connection.close()
        with suppress(Exception):
            destination_connection.close()
    os.chmod(destination, 0o600)
    return destination


def restore_database(
    snapshot_path: str | Path,
    target_path: str | Path,
    *,
    expected_role: str,
) -> Path:
    """Validate a snapshot and atomically replace a closed target database.

    Callers must close all target connections before invoking this primitive.
    The forget ledger is intentionally outside this function and must be
    re-applied by the higher-level restore workflow before serving reads.
    """

    snapshot = Path(snapshot_path).expanduser().resolve()
    target = Path(target_path).expanduser().resolve()
    if not snapshot.is_file():
        raise FileNotFoundError(snapshot)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(target.parent, 0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.restore-", dir=target.parent
    )
    os.close(descriptor)
    temp_path = Path(temporary_name)
    temp_path.unlink()
    try:
        online_backup(snapshot, temp_path)
        validation = sqlite3.connect(temp_path)
        try:
            validation.row_factory = sqlite3.Row
            check_integrity(validation)
            row = validation.execute(
                "SELECT value FROM metadata WHERE key = 'database_role'"
            ).fetchone()
            actual_role = None if row is None else row[0]
            if actual_role != expected_role:
                raise DatabaseError(f"Expected {expected_role!r} snapshot, found {actual_role!r}")
            validation.execute("PRAGMA journal_mode = DELETE")
        finally:
            validation.close()
        for suffix in ("-wal", "-shm"):
            Path(f"{target}{suffix}").unlink(missing_ok=True)
        os.replace(temp_path, target)
        os.chmod(target, 0o600)
        return target
    finally:
        temp_path.unlink(missing_ok=True)


class SnapshotManager:
    """Create immutable paired memory/operations snapshots with a manifest."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory).expanduser().resolve()
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)

    def create(self, memory_path: str | Path, operations_path: str | Path) -> Path:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        snapshot_id = f"snapshot-{timestamp}-{uuid.uuid4().hex[:8]}"
        staging = self.directory / f".{snapshot_id}.staging"
        final = self.directory / snapshot_id
        staging.mkdir(mode=0o700)
        try:
            memory_backup = online_backup(memory_path, staging / "memory.sqlite3")
            operations_backup = online_backup(operations_path, staging / "operations.sqlite3")
            memory_connection = sqlite3.connect(memory_backup)
            try:
                revision = int(
                    memory_connection.execute(
                        "SELECT value FROM metadata WHERE key = 'memory_revision'"
                    ).fetchone()[0]
                )
            finally:
                memory_connection.close()
            manifest = {
                "schema_version": 1,
                "snapshot_id": snapshot_id,
                "created_at": utc_now(),
                "memory_revision": revision,
                "files": {
                    "memory.sqlite3": self._sha256(memory_backup),
                    "operations.sqlite3": self._sha256(operations_backup),
                },
            }
            manifest_path = staging / "MANIFEST.json"
            manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
            os.chmod(manifest_path, 0o600)
            for path in staging.iterdir():
                with path.open("rb") as handle:
                    os.fsync(handle.fileno())
            directory_fd = os.open(staging, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            os.replace(staging, final)
            parent_fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
            return final
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def purge(self) -> None:
        """Remove only managed snapshot directories below this exact root."""

        if self.directory in {Path("/"), Path.home().resolve()}:
            raise ValueError("refusing to purge a broad snapshot root")
        for child in self.directory.iterdir():
            managed = child.name.startswith("snapshot-") or child.name.startswith(".snapshot-")
            if child.is_dir() and managed:
                shutil.rmtree(child)

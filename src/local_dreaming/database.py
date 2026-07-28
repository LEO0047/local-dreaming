"""SQLite connection and schema primitives for Local-Dreaming.

The memory database is canonical state.  The operations database contains
runtime telemetry and queues and therefore never owns or increments the memory
revision.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import stat
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from local_dreaming.episode_identity import episode_id_for, event_sequence_fingerprint

SCHEMA_VERSION = 4

_CONNECTION_SETUP_LOCKS_GUARD = threading.Lock()
_CONNECTION_SETUP_LOCKS: dict[Path, threading.Lock] = {}


class DatabaseError(RuntimeError):
    """Base error for storage initialization and consistency failures."""


class DatabaseCapabilityError(DatabaseError):
    """Raised when the local SQLite build lacks a required capability."""


def utc_now() -> str:
    """Return a sortable UTC timestamp with second precision."""

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_json(value: object) -> str:
    """Serialize JSON deterministically for fingerprints and persisted payloads."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _prepare_private_path(path: Path) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)


def _connection_setup_lock_path(database_path: Path) -> Path:
    return database_path.parent / f".{database_path.name}.connection.lock"


def _thread_lock_for(database_path: Path) -> threading.Lock:
    with _CONNECTION_SETUP_LOCKS_GUARD:
        return _CONNECTION_SETUP_LOCKS.setdefault(database_path, threading.Lock())


@contextmanager
def _connection_setup_lock(database_path: Path) -> Iterator[None]:
    """Serialize journal-mode setup for one database across threads and processes."""

    lock_path = _connection_setup_lock_path(database_path)
    thread_lock = _thread_lock_for(database_path)
    with thread_lock:
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError("database connection lock target is not a regular file")
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _connect(path: str | Path) -> sqlite3.Connection:
    os.umask(0o077)
    database_path = Path(path).expanduser().resolve()
    _prepare_private_path(database_path)
    with _connection_setup_lock(database_path):
        connection = sqlite3.connect(
            database_path,
            timeout=30.0,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA temp_store = MEMORY")
            current_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            if current_mode != "wal":
                selected_mode = str(
                    connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                ).lower()
                if selected_mode != "wal":
                    raise DatabaseError(f"Unable to enable WAL journal mode: {selected_mode}")
            for private_file in (
                database_path,
                Path(f"{database_path}-wal"),
                Path(f"{database_path}-shm"),
            ):
                if private_file.exists():
                    os.chmod(private_file, 0o600)
        except BaseException:
            connection.close()
            raise
    return connection


def connect_memory(path: str | Path) -> sqlite3.Connection:
    """Open a configured connection to ``memory.sqlite3``."""

    return _connect(path)


def connect_operations(path: str | Path) -> sqlite3.Connection:
    """Open a configured connection to ``operations.sqlite3``."""

    return _connect(path)


@contextmanager
def transaction(
    connection: sqlite3.Connection,
    *,
    immediate: bool = True,
) -> Iterator[sqlite3.Connection]:
    """Run an atomic transaction, nesting safely through a savepoint."""

    if connection.in_transaction:
        savepoint = f"dream_sp_{id(connection):x}"
        connection.execute(f"SAVEPOINT {savepoint}")
        try:
            yield connection
        except BaseException:
            connection.execute(f"ROLLBACK TO {savepoint}")
            connection.execute(f"RELEASE {savepoint}")
            raise
        else:
            connection.execute(f"RELEASE {savepoint}")
        return

    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


MEMORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS revisions (
    revision INTEGER PRIMARY KEY CHECK (revision >= 0),
    created_at TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    display_name TEXT,
    trust_level TEXT NOT NULL CHECK (
        trust_level IN ('user_direct', 'manual', 'tool_verified', 'conversation', 'advisory')
    ),
    sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (
        sensitivity IN ('normal', 'private', 'secret')
    ),
    model_egress_allowed INTEGER NOT NULL DEFAULT 0 CHECK (model_egress_allowed IN (0, 1)),
    source_fingerprint TEXT NOT NULL UNIQUE,
    policy_version TEXT NOT NULL DEFAULT 'v1',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS source_partitions (
    partition_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    external_partition_id TEXT,
    display_name TEXT,
    opted_in INTEGER NOT NULL DEFAULT 1 CHECK (opted_in IN (0, 1)),
    sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (
        sensitivity IN ('normal', 'private', 'secret')
    ),
    partition_fingerprint TEXT NOT NULL UNIQUE,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(source_id, external_partition_id)
) STRICT;

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    partition_id TEXT REFERENCES source_partitions(partition_id) ON DELETE CASCADE,
    external_event_id TEXT,
    event_type TEXT NOT NULL,
    content_text TEXT,
    content_fingerprint TEXT NOT NULL,
    source_locator TEXT,
    sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (
        sensitivity IN ('normal', 'private', 'secret')
    ),
    occurred_at TEXT,
    captured_at TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    redactor_version TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(source_id, external_event_id)
) STRICT;

CREATE TABLE IF NOT EXISTS event_collapsed_observations (
    collapsed_observation_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    partition_id TEXT NOT NULL
        REFERENCES source_partitions(partition_id) ON DELETE CASCADE,
    source_lineage_fingerprint TEXT NOT NULL,
    evidence_family_fingerprint TEXT NOT NULL,
    observation_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE(source_id, partition_id, observation_fingerprint)
) STRICT;

CREATE TABLE IF NOT EXISTS episodes (
    episode_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    partition_id TEXT REFERENCES source_partitions(partition_id) ON DELETE CASCADE,
    parent_episode_id TEXT REFERENCES episodes(episode_id) ON DELETE SET NULL,
    episode_type TEXT NOT NULL,
    title TEXT,
    content_text TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    event_sequence_fingerprint TEXT NOT NULL,
    segmenter_version TEXT NOT NULL,
    segmentation_reason TEXT NOT NULL,
    sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (
        sensitivity IN ('normal', 'private', 'secret')
    ),
    occurred_from TEXT,
    occurred_to TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(
        source_id, partition_id, event_sequence_fingerprint, segmenter_version
    )
) STRICT;

CREATE TABLE IF NOT EXISTS episode_events (
    episode_id TEXT NOT NULL REFERENCES episodes(episode_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (episode_id, event_id),
    UNIQUE (episode_id, ordinal)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS entities (
    entity_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(entity_type, normalized_name)
) STRICT;

CREATE TABLE IF NOT EXISTS entity_aliases (
    alias_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    source_id TEXT REFERENCES sources(source_id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(entity_id, normalized_alias, source_id)
) STRICT;

CREATE TABLE IF NOT EXISTS candidate_claims (
    candidate_id TEXT PRIMARY KEY,
    episode_id TEXT NOT NULL REFERENCES episodes(episode_id) ON DELETE CASCADE,
    subject_entity_id TEXT REFERENCES entities(entity_id) ON DELETE SET NULL,
    subject_text TEXT NOT NULL,
    predicate TEXT NOT NULL,
    scope TEXT NOT NULL,
    value_json TEXT NOT NULL,
    normalized_identity_fingerprint TEXT NOT NULL,
    value_fingerprint TEXT NOT NULL,
    proposal_type TEXT NOT NULL CHECK (
        proposal_type IN ('add', 'update', 'supersede', 'dispute', 'narrow', 'no_op')
    ),
    epistemic_status TEXT NOT NULL DEFAULT 'provisional',
    valid_from TEXT,
    valid_to TEXT,
    possible_supersedes_claim_id TEXT REFERENCES claims(claim_id) ON DELETE SET NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (
        sensitivity IN ('normal', 'private', 'secret')
    ),
    extraction_fingerprint TEXT NOT NULL UNIQUE,
    extractor_version TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    model_id TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS candidate_evidence (
    candidate_id TEXT NOT NULL
        REFERENCES candidate_claims(candidate_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY(candidate_id, event_id),
    UNIQUE(candidate_id, ordinal)
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS candidate_disposition_events (
    disposition_event_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL
        REFERENCES candidate_claims(candidate_id) ON DELETE CASCADE,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('eligible', 'proposed', 'suppressed')
    ),
    reason_code TEXT NOT NULL,
    recorded_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS claims (
    claim_id TEXT PRIMARY KEY,
    subject_entity_id TEXT REFERENCES entities(entity_id) ON DELETE SET NULL,
    subject_text TEXT NOT NULL,
    predicate TEXT NOT NULL,
    scope TEXT NOT NULL,
    identity_fingerprint TEXT NOT NULL UNIQUE,
    created_revision INTEGER NOT NULL REFERENCES revisions(revision),
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS claim_versions (
    claim_version_id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL REFERENCES claims(claim_id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL CHECK (version_number >= 1),
    value_json TEXT NOT NULL,
    value_fingerprint TEXT NOT NULL,
    summary TEXT NOT NULL,
    mcp_safe_summary TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('approved', 'disputed', 'outcome_unknown', 'superseded', 'rejected')
    ),
    valid_from TEXT,
    valid_to TEXT,
    recorded_revision INTEGER NOT NULL REFERENCES revisions(revision),
    recorded_at TEXT NOT NULL,
    superseded_at TEXT,
    confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    epistemic_status TEXT NOT NULL CHECK (
        epistemic_status IN (
            'observed', 'verified', 'provisional', 'uncertain',
            'user_confirmed', 'outcome_unknown'
        )
    ),
    sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (
        sensitivity IN ('normal', 'private', 'secret')
    ),
    provenance_kind TEXT NOT NULL CHECK (
        provenance_kind IN ('user_direct', 'manual', 'tool_verified', 'model_proposal')
    ),
    UNIQUE(claim_id, version_number),
    UNIQUE(claim_id, value_fingerprint, valid_from, valid_to, recorded_revision)
) STRICT;

CREATE TABLE IF NOT EXISTS claim_relations (
    relation_id TEXT PRIMARY KEY,
    from_claim_version_id TEXT NOT NULL
        REFERENCES claim_versions(claim_version_id) ON DELETE CASCADE,
    to_claim_version_id TEXT NOT NULL
        REFERENCES claim_versions(claim_version_id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL CHECK (
        relation_type IN (
            'supports', 'supersedes', 'contradicts', 'narrows',
            'broadens', 'merges', 'derived_from'
        )
    ),
    created_revision INTEGER NOT NULL REFERENCES revisions(revision),
    created_at TEXT NOT NULL,
    UNIQUE(from_claim_version_id, to_claim_version_id, relation_type),
    CHECK(from_claim_version_id <> to_claim_version_id)
) STRICT;

CREATE TABLE IF NOT EXISTS claim_evidence (
    evidence_id TEXT PRIMARY KEY,
    claim_version_id TEXT NOT NULL REFERENCES claim_versions(claim_version_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    evidence_type TEXT NOT NULL,
    bounded_excerpt TEXT,
    source_locator TEXT,
    captured_at TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    redactor_version TEXT NOT NULL,
    evidence_fingerprint TEXT NOT NULL,
    UNIQUE(claim_version_id, event_id, evidence_fingerprint)
) STRICT;

CREATE TABLE IF NOT EXISTS claim_pin_events (
    pin_event_id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL REFERENCES claims(claim_id) ON DELETE CASCADE,
    pinned INTEGER NOT NULL CHECK (pinned IN (0, 1)),
    recorded_revision INTEGER NOT NULL REFERENCES revisions(revision),
    recorded_at TEXT NOT NULL,
    UNIQUE(claim_id, recorded_revision)
) STRICT;

CREATE TABLE IF NOT EXISTS review_batches (
    batch_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'approved', 'rejected', 'stale', 'applied')
    ),
    base_memory_revision INTEGER NOT NULL REFERENCES revisions(revision),
    extractor_run_id TEXT,
    consolidator_run_id TEXT,
    created_at TEXT NOT NULL,
    decided_at TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS review_proposals (
    proposal_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES review_batches(batch_id) ON DELETE CASCADE,
    candidate_id TEXT REFERENCES candidate_claims(candidate_id) ON DELETE SET NULL,
    target_claim_id TEXT REFERENCES claims(claim_id) ON DELETE SET NULL,
    proposal_type TEXT NOT NULL CHECK (
        proposal_type IN ('add', 'update', 'supersede', 'dispute', 'narrow')
    ),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'approved', 'rejected', 'corrected', 'stale', 'applied')
    ),
    base_memory_revision INTEGER NOT NULL REFERENCES revisions(revision),
    target_slot_fingerprint TEXT NOT NULL,
    expected_head_set_hash TEXT NOT NULL,
    precondition_hash TEXT NOT NULL,
    proposal_payload_fingerprint TEXT NOT NULL,
    evidence_set_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    decision_note TEXT,
    UNIQUE(batch_id, target_slot_fingerprint)
) STRICT;

CREATE TABLE IF NOT EXISTS review_proposal_candidates (
    proposal_id TEXT NOT NULL
        REFERENCES review_proposals(proposal_id) ON DELETE CASCADE,
    candidate_id TEXT NOT NULL
        REFERENCES candidate_claims(candidate_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY(proposal_id, candidate_id),
    UNIQUE(proposal_id, ordinal)
) WITHOUT ROWID, STRICT;

CREATE VIRTUAL TABLE IF NOT EXISTS claim_fts USING fts5(
    claim_version_id UNINDEXED,
    claim_id UNINDEXED,
    subject,
    predicate,
    scope,
    summary,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE VIEW IF NOT EXISTS current_claim_versions AS
SELECT cv.*
FROM claim_versions AS cv
WHERE cv.status IN ('approved', 'disputed', 'outcome_unknown')
  AND NOT EXISTS (
      SELECT 1
      FROM claim_relations AS rel
      JOIN claim_versions AS newer
        ON newer.claim_version_id = rel.from_claim_version_id
      WHERE rel.to_claim_version_id = cv.claim_version_id
        AND rel.relation_type IN ('supersedes', 'narrows', 'merges')
        AND newer.status IN ('approved', 'disputed', 'outcome_unknown')
  );

CREATE VIEW IF NOT EXISTS current_claim_pins AS
SELECT event.claim_id, event.recorded_revision, event.recorded_at
FROM claim_pin_events AS event
WHERE event.pinned = 1
  AND NOT EXISTS (
      SELECT 1 FROM claim_pin_events AS newer
      WHERE newer.claim_id = event.claim_id
        AND (
            newer.recorded_revision > event.recorded_revision
            OR (
                newer.recorded_revision = event.recorded_revision
                AND newer.pin_event_id > event.pin_event_id
            )
        )
  );

CREATE VIEW IF NOT EXISTS current_candidate_dispositions AS
SELECT event.candidate_id, event.disposition, event.reason_code,
       event.recorded_at, event.disposition_event_id
FROM candidate_disposition_events AS event
WHERE NOT EXISTS (
    SELECT 1 FROM candidate_disposition_events AS newer
    WHERE newer.candidate_id = event.candidate_id
      AND newer.rowid > event.rowid
);

CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_events_partition ON events(partition_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_collapsed_observations_event
    ON event_collapsed_observations(event_id, observed_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_stable_content
    ON events(source_id, content_fingerprint, COALESCE(occurred_at, ''))
    WHERE external_event_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_episodes_source ON episodes(source_id, created_at);
CREATE INDEX IF NOT EXISTS idx_candidate_identity
    ON candidate_claims(normalized_identity_fingerprint);
CREATE INDEX IF NOT EXISTS idx_candidate_evidence_event ON candidate_evidence(event_id);
CREATE INDEX IF NOT EXISTS idx_candidate_dispositions_current
    ON candidate_disposition_events(candidate_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_claim_versions_current
    ON claim_versions(claim_id, superseded_at, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_claim_versions_stable_value
    ON claim_versions(
        claim_id, value_fingerprint, COALESCE(valid_from, ''),
        COALESCE(valid_to, ''), recorded_revision
    );
CREATE INDEX IF NOT EXISTS idx_claim_evidence_event ON claim_evidence(event_id);
CREATE INDEX IF NOT EXISTS idx_claim_pin_events_claim
    ON claim_pin_events(claim_id, recorded_revision DESC);
CREATE INDEX IF NOT EXISTS idx_review_proposals_batch ON review_proposals(batch_id, status);
CREATE INDEX IF NOT EXISTS idx_review_proposal_candidates_candidate
    ON review_proposal_candidates(candidate_id);

CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events are immutable');
END;

CREATE TRIGGER IF NOT EXISTS event_collapsed_observations_no_update
BEFORE UPDATE ON event_collapsed_observations
BEGIN
    SELECT RAISE(ABORT, 'collapsed event observations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS episodes_no_update
BEFORE UPDATE ON episodes
BEGIN
    SELECT RAISE(ABORT, 'episodes are immutable');
END;

CREATE TRIGGER IF NOT EXISTS claim_versions_no_update
BEFORE UPDATE ON claim_versions
BEGIN
    SELECT RAISE(ABORT, 'claim versions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS candidate_dispositions_no_update
BEFORE UPDATE ON candidate_disposition_events
BEGIN
    SELECT RAISE(ABORT, 'candidate dispositions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS claim_relations_no_update
BEFORE UPDATE ON claim_relations
BEGIN
    SELECT RAISE(ABORT, 'claim relations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS claim_evidence_no_update
BEFORE UPDATE ON claim_evidence
BEGIN
    SELECT RAISE(ABORT, 'claim evidence is immutable');
END;

CREATE TRIGGER IF NOT EXISTS claim_pin_events_no_update
BEFORE UPDATE ON claim_pin_events
BEGIN
    SELECT RAISE(ABORT, 'claim pin events are immutable');
END;

CREATE TRIGGER IF NOT EXISTS review_proposal_binding_no_update
BEFORE UPDATE ON review_proposals
WHEN NEW.batch_id IS NOT OLD.batch_id
  OR NEW.candidate_id IS NOT OLD.candidate_id
  OR NEW.target_claim_id IS NOT OLD.target_claim_id
  OR NEW.proposal_type IS NOT OLD.proposal_type
  OR NEW.base_memory_revision IS NOT OLD.base_memory_revision
  OR NEW.target_slot_fingerprint IS NOT OLD.target_slot_fingerprint
  OR NEW.expected_head_set_hash IS NOT OLD.expected_head_set_hash
  OR NEW.precondition_hash IS NOT OLD.precondition_hash
  OR NEW.proposal_payload_fingerprint IS NOT OLD.proposal_payload_fingerprint
  OR NEW.evidence_set_fingerprint IS NOT OLD.evidence_set_fingerprint
  OR NEW.payload_json IS NOT OLD.payload_json
BEGIN
    SELECT RAISE(ABORT, 'review proposal binding is immutable');
END;
"""


OPERATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'leased', 'completed', 'failed', 'cancelled')
    ),
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS cursors (
    cursor_name TEXT PRIMARY KEY,
    cursor_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS nightly_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (
        status IN ('running', 'completed', 'failed', 'budget_exhausted')
    ),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    scan_bytes INTEGER NOT NULL DEFAULT 0,
    episode_count INTEGER NOT NULL DEFAULT 0,
    model_calls INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    error_summary TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS workspace_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    workspace_path TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    git_branch TEXT,
    git_dirty_count INTEGER,
    payload_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS automation_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    automation_type TEXT NOT NULL,
    automation_id TEXT NOT NULL,
    status TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS health_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    status TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE IF NOT EXISTS retrieval_usage (
    usage_id TEXT PRIMARY KEY,
    tool_name TEXT NOT NULL,
    memory_revision INTEGER NOT NULL,
    result_count INTEGER NOT NULL CHECK (result_count >= 0),
    returned_bytes INTEGER NOT NULL CHECK (returned_bytes >= 0),
    maximum_sensitivity TEXT NOT NULL CHECK (
        maximum_sensitivity IN ('normal', 'private', 'secret')
    ),
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_calls (
    call_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL CHECK (phase IN ('phase1', 'phase2', 'doctor')),
    source_ids_json TEXT NOT NULL,
    maximum_sensitivity TEXT NOT NULL CHECK (
        maximum_sensitivity IN ('normal', 'private', 'secret')
    ),
    input_bytes INTEGER NOT NULL CHECK (input_bytes >= 0),
    redaction_count INTEGER NOT NULL CHECK (redaction_count >= 0),
    input_tokens INTEGER,
    output_tokens INTEGER,
    model_id TEXT NOT NULL,
    reasoning_effort TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('prepared', 'completed', 'failed', 'quarantined')
    ),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    error_summary TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_jobs_available ON jobs(status, available_at, priority DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_model_calls_created ON model_calls(created_at);
CREATE INDEX IF NOT EXISTS idx_retrieval_usage_created ON retrieval_usage(created_at);
CREATE INDEX IF NOT EXISTS idx_health_snapshots_created ON health_snapshots(captured_at);
"""


def _assert_fts5(connection: sqlite3.Connection) -> None:
    try:
        connection.execute("CREATE VIRTUAL TABLE temp.__dream_fts_probe USING fts5(value)")
        connection.execute("DROP TABLE temp.__dream_fts_probe")
    except sqlite3.OperationalError as exc:
        raise DatabaseCapabilityError("Local-Dreaming requires SQLite FTS5") from exc


_EPISODES_V2_SCHEMA = """
CREATE TABLE episodes_v2 (
    episode_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE CASCADE,
    partition_id TEXT REFERENCES source_partitions(partition_id) ON DELETE CASCADE,
    parent_episode_id TEXT REFERENCES episodes_v2(episode_id) ON DELETE SET NULL,
    episode_type TEXT NOT NULL,
    title TEXT,
    content_text TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    event_sequence_fingerprint TEXT NOT NULL,
    segmenter_version TEXT NOT NULL,
    segmentation_reason TEXT NOT NULL,
    sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (
        sensitivity IN ('normal', 'private', 'secret')
    ),
    occurred_from TEXT,
    occurred_to TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(
        source_id, partition_id, event_sequence_fingerprint, segmenter_version
    )
) STRICT
"""


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _migrate_episode_identity_v2(connection: sqlite3.Connection) -> None:
    """Replace content-based episode uniqueness while preserving stable IDs and FKs."""

    columns = _table_columns(connection, "episodes")
    if not columns or "event_sequence_fingerprint" in columns:
        return
    if connection.in_transaction:
        raise DatabaseError("episode identity migration requires an outer transaction boundary")

    foreign_keys_enabled = bool(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    connection.execute("PRAGMA foreign_keys = OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(_EPISODES_V2_SCHEMA)
        episodes = connection.execute("SELECT * FROM episodes ORDER BY episode_id").fetchall()
        for episode in episodes:
            event_ids = tuple(
                str(row["event_id"])
                for row in connection.execute(
                    """
                    SELECT event_id FROM episode_events
                    WHERE episode_id = ? ORDER BY ordinal
                    """,
                    (episode["episode_id"],),
                ).fetchall()
            )
            source_id = str(episode["source_id"])
            partition_raw = episode["partition_id"]
            if partition_raw is None:
                raise DatabaseError(f"episode {episode['episode_id']} has no partition identity")
            partition_id = str(partition_raw)
            segmenter_version = str(episode["segmenter_version"])
            expected_id = episode_id_for(
                source_id=source_id,
                partition_id=partition_id,
                event_ids=event_ids,
                segmenter_version=segmenter_version,
            )
            if expected_id != episode["episode_id"]:
                raise DatabaseError(
                    "existing episode ID does not match the frozen logical identity; "
                    "an explicit migration mapping is required"
                )
            connection.execute(
                """
                INSERT INTO episodes_v2(
                    episode_id, source_id, partition_id, parent_episode_id,
                    episode_type, title, content_text, content_fingerprint,
                    event_sequence_fingerprint, segmenter_version,
                    segmentation_reason, sensitivity, occurred_from, occurred_to,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    episode["episode_id"],
                    source_id,
                    partition_id,
                    episode["parent_episode_id"],
                    episode["episode_type"],
                    episode["title"],
                    episode["content_text"],
                    episode["content_fingerprint"],
                    event_sequence_fingerprint(event_ids),
                    segmenter_version,
                    episode["segmentation_reason"],
                    episode["sensitivity"],
                    episode["occurred_from"],
                    episode["occurred_to"],
                    episode["created_at"],
                ),
            )
        connection.execute("DROP TABLE episodes")
        connection.execute("ALTER TABLE episodes_v2 RENAME TO episodes")
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise DatabaseError("episode identity migration would break foreign keys")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.execute(f"PRAGMA foreign_keys = {1 if foreign_keys_enabled else 0}")


def _initialize(
    connection: sqlite3.Connection,
    schema: str,
    *,
    role: str,
    require_fts5: bool,
) -> None:
    if require_fts5:
        _assert_fts5(connection)
    if role == "memory":
        _migrate_episode_identity_v2(connection)
    connection.executescript("BEGIN IMMEDIATE;\n" + schema)
    try:
        connection.execute(
            """
            INSERT INTO metadata(key, value) VALUES ('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(SCHEMA_VERSION),),
        )
        connection.execute(
            "INSERT OR IGNORE INTO metadata(key, value) VALUES ('database_role', ?)",
            (role,),
        )
        actual_role = connection.execute(
            "SELECT value FROM metadata WHERE key = 'database_role'"
        ).fetchone()[0]
        if actual_role != role:
            raise DatabaseError(f"Expected {role!r} database, found {actual_role!r}")
        if role == "memory":
            now = utc_now()
            connection.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES ('memory_revision', '0')"
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO revisions(revision, created_at, actor, reason, details_json)
                VALUES (0, ?, 'system', 'database initialized', '{}')
                """,
                (now,),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO candidate_disposition_events(
                    disposition_event_id, candidate_id, disposition,
                    reason_code, recorded_at
                )
                SELECT 'canddisp_legacy_' || cc.candidate_id,
                       cc.candidate_id, 'suppressed',
                       'schema_v3_legacy_suppression', cc.created_at
                FROM candidate_claims AS cc
                WHERE NOT EXISTS (
                    SELECT 1 FROM candidate_disposition_events AS cde
                    WHERE cde.candidate_id = cc.candidate_id
                )
                """
            )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def initialize_memory(path: str | Path) -> None:
    connection = connect_memory(path)
    try:
        _initialize(connection, MEMORY_SCHEMA, role="memory", require_fts5=True)
    finally:
        connection.close()


def initialize_operations(path: str | Path) -> None:
    connection = connect_operations(path)
    try:
        _initialize(connection, OPERATIONS_SCHEMA, role="operations", require_fts5=False)
    finally:
        connection.close()


def initialize_databases(memory_path: str | Path, operations_path: str | Path) -> None:
    """Create or migrate both databases to the current schema."""

    initialize_memory(memory_path)
    initialize_operations(operations_path)


def get_memory_revision(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT value FROM metadata WHERE key = 'memory_revision'").fetchone()
    if row is None:
        raise DatabaseError("memory_revision metadata is missing")
    return int(row[0])


def bump_memory_revision(
    connection: sqlite3.Connection,
    *,
    actor: str,
    reason: str,
    details: object | None = None,
) -> int:
    """Append a revision within the caller's transaction."""

    if not connection.in_transaction:
        raise DatabaseError("bump_memory_revision must run inside a transaction")
    revision = get_memory_revision(connection) + 1
    connection.execute(
        "UPDATE metadata SET value = ? WHERE key = 'memory_revision'",
        (str(revision),),
    )
    connection.execute(
        """
        INSERT INTO revisions(revision, created_at, actor, reason, details_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (revision, utc_now(), actor, reason, canonical_json(details or {})),
    )
    return revision


def check_integrity(connection: sqlite3.Connection) -> None:
    """Raise if SQLite integrity or foreign-key checks find a problem."""

    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise DatabaseError(f"SQLite integrity_check failed: {integrity}")
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        raise DatabaseError(f"SQLite foreign_key_check found {len(foreign_keys)} violation(s)")

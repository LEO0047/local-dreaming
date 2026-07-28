"""Practical erasure semantics for Local-Dreaming.

``forgotten.jsonl`` is intentionally simpler than a compliance-grade erasure
ledger.  It lives outside managed SQLite snapshots so restoring an old database
cannot silently relearn a fact from events that still exist in that snapshot.
"""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import sqlite3
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from .database import bump_memory_revision, canonical_json, utc_now
from .storage import MemoryStore, SnapshotManager, fingerprint


@dataclass(frozen=True, slots=True)
class ForgottenEntry:
    forget_id: str
    status: str
    target_kind: str
    target_fingerprint: str
    identity_fingerprint: str | None
    value_fingerprint: str | None
    created_at: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ForgetReport:
    target_kind: str
    target_id: str
    dry_run: bool
    deleted_sources: int = 0
    deleted_events: int = 0
    deleted_episodes: int = 0
    deleted_candidates: int = 0
    deleted_claims: int = 0
    deleted_claim_versions: int = 0
    deleted_evidence: int = 0
    deleted_aliases: int = 0
    memory_revision: int | None = None


class ForgottenLedger:
    """Append-only, fsync'd JSONL suppression records."""

    VALID_TARGETS = {"claim_exact", "claim_identity", "source"}
    VALID_STATUSES = {"pending", "applied"}

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        if not self.path.exists():
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
        os.chmod(self.path, 0o600)

    def append(self, entry: ForgottenEntry) -> None:
        if entry.target_kind not in self.VALID_TARGETS:
            raise ValueError(f"unsupported forget target: {entry.target_kind}")
        if entry.status not in self.VALID_STATUSES:
            raise ValueError(f"unsupported forget status: {entry.status}")
        encoded = canonical_json(asdict(entry)) + "\n"
        with self.path.open("a", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def prepare(
        self,
        *,
        target_kind: str,
        target_fingerprint: str,
        identity_fingerprint: str | None = None,
        value_fingerprint: str | None = None,
        reason: str | None = None,
    ) -> ForgottenEntry:
        entry = ForgottenEntry(
            forget_id=f"forget_{uuid.uuid4().hex}",
            status="pending",
            target_kind=target_kind,
            target_fingerprint=target_fingerprint,
            identity_fingerprint=identity_fingerprint,
            value_fingerprint=value_fingerprint,
            created_at=utc_now(),
            reason=reason,
        )
        self.append(entry)
        return entry

    def mark_applied(self, entry: ForgottenEntry) -> ForgottenEntry:
        applied = ForgottenEntry(
            forget_id=entry.forget_id,
            status="applied",
            target_kind=entry.target_kind,
            target_fingerprint=entry.target_fingerprint,
            identity_fingerprint=entry.identity_fingerprint,
            value_fingerprint=entry.value_fingerprint,
            created_at=utc_now(),
            reason=entry.reason,
        )
        self.append(applied)
        return applied

    def entries(self) -> list[ForgottenEntry]:
        records: list[ForgottenEntry] = []
        with self.path.open("r", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
            try:
                for line_number, raw_line in enumerate(handle, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                        records.append(ForgottenEntry(**payload))
                    except (json.JSONDecodeError, TypeError) as exc:
                        raise ValueError(
                            f"invalid forgotten ledger record at line {line_number}"
                        ) from exc
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return records

    def effective_entries(self) -> list[ForgottenEntry]:
        """Return one active record per operation, including pending records.

        Pending is deliberately suppression-active: a crash after fsync but
        before the database transaction must fail toward not relearning data.
        """

        latest: dict[str, ForgottenEntry] = {}
        for entry in self.entries():
            latest[entry.forget_id] = entry
        return list(latest.values())

    def is_claim_forgotten(self, identity: str, value: str) -> bool:
        for entry in self.effective_entries():
            if entry.target_kind == "claim_identity" and entry.identity_fingerprint == identity:
                return True
            if (
                entry.target_kind == "claim_exact"
                and entry.identity_fingerprint == identity
                and entry.value_fingerprint == value
            ):
                return True
        return False

    def is_source_forgotten(self, source_fingerprint: str) -> bool:
        return any(
            entry.target_kind == "source" and entry.target_fingerprint == source_fingerprint
            for entry in self.effective_entries()
        )


def _counts_for_claim(connection: sqlite3.Connection, claim_id: str) -> dict[str, int]:
    versions = int(
        connection.execute(
            "SELECT COUNT(*) FROM claim_versions WHERE claim_id = ?", (claim_id,)
        ).fetchone()[0]
    )
    evidence = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM claim_evidence
            WHERE claim_version_id IN (
                SELECT claim_version_id FROM claim_versions WHERE claim_id = ?
            )
            """,
            (claim_id,),
        ).fetchone()[0]
    )
    return {"versions": versions, "evidence": evidence}


def _purge_artifacts(directory: str | Path | None) -> None:
    if directory is None:
        return
    root = Path(directory).expanduser().resolve()
    if not root.exists():
        return
    if root in {Path("/"), Path.home().resolve()}:
        raise ValueError("refusing to purge a broad artifact root")
    for child in root.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _purge_review_material(
    connection: sqlite3.Connection,
    *,
    candidate_ids: Sequence[str] = (),
    target_claim_ids: Sequence[str] = (),
    target_slots: Sequence[str] = (),
) -> int:
    """Delete proposal payloads derived from data that is being forgotten."""

    proposal_ids: set[str] = set()
    if candidate_ids:
        placeholders = ",".join("?" for _ in candidate_ids)
        proposal_ids.update(
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT proposal_id FROM review_proposal_candidates
                WHERE candidate_id IN ({placeholders})
                UNION
                SELECT proposal_id FROM review_proposals
                WHERE candidate_id IN ({placeholders})
                """,
                (*candidate_ids, *candidate_ids),
            )
        )
    if target_claim_ids:
        placeholders = ",".join("?" for _ in target_claim_ids)
        proposal_ids.update(
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT proposal_id FROM review_proposals
                WHERE target_claim_id IN ({placeholders})
                """,
                tuple(target_claim_ids),
            )
        )
    if target_slots:
        placeholders = ",".join("?" for _ in target_slots)
        proposal_ids.update(
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT proposal_id FROM review_proposals
                WHERE target_slot_fingerprint IN ({placeholders})
                """,
                tuple(target_slots),
            )
        )
    if proposal_ids:
        placeholders = ",".join("?" for _ in proposal_ids)
        connection.execute(
            f"DELETE FROM review_proposals WHERE proposal_id IN ({placeholders})",
            tuple(sorted(proposal_ids)),
        )
        connection.execute(
            """
            DELETE FROM review_batches
            WHERE NOT EXISTS (
                SELECT 1 FROM review_proposals
                WHERE review_proposals.batch_id = review_batches.batch_id
            )
            """
        )
    return len(proposal_ids)


def _refresh_snapshots(
    snapshot_manager: SnapshotManager | None,
    *,
    memory_path: Path,
    operations_path: str | Path | None,
) -> None:
    if snapshot_manager is None:
        return
    snapshot_manager.purge()
    if operations_path is not None:
        snapshot_manager.create(memory_path, operations_path)


def forget_claim(
    store: MemoryStore,
    ledger: ForgottenLedger,
    claim_id: str,
    *,
    identity_wide: bool = False,
    reason: str | None = None,
    dry_run: bool = False,
    snapshot_manager: SnapshotManager | None = None,
    operations_path: str | Path | None = None,
    artifacts_dir: str | Path | None = None,
) -> ForgetReport:
    """Forget every canonical version under a claim identity.

    Default suppression records every currently known value.  ``identity_wide``
    instead prevents any future value in the subject/predicate/scope slot.
    """

    prepared: list[ForgottenEntry] = []
    with store.transaction() as connection:
        claim = connection.execute(
            "SELECT identity_fingerprint FROM claims WHERE claim_id = ?", (claim_id,)
        ).fetchone()
        if claim is None:
            raise KeyError(f"unknown claim: {claim_id}")
        identity = str(claim[0])
        version_rows = connection.execute(
            """
            SELECT value_fingerprint FROM claim_versions
            WHERE claim_id = ? ORDER BY version_number
            """,
            (claim_id,),
        ).fetchall()
        counts = _counts_for_claim(connection, claim_id)
        value_fingerprints = {str(row[0]) for row in version_rows}
        if identity_wide:
            candidate_rows = connection.execute(
                """
                SELECT candidate_id FROM candidate_claims
                WHERE normalized_identity_fingerprint = ?
                """,
                (identity,),
            ).fetchall()
        elif value_fingerprints:
            placeholders = ",".join("?" for _ in value_fingerprints)
            candidate_rows = connection.execute(
                f"""
                SELECT candidate_id FROM candidate_claims
                WHERE normalized_identity_fingerprint = ?
                  AND value_fingerprint IN ({placeholders})
                """,
                (identity, *sorted(value_fingerprints)),
            ).fetchall()
        else:
            candidate_rows = []
        candidate_ids = tuple(str(row[0]) for row in candidate_rows)

        report = ForgetReport(
            target_kind="claim_identity" if identity_wide else "claim_exact",
            target_id=claim_id,
            dry_run=dry_run,
            deleted_claims=1,
            deleted_claim_versions=counts["versions"],
            deleted_evidence=counts["evidence"],
            deleted_candidates=len(candidate_ids),
        )
        if dry_run:
            return report

        # Purge compiled reads before making suppression durable.  Holding the
        # canonical maintenance lock prevents another repository writer from
        # slipping a new value/source-derived version into the read-set below.
        _purge_artifacts(artifacts_dir)
        if identity_wide:
            prepared.append(
                ledger.prepare(
                    target_kind="claim_identity",
                    target_fingerprint=fingerprint("forget-claim-identity-v1", identity),
                    identity_fingerprint=identity,
                    reason=reason,
                )
            )
        else:
            if not version_rows:
                prepared.append(
                    ledger.prepare(
                        target_kind="claim_identity",
                        target_fingerprint=fingerprint("forget-claim-identity-v1", identity),
                        identity_fingerprint=identity,
                        reason=reason,
                    )
                )
            for row in version_rows:
                value = str(row[0])
                prepared.append(
                    ledger.prepare(
                        target_kind="claim_exact",
                        target_fingerprint=fingerprint("forget-claim-exact-v1", identity, value),
                        identity_fingerprint=identity,
                        value_fingerprint=value,
                        reason=reason,
                    )
                )

        _purge_review_material(
            connection,
            candidate_ids=candidate_ids,
            target_claim_ids=(claim_id,),
            target_slots=(identity,),
        )
        if candidate_ids:
            placeholders = ",".join("?" for _ in candidate_ids)
            connection.execute(
                f"DELETE FROM candidate_claims WHERE candidate_id IN ({placeholders})",
                candidate_ids,
            )
        connection.execute(
            "DELETE FROM claim_fts WHERE claim_id = ?",
            (claim_id,),
        )
        deleted = connection.execute("DELETE FROM claims WHERE claim_id = ?", (claim_id,))
        if deleted.rowcount != 1:
            raise RuntimeError("claim changed while forgetting")
        revision = bump_memory_revision(
            connection,
            actor="leo",
            reason="claim forgotten",
            details={"claim_id": claim_id, "identity_wide": identity_wide},
        )
    for entry in prepared:
        ledger.mark_applied(entry)
    _refresh_snapshots(
        snapshot_manager,
        memory_path=store.path,
        operations_path=operations_path,
    )
    return ForgetReport(**{**asdict(report), "memory_revision": revision})


def _source_counts(connection: sqlite3.Connection, source_id: str) -> dict[str, int]:
    queries = {
        "events": "SELECT COUNT(*) FROM events WHERE source_id = ?",
        "episodes": "SELECT COUNT(*) FROM episodes WHERE source_id = ?",
        "candidates": """
            SELECT COUNT(*) FROM candidate_claims
            WHERE episode_id IN (SELECT episode_id FROM episodes WHERE source_id = ?)
        """,
        "evidence": """
            SELECT COUNT(*) FROM claim_evidence
            WHERE event_id IN (SELECT event_id FROM events WHERE source_id = ?)
        """,
        "aliases": "SELECT COUNT(*) FROM entity_aliases WHERE source_id = ?",
    }
    return {
        name: int(connection.execute(query, (source_id,)).fetchone()[0])
        for name, query in queries.items()
    }


def _prune_unsupported_versions(
    connection: sqlite3.Connection,
    *,
    candidate_version_ids: set[str] | None = None,
) -> tuple[int, int]:
    params: list[object] = []
    candidate_clause = ""
    if candidate_version_ids is not None:
        if not candidate_version_ids:
            return 0, 0
        placeholders = ",".join("?" for _ in candidate_version_ids)
        candidate_clause = f" AND cv.claim_version_id IN ({placeholders})"
        params.extend(sorted(candidate_version_ids))
    unsupported = connection.execute(
        f"""
        SELECT cv.claim_version_id, cv.claim_id
        FROM claim_versions AS cv
        WHERE cv.provenance_kind IN ('tool_verified', 'model_proposal')
          AND NOT EXISTS (
              SELECT 1 FROM claim_evidence AS ce
              WHERE ce.claim_version_id = cv.claim_version_id
          )
          {candidate_clause}
        """,
        params,
    ).fetchall()
    version_ids = [str(row[0]) for row in unsupported]
    for version_id in version_ids:
        connection.execute("DELETE FROM claim_fts WHERE claim_version_id = ?", (version_id,))
        connection.execute("DELETE FROM claim_versions WHERE claim_version_id = ?", (version_id,))
    orphan_claims = connection.execute(
        """
        SELECT claim_id FROM claims
        WHERE NOT EXISTS (
            SELECT 1 FROM claim_versions WHERE claim_versions.claim_id = claims.claim_id
        )
        """
    ).fetchall()
    for row in orphan_claims:
        connection.execute("DELETE FROM claims WHERE claim_id = ?", (row[0],))
    return len(version_ids), len(orphan_claims)


def forget_source(
    store: MemoryStore,
    ledger: ForgottenLedger,
    source_id: str,
    *,
    reason: str | None = None,
    dry_run: bool = False,
    snapshot_manager: SnapshotManager | None = None,
    operations_path: str | Path | None = None,
    artifacts_dir: str | Path | None = None,
) -> ForgetReport:
    """Forget a source and prune only claims left without independent support."""

    with store.transaction() as connection:
        source = connection.execute(
            "SELECT source_fingerprint FROM sources WHERE source_id = ?", (source_id,)
        ).fetchone()
        if source is None:
            raise KeyError(f"unknown source: {source_id}")
        source_fingerprint = str(source[0])
        counts = _source_counts(connection, source_id)
        affected_version_rows = connection.execute(
            """
            SELECT DISTINCT cv.claim_version_id
            FROM claim_versions AS cv
            JOIN claim_evidence AS ce ON ce.claim_version_id = cv.claim_version_id
            JOIN events AS e ON e.event_id = ce.event_id
            WHERE e.source_id = ?
            """,
            (source_id,),
        ).fetchall()
        affected_version_ids = {str(row[0]) for row in affected_version_rows}
        candidate_id_rows = connection.execute(
            """
            SELECT candidate_id FROM candidate_claims
            WHERE episode_id IN (SELECT episode_id FROM episodes WHERE source_id = ?)
            ORDER BY candidate_id
            """,
            (source_id,),
        ).fetchall()
        candidate_ids = tuple(str(row[0]) for row in candidate_id_rows)
        potentially_pruned = int(
            connection.execute(
                """
                SELECT COUNT(DISTINCT cv.claim_version_id)
                FROM claim_versions AS cv
                JOIN claim_evidence AS ce ON ce.claim_version_id = cv.claim_version_id
                JOIN events AS e ON e.event_id = ce.event_id
                WHERE e.source_id = ?
                  AND cv.provenance_kind IN ('tool_verified', 'model_proposal')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM claim_evidence AS ce2
                      JOIN events AS e2 ON e2.event_id = ce2.event_id
                      WHERE ce2.claim_version_id = cv.claim_version_id
                        AND e2.source_id <> ?
                  )
                """,
                (source_id, source_id),
            ).fetchone()[0]
        )

        report = ForgetReport(
            target_kind="source",
            target_id=source_id,
            dry_run=dry_run,
            deleted_sources=1,
            deleted_events=counts["events"],
            deleted_episodes=counts["episodes"],
            deleted_candidates=counts["candidates"],
            deleted_claim_versions=potentially_pruned,
            deleted_evidence=counts["evidence"],
            deleted_aliases=counts["aliases"],
        )
        if dry_run:
            return report

        _purge_artifacts(artifacts_dir)
        prepared = ledger.prepare(
            target_kind="source",
            target_fingerprint=source_fingerprint,
            reason=reason,
        )
        _purge_review_material(connection, candidate_ids=candidate_ids)
        deleted = connection.execute("DELETE FROM sources WHERE source_id = ?", (source_id,))
        if deleted.rowcount != 1:
            raise RuntimeError("source changed while forgetting")
        deleted_versions, deleted_claims = _prune_unsupported_versions(
            connection,
            candidate_version_ids=affected_version_ids,
        )
        revision = bump_memory_revision(
            connection,
            actor="leo",
            reason="source forgotten",
            details={"source_id": source_id},
        )
    ledger.mark_applied(prepared)
    _refresh_snapshots(
        snapshot_manager,
        memory_path=store.path,
        operations_path=operations_path,
    )
    return ForgetReport(
        **{
            **asdict(report),
            "deleted_claims": deleted_claims,
            "deleted_claim_versions": deleted_versions,
            "memory_revision": revision,
        }
    )


def apply_forgotten_ledger(store: MemoryStore, ledger: ForgottenLedger) -> int:
    """Reapply current suppressions after restoring an older memory snapshot."""

    entries = ledger.effective_entries()
    changed = 0
    with store.transaction() as connection:
        for entry in entries:
            if entry.target_kind == "source":
                source_rows = connection.execute(
                    "SELECT source_id FROM sources WHERE source_fingerprint = ?",
                    (entry.target_fingerprint,),
                ).fetchall()
                for row in source_rows:
                    source_id = str(row[0])
                    affected_version_ids = {
                        str(version[0])
                        for version in connection.execute(
                            """
                            SELECT DISTINCT ce.claim_version_id
                            FROM claim_evidence AS ce
                            JOIN events AS e ON e.event_id = ce.event_id
                            WHERE e.source_id = ?
                            """,
                            (source_id,),
                        )
                    }
                    candidate_ids = tuple(
                        str(candidate[0])
                        for candidate in connection.execute(
                            """
                            SELECT candidate_id FROM candidate_claims
                            WHERE episode_id IN (
                                SELECT episode_id FROM episodes WHERE source_id = ?
                            )
                            """,
                            (source_id,),
                        )
                    )
                    _purge_review_material(connection, candidate_ids=candidate_ids)
                    connection.execute("DELETE FROM sources WHERE source_id = ?", (source_id,))
                    changed += 1
                    pruned_versions, pruned_claims = _prune_unsupported_versions(
                        connection,
                        candidate_version_ids=affected_version_ids,
                    )
                    changed += pruned_versions + pruned_claims
            elif entry.target_kind == "claim_identity":
                claim_rows = connection.execute(
                    "SELECT claim_id FROM claims WHERE identity_fingerprint = ?",
                    (entry.identity_fingerprint,),
                ).fetchall()
                candidate_ids = tuple(
                    str(candidate[0])
                    for candidate in connection.execute(
                        """
                        SELECT candidate_id FROM candidate_claims
                        WHERE normalized_identity_fingerprint = ?
                        """,
                        (entry.identity_fingerprint,),
                    )
                )
                _purge_review_material(
                    connection,
                    candidate_ids=candidate_ids,
                    target_claim_ids=tuple(str(row[0]) for row in claim_rows),
                    target_slots=(str(entry.identity_fingerprint),),
                )
                if candidate_ids:
                    placeholders = ",".join("?" for _ in candidate_ids)
                    connection.execute(
                        f"DELETE FROM candidate_claims WHERE candidate_id IN ({placeholders})",
                        candidate_ids,
                    )
                for row in claim_rows:
                    connection.execute("DELETE FROM claim_fts WHERE claim_id = ?", (row[0],))
                    connection.execute("DELETE FROM claims WHERE claim_id = ?", (row[0],))
                    changed += 1
            elif entry.target_kind == "claim_exact":
                version_rows = connection.execute(
                    """
                    SELECT cv.claim_version_id, cv.claim_id
                    FROM claim_versions AS cv
                    JOIN claims AS c ON c.claim_id = cv.claim_id
                    WHERE c.identity_fingerprint = ? AND cv.value_fingerprint = ?
                    """,
                    (entry.identity_fingerprint, entry.value_fingerprint),
                ).fetchall()
                candidate_ids = tuple(
                    str(candidate[0])
                    for candidate in connection.execute(
                        """
                        SELECT candidate_id FROM candidate_claims
                        WHERE normalized_identity_fingerprint = ?
                          AND value_fingerprint = ?
                        """,
                        (entry.identity_fingerprint, entry.value_fingerprint),
                    )
                )
                _purge_review_material(
                    connection,
                    candidate_ids=candidate_ids,
                    target_claim_ids=tuple(str(row[1]) for row in version_rows),
                    target_slots=(str(entry.identity_fingerprint),),
                )
                if candidate_ids:
                    placeholders = ",".join("?" for _ in candidate_ids)
                    connection.execute(
                        f"DELETE FROM candidate_claims WHERE candidate_id IN ({placeholders})",
                        candidate_ids,
                    )
                for row in version_rows:
                    connection.execute(
                        "DELETE FROM claim_fts WHERE claim_version_id = ?", (row[0],)
                    )
                    connection.execute(
                        "DELETE FROM claim_versions WHERE claim_version_id = ?", (row[0],)
                    )
                    changed += 1
                exact_claim_ids = {str(row[1]) for row in version_rows}
                for claim_id in exact_claim_ids:
                    deleted = connection.execute(
                        """
                        DELETE FROM claims
                        WHERE claim_id = ? AND NOT EXISTS (
                            SELECT 1 FROM claim_versions WHERE claim_id = ?
                        )
                        """,
                        (claim_id, claim_id),
                    )
                    changed += deleted.rowcount
        if changed:
            bump_memory_revision(
                connection,
                actor="system",
                reason="forgotten ledger reapplied after restore",
                details={"deleted_records": changed},
            )
    return changed

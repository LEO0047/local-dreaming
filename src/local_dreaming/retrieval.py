from __future__ import annotations

import json
import re
import sqlite3
from contextlib import suppress
from pathlib import Path
from typing import Any

from local_dreaming.database import connect_memory, get_memory_revision
from local_dreaming.forgetting import ForgottenLedger
from local_dreaming.storage import OperationsStore


def _fts_expression(query: str) -> str:
    tokens = [token for token in re.findall(r"[^\s\"'():*+-]+", query) if token]
    if not tokens:
        return '""'
    return " OR ".join(f'"{token.replace(chr(34), chr(34) * 2)}"' for token in tokens)


def _row_summary(row: sqlite3.Row, *, mcp: bool) -> str | None:
    sensitivity = str(row["sensitivity"])
    if sensitivity == "secret":
        return None
    if sensitivity == "private" and mcp:
        safe = row["mcp_safe_summary"]
        return str(safe) if safe else None
    return str(row["summary"])


class RetrievalService:
    """Bounded local and MCP retrieval over approved current claim versions."""

    def __init__(self, memory_path: Path, operations_path: Path) -> None:
        self.memory_path = Path(memory_path)
        self.operations = OperationsStore(operations_path)
        self.forgotten = ForgottenLedger(self.memory_path.parent / "forgotten.jsonl")

    def _connection(self) -> sqlite3.Connection:
        return connect_memory(self.memory_path)

    def _suppression_state(
        self,
    ) -> tuple[set[str], set[tuple[str, str]], set[str]]:
        identities: set[str] = set()
        exact: set[tuple[str, str]] = set()
        sources: set[str] = set()
        for entry in self.forgotten.effective_entries():
            if entry.target_kind == "claim_identity" and entry.identity_fingerprint:
                identities.add(entry.identity_fingerprint)
            elif (
                entry.target_kind == "claim_exact"
                and entry.identity_fingerprint
                and entry.value_fingerprint
            ):
                exact.add((entry.identity_fingerprint, entry.value_fingerprint))
            elif entry.target_kind == "source":
                sources.add(entry.target_fingerprint)
        return identities, exact, sources

    @staticmethod
    def _source_suppressed_versions(
        connection: sqlite3.Connection, source_fingerprints: set[str]
    ) -> set[str]:
        if not source_fingerprints:
            return set()
        placeholders = ",".join("?" for _ in source_fingerprints)
        return {
            str(row[0])
            for row in connection.execute(
                f"""
                SELECT DISTINCT ce.claim_version_id
                FROM claim_evidence AS ce
                JOIN events AS e ON e.event_id = ce.event_id
                JOIN sources AS s ON s.source_id = e.source_id
                WHERE s.source_fingerprint IN ({placeholders})
                """,
                tuple(sorted(source_fingerprints)),
            )
        }

    @staticmethod
    def _is_suppressed(
        row: sqlite3.Row,
        identities: set[str],
        exact: set[tuple[str, str]],
        source_versions: set[str],
    ) -> bool:
        identity = str(row["identity_fingerprint"])
        value = str(row["value_fingerprint"])
        return (
            identity in identities
            or (identity, value) in exact
            or str(row["claim_version_id"]) in source_versions
        )

    def _record(self, tool: str, revision: int, items: list[dict[str, Any]]) -> None:
        sensitivities = {str(item.pop("_sensitivity", "normal")) for item in items}
        encoded = json.dumps(items, ensure_ascii=False, sort_keys=True).encode()
        maximum = "private" if "private" in sensitivities else "normal"
        # Practical profile: local usage telemetry must never block retrieval.
        with suppress(OSError, sqlite3.Error, ValueError):
            self.operations.record_retrieval_usage(
                tool_name=tool,
                memory_revision=revision,
                result_count=len(items),
                returned_bytes=len(encoded),
                maximum_sensitivity=maximum,
            )

    @staticmethod
    def _item(row: sqlite3.Row, summary: str) -> dict[str, Any]:
        return {
            "opaque_claim_id": str(row["claim_id"]),
            "opaque_claim_version_id": str(row["claim_version_id"]),
            "subject": str(row["subject_text"]),
            "predicate": str(row["predicate"]),
            "scope": str(row["scope"]),
            "approved_summary": summary,
            "valid_time": {"from": row["valid_from"], "to": row["valid_to"]},
            "recorded_revision": int(row["recorded_revision"]),
            "epistemic_status": str(row["epistemic_status"]),
            "status": str(row["status"]),
            "_sensitivity": str(row["sensitivity"]),
        }

    def _query(
        self,
        *,
        where: str,
        params: tuple[object, ...],
        limit: int,
        mcp: bool,
        fts_query: str | None = None,
        fallback_query: str | None = None,
    ) -> tuple[int, list[dict[str, Any]], bool]:
        if fts_query is None:
            sql = f"""
                SELECT cv.claim_version_id, cv.claim_id, c.subject_text, c.predicate,
                       c.scope, cv.summary, cv.mcp_safe_summary, cv.status,
                       cv.valid_from, cv.valid_to, cv.recorded_revision,
                       cv.epistemic_status, cv.sensitivity,
                       c.identity_fingerprint, cv.value_fingerprint
                FROM current_claim_versions AS cv
                JOIN claims AS c ON c.claim_id = cv.claim_id
                WHERE {where} AND cv.sensitivity <> 'secret'
                ORDER BY COALESCE(cv.valid_from, ''), cv.recorded_revision DESC,
                         c.subject_text, c.predicate, cv.claim_version_id
            """
            arguments = params
        else:
            sql = f"""
                SELECT cv.claim_version_id, cv.claim_id, c.subject_text, c.predicate,
                       c.scope, cv.summary, cv.mcp_safe_summary, cv.status,
                       cv.valid_from, cv.valid_to, cv.recorded_revision,
                       cv.epistemic_status, cv.sensitivity,
                       c.identity_fingerprint, cv.value_fingerprint,
                       bm25(claim_fts) AS rank
                FROM claim_fts
                JOIN current_claim_versions AS cv
                  ON cv.claim_version_id = claim_fts.claim_version_id
                JOIN claims AS c ON c.claim_id = cv.claim_id
                WHERE claim_fts MATCH ? AND {where} AND cv.sensitivity <> 'secret'
                ORDER BY rank, cv.recorded_revision DESC, cv.claim_version_id
            """
            arguments = (fts_query, *params)
        with self._connection() as connection:
            revision = get_memory_revision(connection)
            rows = connection.execute(sql, arguments).fetchall()
            if fts_query is not None and fallback_query and not rows:
                pattern = f"%{fallback_query}%"
                rows = connection.execute(
                    f"""
                    SELECT cv.claim_version_id, cv.claim_id, c.subject_text, c.predicate,
                           c.scope, cv.summary, cv.mcp_safe_summary, cv.status,
                           cv.valid_from, cv.valid_to, cv.recorded_revision,
                           cv.epistemic_status, cv.sensitivity,
                           c.identity_fingerprint, cv.value_fingerprint
                    FROM current_claim_versions AS cv
                    JOIN claims AS c ON c.claim_id = cv.claim_id
                    WHERE {where} AND cv.sensitivity <> 'secret'
                      AND (cv.summary LIKE ? OR c.subject_text LIKE ?
                           OR c.predicate LIKE ? OR c.scope LIKE ?)
                    ORDER BY cv.recorded_revision DESC, cv.claim_version_id
                    """,
                    (*params, pattern, pattern, pattern, pattern),
                ).fetchall()
            identities, exact, source_fingerprints = self._suppression_state()
            source_versions = self._source_suppressed_versions(connection, source_fingerprints)
        items: list[dict[str, Any]] = []
        for row in rows:
            if self._is_suppressed(row, identities, exact, source_versions):
                continue
            summary = _row_summary(row, mcp=mcp)
            if summary is not None:
                items.append(self._item(row, summary))
        return revision, items[:limit], len(items) > limit

    def _envelope(
        self,
        tool: str,
        revision: int,
        items: list[dict[str, Any]],
        *,
        truncated: bool,
    ) -> dict[str, Any]:
        self._record(tool, revision, items)
        for item in items:
            item.pop("_sensitivity", None)
        return {
            "schema_version": 1,
            "memory_revision": revision,
            "items": items,
            "truncated": truncated,
            "next_cursor": None,
        }

    def _search(
        self,
        query: str,
        *,
        limit: int,
        mcp: bool,
        tool: str,
    ) -> dict[str, Any]:
        limit = min(max(limit, 1), 100)
        revision, items, truncated = self._query(
            where="1 = 1",
            params=(),
            limit=limit,
            mcp=mcp,
            fts_query=_fts_expression(query),
            fallback_query=query.strip(),
        )
        return self._envelope(tool, revision, items, truncated=truncated)

    def search(self, query: str, *, limit: int = 10, mcp: bool = False) -> dict[str, Any]:
        return self._search(query, limit=limit, mcp=mcp, tool="memory_search")

    def context(self, query: str, *, limit: int = 12, mcp: bool = False) -> dict[str, Any]:
        result = self._search(query, limit=limit, mcp=mcp, tool="memory_context")
        result["context_kind"] = "bounded"
        return result

    def profile(self, *, mcp: bool = False) -> dict[str, Any]:
        revision, items, truncated = self._query(
            where="c.scope = 'user_profile'",
            params=(),
            limit=100,
            mcp=mcp,
        )
        return self._envelope("memory_get_profile", revision, items, truncated=truncated)

    def active_context(self, *, mcp: bool = False) -> dict[str, Any]:
        revision, items, truncated = self._query(
            where=(
                "c.scope = 'project_state' "
                "AND cv.status IN ('approved', 'disputed', 'outcome_unknown')"
            ),
            params=(),
            limit=100,
            mcp=mcp,
        )
        return self._envelope("memory_get_active_context", revision, items, truncated=truncated)

    def timeline(self, *, limit: int = 30, mcp: bool = False) -> dict[str, Any]:
        limit = min(max(limit, 1), 200)
        revision, items, truncated = self._query(
            where="cv.valid_from IS NOT NULL OR cv.valid_to IS NOT NULL",
            params=(),
            limit=limit,
            mcp=mcp,
        )
        return self._envelope("memory_timeline", revision, items, truncated=truncated)

    def explain(self, claim_id: str, *, mcp: bool = False) -> dict[str, Any]:
        with self._connection() as connection:
            revision = get_memory_revision(connection)
            rows = connection.execute(
                """
                SELECT cv.claim_version_id, cv.claim_id, c.subject_text, c.predicate,
                       c.scope, cv.summary, cv.mcp_safe_summary, cv.status,
                       cv.valid_from, cv.valid_to, cv.recorded_revision,
                       cv.epistemic_status, cv.sensitivity,
                       c.identity_fingerprint, cv.value_fingerprint, ce.evidence_id,
                       ce.evidence_type, ce.captured_at, ce.bounded_excerpt,
                       ce.source_locator, s.source_type AS source_class
                FROM current_claim_versions AS cv
                JOIN claims AS c ON c.claim_id = cv.claim_id
                LEFT JOIN claim_evidence AS ce ON ce.claim_version_id = cv.claim_version_id
                LEFT JOIN events AS e ON e.event_id = ce.event_id
                LEFT JOIN sources AS s ON s.source_id = e.source_id
                WHERE cv.claim_id = ? AND cv.sensitivity <> 'secret'
                ORDER BY cv.recorded_revision DESC, ce.evidence_id
                """,
                (claim_id,),
            ).fetchall()
            identities, exact, source_fingerprints = self._suppression_state()
            source_versions = self._source_suppressed_versions(connection, source_fingerprints)
        items: list[dict[str, Any]] = []
        for row in rows:
            if self._is_suppressed(row, identities, exact, source_versions):
                continue
            summary = _row_summary(row, mcp=mcp)
            if summary is None:
                continue
            item = self._item(row, summary)
            item["evidence"] = {
                "opaque_evidence_id": row["evidence_id"],
                "evidence_type": row["evidence_type"],
                "source_class": row["source_class"],
                "captured_at": row["captured_at"],
            }
            if not mcp:
                item["evidence"].update(
                    {
                        "bounded_excerpt": row["bounded_excerpt"],
                        "source_locator": row["source_locator"],
                    }
                )
            items.append(item)
        return self._envelope("memory_explain", revision, items, truncated=False)

    def history(self, claim_id: str, *, mcp: bool = False) -> dict[str, Any]:
        with self._connection() as connection:
            revision = get_memory_revision(connection)
            rows = connection.execute(
                """
                SELECT cv.claim_version_id, cv.claim_id, c.subject_text, c.predicate,
                       c.scope, cv.summary, cv.mcp_safe_summary, cv.status,
                       cv.valid_from, cv.valid_to, cv.recorded_revision,
                       cv.epistemic_status, cv.sensitivity,
                       c.identity_fingerprint, cv.value_fingerprint
                FROM claim_versions AS cv
                JOIN claims AS c ON c.claim_id = cv.claim_id
                WHERE cv.claim_id = ? AND cv.sensitivity <> 'secret'
                ORDER BY cv.recorded_revision DESC, cv.version_number DESC
                """,
                (claim_id,),
            ).fetchall()
            identities, exact, source_fingerprints = self._suppression_state()
            source_versions = self._source_suppressed_versions(connection, source_fingerprints)
        items: list[dict[str, Any]] = []
        for row in rows:
            if self._is_suppressed(row, identities, exact, source_versions):
                continue
            summary = _row_summary(row, mcp=mcp)
            if summary is not None:
                items.append(self._item(row, summary))
        return self._envelope("memory_history", revision, items, truncated=False)

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, TypedDict

MAX_SEMANTIC_NEIGHBORS = 6
MAX_NEIGHBOR_SUMMARY_CHARS = 1_000
MAX_SCORING_TEXT_CHARS = 6_000

_CONTEXT_VERSION = "semantic-neighbors-v1"
_ASCII_WORD = re.compile(r"[a-z0-9]+")
_CJK_RUN = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]+")
_CONCEPT_ALIASES: Mapping[str, tuple[str, ...]] = {
    "dreaming_context": (
        "dreaming",
        "做夢",
        "記憶功能",
        "長期脈絡",
        "個人脈絡",
        "理解使用者",
        "承接脈絡",
        "未完成工作",
    ),
    "local_runtime": ("本機", "local"),
    "timeline": ("時間軸", "timeline"),
    "handoff_governance": ("agents.md", "dreaming_handoff", "交接", "跨 session"),
}


class _SafeCanonicalRow(TypedDict):
    claim_id: str
    claim_version_id: str
    subject: str
    predicate: str
    scope: str
    summary: str
    status: str
    valid_from: str | None
    valid_to: str | None
    sensitivity: str
    content_policy: str
    recorded_at: str | None
    recorded_revision: int | None


@dataclass(frozen=True, slots=True)
class SemanticNeighbor:
    claim_id: str
    claim_version_id: str
    subject: str
    predicate: str
    scope: str
    summary: str
    status: str
    valid_from: str | None
    valid_to: str | None
    sensitivity: str
    content_policy: str
    recorded_at: str | None
    recorded_revision: int | None
    summary_truncated: bool
    summary_fingerprint: str
    score: int

    def as_payload(self) -> dict[str, object]:
        return asdict(self)

    def binding_payload(self) -> dict[str, object]:
        """Return the immutable fields that bind one transmitted neighbor."""

        return {
            "claim_id": self.claim_id,
            "claim_version_id": self.claim_version_id,
            "content_policy": self.content_policy,
            "predicate": self.predicate,
            "recorded_at": self.recorded_at,
            "recorded_revision": self.recorded_revision,
            "scope": self.scope,
            "score": self.score,
            "sensitivity": self.sensitivity,
            "status": self.status,
            "subject": self.subject,
            "summary_fingerprint": self.summary_fingerprint,
            "summary_truncated": self.summary_truncated,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
        }


@dataclass(frozen=True, slots=True)
class SemanticNeighborContext:
    neighbors: tuple[SemanticNeighbor, ...]
    context_hash: str
    eligible_count: int
    omitted_count: int
    query_truncated: bool
    version: str = _CONTEXT_VERSION

    def as_payload(self) -> dict[str, object]:
        return {
            "context_hash": self.context_hash,
            "eligible_count": self.eligible_count,
            "max_neighbors": MAX_SEMANTIC_NEIGHBORS,
            "neighbors": [neighbor.as_payload() for neighbor in self.neighbors],
            "omitted_count": self.omitted_count,
            "query_truncated": self.query_truncated,
            "version": self.version,
        }


def select_semantic_neighbors(
    *,
    candidate_subject: str,
    candidate_predicate: str,
    candidate_scope: str,
    candidate_value: object,
    canonical_rows: Sequence[Mapping[str, Any]],
    limit: int = MAX_SEMANTIC_NEIGHBORS,
) -> SemanticNeighborContext:
    """Select deterministic cross-slot canonical context without making a decision.

    ``canonical_rows`` may contain normal and private current heads. Secret rows are
    never eligible. A private row is eligible only through its explicitly approved
    ``mcp_safe_summary``; its raw ``summary`` is never scored, returned, or hashed.
    """

    if limit < 1 or limit > MAX_SEMANTIC_NEIGHBORS:
        raise ValueError(f"semantic-neighbor limit must be between 1 and {MAX_SEMANTIC_NEIGHBORS}")

    candidate_slot = (
        _normalize_text(candidate_subject),
        _normalize_text(candidate_predicate),
        _normalize_text(candidate_scope),
    )
    canonical_value = _canonical_json(candidate_value)
    raw_query = "\n".join((candidate_subject, candidate_predicate, canonical_value))
    query_truncated = len(raw_query) > MAX_SCORING_TEXT_CHARS
    query_text = raw_query[:MAX_SCORING_TEXT_CHARS]
    query_features = _features(query_text)

    scored: list[SemanticNeighbor] = []
    for row in canonical_rows:
        safe = _safe_row(row)
        if safe is None:
            continue
        row_slot = (
            _normalize_text(safe["subject"]),
            _normalize_text(safe["predicate"]),
            _normalize_text(safe["scope"]),
        )
        if row_slot == candidate_slot:
            continue
        full_summary = safe["summary"]
        row_text = "\n".join((safe["subject"], safe["predicate"], full_summary))[
            :MAX_SCORING_TEXT_CHARS
        ]
        score = _relatedness_score(
            candidate_slot=candidate_slot,
            row_slot=row_slot,
            query_text=query_text,
            row_text=row_text,
            query_features=query_features,
        )
        if score <= 0:
            continue
        bounded_summary, summary_truncated = _bounded_summary(full_summary)
        scored.append(
            SemanticNeighbor(
                claim_id=safe["claim_id"],
                claim_version_id=safe["claim_version_id"],
                subject=safe["subject"],
                predicate=safe["predicate"],
                scope=safe["scope"],
                summary=bounded_summary,
                status=safe["status"],
                valid_from=safe["valid_from"],
                valid_to=safe["valid_to"],
                sensitivity=safe["sensitivity"],
                content_policy=safe["content_policy"],
                recorded_at=safe["recorded_at"],
                recorded_revision=safe["recorded_revision"],
                summary_truncated=summary_truncated,
                summary_fingerprint=_sha256(full_summary),
                score=score,
            )
        )

    selected = tuple(
        sorted(
            scored,
            key=lambda item: (-item.score, item.claim_id, item.claim_version_id),
        )[:limit]
    )
    candidate_binding = {
        "predicate": candidate_slot[1],
        "scope": candidate_slot[2],
        "subject": candidate_slot[0],
        "value_fingerprint": _sha256(canonical_value),
    }
    context_hash = _sha256(
        _canonical_json(
            {
                "candidate": candidate_binding,
                "max_neighbors": limit,
                "neighbors": [neighbor.binding_payload() for neighbor in selected],
                "version": _CONTEXT_VERSION,
            }
        )
    )
    return SemanticNeighborContext(
        neighbors=selected,
        context_hash=context_hash,
        eligible_count=len(scored),
        omitted_count=max(0, len(scored) - len(selected)),
        query_truncated=query_truncated,
    )


def _safe_row(row: Mapping[str, Any]) -> _SafeCanonicalRow | None:
    sensitivity = str(row.get("sensitivity") or "normal")
    if sensitivity == "secret":
        return None
    raw_safe_summary = row.get("mcp_safe_summary")
    if sensitivity == "private":
        if not isinstance(raw_safe_summary, str) or not raw_safe_summary.strip():
            return None
        summary = raw_safe_summary.strip()
        content_policy = "mcp_safe_summary"
        transmitted_sensitivity = "normal"
    else:
        raw_summary = row.get("summary")
        if not isinstance(raw_summary, str) or not raw_summary.strip():
            return None
        summary = raw_summary.strip()
        content_policy = "canonical_summary"
        transmitted_sensitivity = sensitivity

    return {
        "claim_id": _required_text(row.get("claim_id")),
        "claim_version_id": _required_text(row.get("claim_version_id")),
        "predicate": _required_text(row.get("predicate")),
        "scope": _required_text(row.get("scope")),
        "subject": _required_text(row.get("subject_text", row.get("subject"))),
        "content_policy": content_policy,
        "sensitivity": transmitted_sensitivity,
        "status": str(row.get("status") or "approved"),
        "summary": summary,
        "valid_from": _optional_text(row.get("valid_from")),
        "valid_to": _optional_text(row.get("valid_to")),
        "recorded_at": _optional_text(row.get("recorded_at")),
        "recorded_revision": _optional_int(row.get("recorded_revision")),
    }


def load_semantic_neighbor_context(
    connection: sqlite3.Connection,
    candidate_ids: Sequence[str],
    *,
    limit: int = MAX_SEMANTIC_NEIGHBORS,
) -> SemanticNeighborContext:
    """Load one exact candidate group and its safe cross-slot canonical context."""

    identifiers = tuple(dict.fromkeys(str(item) for item in candidate_ids))
    if not identifiers:
        raise ValueError("semantic context requires at least one candidate")
    placeholders = ",".join("?" for _ in identifiers)
    candidate_rows = connection.execute(
        f"""
        SELECT candidate_id, subject_text, predicate, scope, value_json,
               normalized_identity_fingerprint
        FROM candidate_claims
        WHERE candidate_id IN ({placeholders})
        ORDER BY candidate_id
        """,
        identifiers,
    ).fetchall()
    if len(candidate_rows) != len(identifiers):
        raise ValueError("semantic context references an unknown candidate")
    identities = {str(row["normalized_identity_fingerprint"]) for row in candidate_rows}
    if len(identities) != 1:
        raise ValueError("semantic context candidates must share one exact slot")
    first = candidate_rows[0]
    canonical_rows = connection.execute(
        """
        SELECT c.claim_id, cv.claim_version_id, c.subject_text, c.predicate,
               c.scope, cv.summary, cv.mcp_safe_summary, cv.sensitivity,
               cv.status, cv.valid_from, cv.valid_to, cv.recorded_at,
               cv.recorded_revision
        FROM current_claim_versions AS cv
        JOIN claims AS c ON c.claim_id = cv.claim_id
        ORDER BY c.claim_id, cv.claim_version_id
        """
    ).fetchall()
    candidate_values = [json.loads(str(row["value_json"])) for row in candidate_rows]
    return select_semantic_neighbors(
        candidate_subject=str(first["subject_text"]),
        candidate_predicate=str(first["predicate"]),
        candidate_scope=str(first["scope"]),
        candidate_value=candidate_values,
        canonical_rows=[dict(row) for row in canonical_rows],
        limit=limit,
    )


def _relatedness_score(
    *,
    candidate_slot: tuple[str, str, str],
    row_slot: tuple[str, str, str],
    query_text: str,
    row_text: str,
    query_features: frozenset[str],
) -> int:
    row_features = _features(row_text)
    overlap = query_features & row_features
    union = query_features | row_features
    lexical = (10_000 * len(overlap)) // max(1, len(union))
    score = lexical
    if candidate_slot[0] == row_slot[0]:
        score += 3_000
    if candidate_slot[1] == row_slot[1]:
        score += 3_500
    if score > 0 and candidate_slot[2] == row_slot[2]:
        score += 400
    normalized_query = _compact_text(query_text)
    normalized_row = _compact_text(row_text)
    if min(len(normalized_query), len(normalized_row)) >= 4 and (
        normalized_query in normalized_row or normalized_row in normalized_query
    ):
        score += 1_500
    return score


def _features(value: str) -> frozenset[str]:
    normalized = _normalize_text(value)
    features = {f"w:{word}" for word in _ASCII_WORD.findall(normalized) if len(word) >= 2}
    for run in _CJK_RUN.findall(normalized):
        for size in (2, 3):
            features.update(
                f"c{size}:{run[index : index + size]}" for index in range(len(run) - size + 1)
            )
    compact = _compact_text(normalized)
    for concept, aliases in _CONCEPT_ALIASES.items():
        if any(_compact_text(alias) in compact for alias in aliases):
            features.add(f"concept:{concept}")
    return frozenset(features)


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(normalized.split())


def _compact_text(value: str) -> str:
    return "".join(character for character in _normalize_text(value) if character.isalnum())


def _bounded_summary(value: str) -> tuple[str, bool]:
    if len(value) <= MAX_NEIGHBOR_SUMMARY_CHARS:
        return value, False
    return value[: MAX_NEIGHBOR_SUMMARY_CHARS - 1] + "…", True


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool | int):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise ValueError("canonical recorded_revision must be an integer")


def _required_text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("canonical semantic rows require non-empty claim and slot fields")
    return value.strip()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

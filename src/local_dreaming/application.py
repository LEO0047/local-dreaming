from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from local_dreaming.database import bump_memory_revision, canonical_json, utc_now
from local_dreaming.errors import StaleProposalError
from local_dreaming.forgetting import ForgottenLedger
from local_dreaming.models import ClaimHead
from local_dreaming.redaction import has_secret_material
from local_dreaming.review import fingerprint_head_set, proposal_evidence_binding
from local_dreaming.semantic_neighbors import load_semantic_neighbor_context
from local_dreaming.storage import (
    MemoryStore,
    fingerprint,
    identity_fingerprint,
    new_id,
    value_fingerprint,
)


def _current_heads(connection: sqlite3.Connection, identity: str) -> list[ClaimHead]:
    rows = connection.execute(
        """
        SELECT cv.claim_version_id, cv.value_fingerprint, cv.status,
               cv.valid_from, cv.valid_to
        FROM claims AS c
        JOIN current_claim_versions AS cv ON cv.claim_id = c.claim_id
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


def _require_payload(payload: Mapping[str, Any]) -> None:
    required = {"subject_text", "predicate", "scope", "value", "summary"}
    missing = required - payload.keys()
    if missing:
        raise ValueError(f"proposal payload is missing: {', '.join(sorted(missing))}")
    if payload.get("sensitivity", "normal") == "secret" or has_secret_material(payload):
        raise ValueError("secret proposal cannot become canonical memory")


def _canonical_status(operation: str, payload: Mapping[str, Any]) -> str:
    requested = payload.get("canonical_status")
    if operation == "dispute":
        if requested not in {None, "disputed"}:
            raise ValueError("DISPUTE proposal has an incompatible canonical status")
        return "disputed"
    if requested in {None, "approved"}:
        return "approved"
    if requested == "outcome_unknown":
        if operation not in {"update", "supersede", "narrow"}:
            raise ValueError("outcome_unknown requires an existing claim update")
        if payload.get("epistemic_status") != "outcome_unknown":
            raise ValueError("outcome_unknown status requires matching epistemic status")
        if not payload.get("valid_to"):
            raise ValueError("outcome_unknown requires an expired validity boundary")
        return "outcome_unknown"
    raise ValueError("unsupported canonical status in proposal payload")


class CanonicalApplicationService:
    """Apply a fully approved review batch in one canonical transaction."""

    def __init__(self, memory_path: Path, forgotten_path: Path | None = None) -> None:
        self.store = MemoryStore(memory_path)
        self.forgotten = ForgottenLedger(
            forgotten_path or Path(memory_path).parent / "forgotten.jsonl"
        )

    def apply_batch(self, batch_id: str, *, actor: str = "leo") -> tuple[int, tuple[str, ...]]:
        with self.store.transaction() as connection:
            batch = connection.execute(
                "SELECT * FROM review_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise KeyError(f"unknown review batch: {batch_id}")
            if batch["status"] != "approved":
                raise ValueError(f"review batch is {batch['status']}, not approved")
            proposals = connection.execute(
                """
                SELECT * FROM review_proposals
                WHERE batch_id = ? ORDER BY created_at, proposal_id
                """,
                (batch_id,),
            ).fetchall()
            invalid_status = any(
                row["status"] not in {"approved", "corrected"} for row in proposals
            )
            if not proposals or invalid_status:
                raise ValueError("review batch does not contain only approved decisions")

            decoded: list[tuple[sqlite3.Row, dict[str, Any], list[ClaimHead], tuple[str, ...]]] = []
            for row in proposals:
                payload = json.loads(str(row["payload_json"]))
                if not isinstance(payload, dict):
                    raise ValueError("proposal payload must be an object")
                _require_payload(payload)
                if fingerprint(payload) != row["proposal_payload_fingerprint"]:
                    raise ValueError("proposal payload fingerprint mismatch")
                evidence_ids = payload.get("evidence_event_ids", [])
                if not isinstance(evidence_ids, Sequence) or isinstance(evidence_ids, str | bytes):
                    raise ValueError("evidence_event_ids must be a list")
                normalized_evidence_ids = tuple(str(value) for value in evidence_ids)
                if (
                    fingerprint(list(proposal_evidence_binding(payload)))
                    != row["evidence_set_fingerprint"]
                ):
                    raise ValueError("proposal evidence fingerprint mismatch")
                expected_semantic_hash = payload.get("semantic_context_hash")
                if isinstance(expected_semantic_hash, str) and expected_semantic_hash:
                    candidate_ids = payload.get("candidate_ids")
                    if not isinstance(candidate_ids, Sequence) or isinstance(
                        candidate_ids, str | bytes
                    ):
                        raise ValueError("proposal candidate_ids must be a list")
                    current_semantic_hash = load_semantic_neighbor_context(
                        connection, tuple(str(item) for item in candidate_ids)
                    ).context_hash
                    if current_semantic_hash != expected_semantic_hash:
                        raise StaleProposalError(
                            f"proposal {row['proposal_id']} semantic context changed"
                        )
                identity = identity_fingerprint(
                    str(payload["subject_text"]),
                    str(payload["predicate"]),
                    str(payload["scope"]),
                )
                if identity != row["target_slot_fingerprint"]:
                    raise ValueError("proposal payload identity does not match target slot")
                if self.forgotten.is_claim_forgotten(identity, value_fingerprint(payload["value"])):
                    raise ValueError("approved proposal is suppressed by forgotten.jsonl")
                heads = _current_heads(connection, identity)
                current_hash = fingerprint_head_set(heads)
                if current_hash != row["expected_head_set_hash"]:
                    raise StaleProposalError(
                        f"proposal {row['proposal_id']} target head set changed"
                    )
                decoded.append((row, payload, heads, normalized_evidence_ids))

            revision = bump_memory_revision(
                connection,
                actor=actor,
                reason="approved review batch applied",
                details={"batch_id": batch_id, "proposal_count": len(decoded)},
            )
            now = utc_now()
            version_ids: list[str] = []
            for row, payload, heads, evidence_ids in decoded:
                identity = str(row["target_slot_fingerprint"])
                existing = connection.execute(
                    "SELECT claim_id FROM claims WHERE identity_fingerprint = ?", (identity,)
                ).fetchone()
                claim_id = str(existing[0]) if existing else new_id("claim")
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO claims(
                            claim_id, subject_entity_id, subject_text, predicate, scope,
                            identity_fingerprint, created_revision, created_at
                        ) VALUES (?, NULL, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            claim_id,
                            str(payload["subject_text"]),
                            str(payload["predicate"]),
                            str(payload["scope"]),
                            identity,
                            revision,
                            now,
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
                version_id = new_id("cv")
                operation = str(row["proposal_type"])
                status = _canonical_status(operation, payload)
                value = payload["value"]
                value_fp = value_fingerprint(value)
                sensitivity = str(payload.get("sensitivity", "normal"))
                connection.execute(
                    """
                    INSERT INTO claim_versions(
                        claim_version_id, claim_id, version_number, value_json,
                        value_fingerprint, summary, mcp_safe_summary, status,
                        valid_from, valid_to, recorded_revision, recorded_at,
                        superseded_at, confidence, epistemic_status, sensitivity,
                        provenance_kind
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, 'model_proposal')
                    """,
                    (
                        version_id,
                        claim_id,
                        version_number,
                        canonical_json(value),
                        value_fp,
                        str(payload["summary"]),
                        payload.get("mcp_safe_summary"),
                        status,
                        payload.get("valid_from"),
                        payload.get("valid_to"),
                        revision,
                        now,
                        float(payload.get("confidence", 0.8)),
                        str(payload.get("epistemic_status", "provisional")),
                        sensitivity,
                    ),
                )
                relation_type = {
                    "update": "supersedes",
                    "supersede": "supersedes",
                    "dispute": "contradicts",
                    "narrow": "narrows",
                }.get(operation)
                if relation_type:
                    for head in heads:
                        connection.execute(
                            """
                            INSERT INTO claim_relations(
                                relation_id, from_claim_version_id, to_claim_version_id,
                                relation_type, created_revision, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                new_id("rel"),
                                version_id,
                                head.claim_version_id,
                                relation_type,
                                revision,
                                now,
                            ),
                        )
                for event_id in dict.fromkeys(evidence_ids):
                    event = connection.execute(
                        """
                        SELECT content_text, source_locator, captured_at,
                               parser_version, redactor_version, content_fingerprint
                        FROM events WHERE event_id = ?
                        """,
                        (event_id,),
                    ).fetchone()
                    if event is None:
                        raise ValueError(f"unknown evidence event: {event_id}")
                    connection.execute(
                        """
                        INSERT INTO claim_evidence(
                            evidence_id, claim_version_id, event_id, evidence_type,
                            bounded_excerpt, source_locator, captured_at, parser_version,
                            redactor_version, evidence_fingerprint
                        ) VALUES (?, ?, ?, 'candidate_evidence', ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            new_id("evd"),
                            version_id,
                            event_id,
                            str(event["content_text"] or "")[:500] or None,
                            event["source_locator"],
                            event["captured_at"],
                            event["parser_version"],
                            event["redactor_version"],
                            event["content_fingerprint"],
                        ),
                    )
                connection.execute(
                    """
                    INSERT INTO claim_fts(
                        claim_version_id, claim_id, subject, predicate, scope, summary
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        version_id,
                        claim_id,
                        str(payload["subject_text"]),
                        str(payload["predicate"]),
                        str(payload["scope"]),
                        str(payload["summary"]),
                    ),
                )
                connection.execute(
                    "UPDATE review_proposals SET status = 'applied' WHERE proposal_id = ?",
                    (row["proposal_id"],),
                )
                version_ids.append(version_id)
            connection.execute(
                "UPDATE review_batches SET status = 'applied' WHERE batch_id = ?", (batch_id,)
            )
            return revision, tuple(version_ids)

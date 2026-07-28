from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from local_dreaming.models import ProposalType
from local_dreaming.review import fingerprint_head_set, fingerprint_precondition
from local_dreaming.storage import MemoryStore, ReviewProposalInput, fingerprint

_LOCAL_TIMEZONE = ZoneInfo("Asia/Taipei")


@dataclass(frozen=True, slots=True)
class TemporalReviewResult:
    proposal_count: int
    batch_id: str | None
    claim_ids: tuple[str, ...]
    dry_run: bool


def _is_expired(value: str, now: datetime) -> bool:
    normalized = value.strip()
    if not normalized:
        return False
    if "T" not in normalized and " " not in normalized:
        try:
            return date.fromisoformat(normalized) < now.astimezone(_LOCAL_TIMEZONE).date()
        except ValueError:
            return False
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=_LOCAL_TIMEZONE)
    return parsed.astimezone(UTC) < now.astimezone(UTC)


def propose_expired_plan_outcomes(
    store: MemoryStore,
    *,
    now: datetime | None = None,
    dry_run: bool = False,
) -> TemporalReviewResult:
    """Propose outcome_unknown for expired plan-like heads; never apply it."""

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    records: list[ReviewProposalInput] = []
    claim_ids: list[str] = []
    with store.maintenance_lock():
        with store.connection() as connection:
            rows = connection.execute(
                """
                SELECT cv.claim_version_id, cv.claim_id, cv.value_json,
                       cv.value_fingerprint, cv.summary, cv.valid_from, cv.valid_to,
                       cv.confidence, cv.sensitivity, c.subject_text, c.predicate,
                       c.scope, c.identity_fingerprint
                FROM current_claim_versions AS cv
                JOIN claims AS c ON c.claim_id = cv.claim_id
                WHERE cv.status = 'approved'
                  AND c.scope = 'project_state'
                  AND cv.valid_to IS NOT NULL
                  AND (
                      lower(c.predicate) LIKE 'plan.%'
                      OR lower(c.predicate) LIKE '%.plan.%'
                      OR lower(c.predicate) LIKE '%deadline%'
                  )
                ORDER BY c.identity_fingerprint, cv.claim_version_id
                """
            ).fetchall()
            for row in rows:
                valid_to = str(row["valid_to"])
                if not _is_expired(valid_to, current_time):
                    continue
                identity = str(row["identity_fingerprint"])
                heads = store.load_heads(identity)
                expected_hash = fingerprint_head_set(heads)
                already_proposed = connection.execute(
                    """
                    SELECT payload_json FROM review_proposals
                    WHERE target_slot_fingerprint = ?
                      AND expected_head_set_hash = ?
                      AND proposal_type = 'update'
                    """,
                    (identity, expected_hash),
                ).fetchall()
                if any(
                    json.loads(str(item[0])).get("canonical_status") == "outcome_unknown"
                    for item in already_proposed
                ):
                    continue
                summary = str(row["summary"])
                suffix = "（期限已過，結果尚未確認）"
                payload = {
                    "subject_text": str(row["subject_text"]),
                    "predicate": str(row["predicate"]),
                    "scope": str(row["scope"]),
                    "value": json.loads(str(row["value_json"])),
                    "summary": summary if summary.endswith(suffix) else summary + suffix,
                    "valid_from": row["valid_from"],
                    "valid_to": row["valid_to"],
                    "confidence": float(row["confidence"]),
                    "epistemic_status": "outcome_unknown",
                    "canonical_status": "outcome_unknown",
                    "sensitivity": str(row["sensitivity"]),
                    "evidence_event_ids": [],
                    "rationale": "The plan validity boundary passed without outcome evidence.",
                }
                records.append(
                    ReviewProposalInput(
                        proposal_type=ProposalType.UPDATE.value,
                        target_slot_fingerprint=identity,
                        expected_head_set_hash=expected_hash,
                        precondition_hash=fingerprint_precondition(
                            proposal_type=ProposalType.UPDATE,
                            target_slot_fingerprint=identity,
                            expected_head_set_hash=expected_hash,
                        ),
                        proposal_payload_fingerprint=fingerprint(payload),
                        evidence_set_fingerprint=fingerprint([]),
                        payload=payload,
                        target_claim_id=str(row["claim_id"]),
                    )
                )
                claim_ids.append(str(row["claim_id"]))
        batch_id = None if dry_run or not records else store.create_review_batch(records)
    return TemporalReviewResult(
        proposal_count=len(records),
        batch_id=batch_id,
        claim_ids=tuple(claim_ids),
        dry_run=dry_run,
    )

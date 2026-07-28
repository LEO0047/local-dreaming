from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

MAX_MODEL_INPUT_CHARS = 12_000
_ADJACENT_DUPLICATE_SECONDS = 1.0
_RECORD_NUMBER = re.compile(r"(?:^|:)record=(\d+)(?::|$)")


def effective_evidence_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collapse adjacent duplicate observations within one source lineage.

    Content equality alone is intentionally insufficient.  A duplicate family is
    scoped to source, partition, role, and source order, then bounded to records
    observed within one second.  This preserves repeated statements made later
    and identical text observed from a different source.
    """

    materialized = [dict(row) for row in rows]
    by_lineage: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in materialized:
        by_lineage[_lineage_key(row)].append(row)

    representative_by_event: dict[str, str] = {}
    family_by_event: dict[str, str] = {}
    for lineage, lineage_rows in by_lineage.items():
        previous: dict[str, Any] | None = None
        previous_representative: str | None = None
        for row in sorted(lineage_rows, key=_source_order_key):
            event_id = str(row["event_id"])
            same_family = (
                previous is not None
                and str(previous.get("content_fingerprint", ""))
                == str(row.get("content_fingerprint", ""))
                and _within_duplicate_window(previous, row)
            )
            if same_family:
                assert previous_representative is not None
                representative = previous_representative
            else:
                representative = event_id
            family = _family_fingerprint(
                lineage,
                str(row.get("content_fingerprint") or ""),
                representative,
            )
            representative_by_event[event_id] = representative
            family_by_event[event_id] = family
            previous = row
            previous_representative = representative

    selected: list[dict[str, Any]] = []
    seen_families: set[str] = set()
    for row in materialized:
        event_id = str(row["event_id"])
        family = family_by_event[event_id]
        if family in seen_families:
            continue
        seen_families.add(family)
        representative = representative_by_event[event_id]
        representative_row = next(
            item for item in materialized if str(item["event_id"]) == representative
        )
        selected.append({**representative_row, "evidence_family_fingerprint": family})
    return selected


def effective_evidence_fingerprints(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    return tuple(str(row["evidence_family_fingerprint"]) for row in effective_evidence_rows(rows))


def _lineage_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    metadata = _metadata(row)
    role = str(metadata.get("role") or row.get("event_type") or "unknown")
    return (
        str(row.get("source_id") or ""),
        str(row.get("partition_id") or ""),
        role,
    )


def _metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = row.get("metadata_json", row.get("metadata", {}))
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _source_order_key(row: Mapping[str, Any]) -> tuple[int, str, str]:
    metadata = _metadata(row)
    sequence = metadata.get("source_sequence")
    if isinstance(sequence, int) and sequence >= 0:
        return (sequence, str(row.get("occurred_at") or ""), str(row["event_id"]))
    locator = str(row.get("source_locator") or "")
    match = _RECORD_NUMBER.search(locator)
    if match is not None:
        return (int(match.group(1)), str(row.get("occurred_at") or ""), str(row["event_id"]))
    ordinal = row.get("ordinal")
    return (
        int(ordinal) if isinstance(ordinal, int) and ordinal >= 0 else 2**63 - 1,
        str(row.get("occurred_at") or ""),
        str(row["event_id"]),
    )


def _within_duplicate_window(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_time = _parse_time(left.get("occurred_at"))
    right_time = _parse_time(right.get("occurred_at"))
    if left_time is None or right_time is None:
        return False
    delta = (right_time - left_time).total_seconds()
    return 0.0 <= delta <= _ADJACENT_DUPLICATE_SECONDS


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _family_fingerprint(
    lineage: tuple[str, str, str],
    content_fingerprint: str,
    representative: str,
) -> str:
    payload = json.dumps(
        {
            "content_fingerprint": content_fingerprint,
            "lineage": lineage,
            "representative_event_id": representative,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()

from __future__ import annotations

from local_dreaming.evidence import effective_evidence_rows


def _row(
    event_id: str,
    *,
    source_id: str = "source-a",
    partition_id: str = "partition-a",
    content_fingerprint: str = "same-content",
    occurred_at: str = "2026-07-20T03:01:00.001Z",
    source_sequence: int = 1,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "source_id": source_id,
        "partition_id": partition_id,
        "event_type": "codex_task",
        "content_fingerprint": content_fingerprint,
        "occurred_at": occurred_at,
        "ordinal": source_sequence,
        "metadata": {"role": "user", "source_sequence": source_sequence},
    }


def test_effective_evidence_collapses_only_adjacent_same_lineage() -> None:
    rows = [
        _row("event-1", source_sequence=10),
        _row(
            "event-2",
            occurred_at="2026-07-20T03:01:00.018Z",
            source_sequence=11,
        ),
        _row(
            "event-3",
            occurred_at="2026-07-20T03:03:00Z",
            source_sequence=20,
        ),
    ]

    effective = effective_evidence_rows(rows)

    assert [row["event_id"] for row in effective] == ["event-1", "event-3"]
    assert len({row["evidence_family_fingerprint"] for row in effective}) == 2


def test_effective_evidence_keeps_identical_text_from_different_sources() -> None:
    effective = effective_evidence_rows(
        [
            _row("event-a", source_id="source-a", partition_id="partition-a"),
            _row("event-b", source_id="source-b", partition_id="partition-b"),
        ]
    )

    assert [row["event_id"] for row in effective] == ["event-a", "event-b"]

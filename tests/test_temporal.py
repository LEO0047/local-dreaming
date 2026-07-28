from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from local_dreaming.application import CanonicalApplicationService
from local_dreaming.review import ReviewService
from local_dreaming.storage import ClaimVersionInput, MemoryStore
from local_dreaming.temporal import propose_expired_plan_outcomes


def test_expired_plan_proposes_unknown_outcome_and_still_requires_approval(
    tmp_path: Path,
) -> None:
    memory = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory)
    claim_id, _, revision = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="plan.deadline",
            scope="project_state",
            value={"project": "Local-Dreaming"},
            summary="預計完成 Local-Dreaming",
            valid_to="2026-07-18",
        )
    )

    dry_run = propose_expired_plan_outcomes(
        store, now=datetime(2026, 7, 19, 12, tzinfo=UTC), dry_run=True
    )
    assert dry_run.proposal_count == 1
    assert store.current_revision() == revision

    created = propose_expired_plan_outcomes(store, now=datetime(2026, 7, 19, 12, tzinfo=UTC))
    assert created.batch_id is not None
    assert store.current_revision() == revision
    assert (
        propose_expired_plan_outcomes(
            store, now=datetime(2026, 7, 20, 12, tzinfo=UTC)
        ).proposal_count
        == 0
    )

    ReviewService(store).record_approve_all(created.batch_id)
    new_revision, _ = CanonicalApplicationService(memory).apply_batch(created.batch_id)
    assert new_revision == revision + 1
    with store.connection() as connection:
        status = connection.execute(
            """
            SELECT cv.status FROM current_claim_versions AS cv
            WHERE cv.claim_id = ?
            """,
            (claim_id,),
        ).fetchone()[0]
    assert status == "outcome_unknown"


def test_non_plan_or_future_claim_is_not_reclassified(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory)
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project.completed",
            scope="project_state",
            value=True,
            summary="已完成",
            valid_to="2026-07-18",
        )
    )
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="plan.deadline",
            scope="project_state",
            value="future",
            summary="未來期限",
            valid_to="2026-07-21",
        )
    )

    result = propose_expired_plan_outcomes(
        store, now=datetime(2026, 7, 19, 12, tzinfo=UTC), dry_run=True
    )

    assert result.proposal_count == 0

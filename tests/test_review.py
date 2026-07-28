from __future__ import annotations

from pathlib import Path

import pytest

from local_dreaming.errors import StaleProposalError
from local_dreaming.models import ClaimHead, ProposalType, ReviewProposal
from local_dreaming.review import (
    ReviewService,
    assert_review_batch_fresh,
    fingerprint_head_set,
    fingerprint_precondition,
    fingerprint_target_slot,
    validate_proposal,
    validate_review_batch,
)
from local_dreaming.storage import (
    ClaimVersionInput,
    MemoryStore,
    ReviewProposalInput,
    fingerprint,
    identity_fingerprint,
)


def _head(version: str, value: str) -> ClaimHead:
    return ClaimHead(
        claim_version_id=version,
        value_fingerprint=value,
        status="approved",
        valid_from="2026-01-01",
    )


def _proposal(
    proposal_type: ProposalType,
    expected_heads: tuple[ClaimHead, ...],
    *,
    proposal_id: str = "proposal-1",
    slot: str | None = None,
) -> ReviewProposal:
    target_slot = slot or fingerprint_target_slot(
        subject_text="Leo",
        predicate="preferred_language",
        scope="user_profile",
    )
    expected_hash = fingerprint_head_set(expected_heads)
    return ReviewProposal(
        proposal_id=proposal_id,
        batch_id="batch-1",
        proposal_type=proposal_type,
        base_memory_revision=2,
        target_slot_fingerprint=target_slot,
        expected_head_set_hash=expected_hash,
        precondition_hash=fingerprint_precondition(
            proposal_type=proposal_type,
            target_slot_fingerprint=target_slot,
            expected_head_set_hash=expected_hash,
        ),
        proposal_payload_fingerprint="payload",
        evidence_set_fingerprint="evidence",
    )


def test_unrelated_revision_change_does_not_stale_proposal() -> None:
    heads = (_head("v1", "traditional-chinese"),)
    proposal = _proposal(ProposalType.UPDATE, heads)

    validation = validate_proposal(proposal, heads, current_memory_revision=99)

    assert validation.fresh
    assert validation.reason is None


def test_any_multi_head_change_stales_proposal() -> None:
    expected = (_head("v1", "taipei"), _head("v2", "perth"))
    current = (_head("v1", "taipei"), _head("v3", "sydney"))
    proposal = _proposal(ProposalType.DISPUTE, expected)

    validation = validate_proposal(proposal, current, current_memory_revision=3)

    assert not validation.fresh
    assert validation.reason == "target_head_set_changed"
    with pytest.raises(StaleProposalError, match="proposal-1"):
        assert_review_batch_fresh(
            [proposal],
            {proposal.target_slot_fingerprint: current},
            current_memory_revision=3,
        )


def test_add_requires_target_to_remain_empty() -> None:
    proposal = _proposal(ProposalType.ADD, ())

    assert validate_proposal(proposal, (), current_memory_revision=2).fresh
    stale = validate_proposal(proposal, (_head("v1", "exists"),), current_memory_revision=3)
    assert not stale.fresh
    assert stale.reason == "add_target_is_no_longer_empty"


def test_batch_rejects_overlapping_target_slots_before_application() -> None:
    first = _proposal(ProposalType.ADD, (), proposal_id="one")
    second = _proposal(ProposalType.ADD, (), proposal_id="two")

    with pytest.raises(ValueError, match="same target slot"):
        validate_review_batch(
            [first, second],
            {first.target_slot_fingerprint: ()},
            current_memory_revision=2,
        )


def test_target_slot_matches_storage_identity_and_persisted_batch_goes_stale(
    tmp_path: Path,
) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preferred_language",
            scope="user_profile",
            value="Traditional Chinese",
            summary="Leo prefers Traditional Chinese.",
        )
    )
    slot = fingerprint_target_slot(
        subject_text="Leo",
        predicate="preferred_language",
        scope="user_profile",
    )
    assert slot == identity_fingerprint("Leo", "preferred_language", "user_profile")
    heads = tuple(store.load_heads(slot))
    proposal = _proposal(ProposalType.UPDATE, heads, slot=slot)
    payload = {"summary": "Leo prefers English.", "evidence_event_ids": []}
    batch_id = store.create_review_batch(
        [
            ReviewProposalInput(
                proposal_id=proposal.proposal_id,
                proposal_type=proposal.proposal_type.value,
                target_slot_fingerprint=proposal.target_slot_fingerprint,
                expected_head_set_hash=proposal.expected_head_set_hash,
                precondition_hash=proposal.precondition_hash,
                proposal_payload_fingerprint=fingerprint(payload),
                evidence_set_fingerprint=fingerprint([]),
                payload=payload,
            )
        ],
        base_memory_revision=store.current_revision(),
        batch_id=proposal.batch_id,
    )
    service = ReviewService(store)
    assert service.validate_batch(batch_id).fresh

    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preferred_language",
            scope="user_profile",
            value="Traditional Chinese (Taiwan)",
            summary="Leo corrected the preference to Traditional Chinese (Taiwan).",
        )
    )

    with pytest.raises(StaleProposalError, match="target_head_set_changed"):
        service.record_approve_all(batch_id)
    stored = store.load_review_batch(batch_id)
    assert stored["status"] == "stale"
    assert stored["proposals"][0]["status"] == "stale"


@pytest.mark.parametrize("mismatch", ["payload", "evidence"])
def test_approval_rejects_initially_inconsistent_proposal_binding(
    tmp_path: Path, mismatch: str
) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    slot = fingerprint_target_slot(
        subject_text="Leo",
        predicate="project.status",
        scope="project_state",
    )
    expected = fingerprint_head_set(())
    payload = {
        "subject_text": "Leo",
        "predicate": "project.status",
        "scope": "project_state",
        "value": "planned",
        "summary": "Project is planned.",
        "evidence_event_ids": [],
    }
    batch_id = store.create_review_batch(
        [
            ReviewProposalInput(
                proposal_type="add",
                target_slot_fingerprint=slot,
                expected_head_set_hash=expected,
                precondition_hash=fingerprint_precondition(
                    proposal_type=ProposalType.ADD,
                    target_slot_fingerprint=slot,
                    expected_head_set_hash=expected,
                ),
                proposal_payload_fingerprint=(
                    "mismatched" if mismatch == "payload" else fingerprint(payload)
                ),
                evidence_set_fingerprint=(
                    "mismatched" if mismatch == "evidence" else fingerprint([])
                ),
                payload=payload,
            )
        ]
    )

    with pytest.raises(ValueError, match=f"proposal {mismatch} fingerprint mismatch"):
        ReviewService(store).record_approve_all(batch_id)

    stored = store.load_review_batch(batch_id)
    assert stored["status"] == "pending"
    assert stored["proposals"][0]["status"] == "pending"

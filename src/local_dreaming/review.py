from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Protocol, cast

from local_dreaming.errors import StaleProposalError
from local_dreaming.models import (
    ClaimHead,
    ProposalStatus,
    ProposalType,
    ReviewProposal,
    ReviewValidation,
)


def fingerprint_target_slot(
    *,
    subject_text: str,
    predicate: str,
    scope: str,
) -> str:
    """Match the canonical claim identity used by ``MemoryStore``.

    Valid intervals belong to immutable claim versions. The optimistic-lock
    slot is the logical subject/predicate/scope identity so a concurrent ADD at
    any interval cannot be overlooked.
    """

    return _canonical_hash(
        [
            "claim-identity-v1",
            _normalize_text(subject_text),
            _normalize_text(predicate),
            _normalize_text(scope),
        ]
    )


def fingerprint_head_set(heads: Iterable[ClaimHead]) -> str:
    canonical_heads = sorted(
        (
            {
                "claim_version_id": head.claim_version_id,
                "status": head.status,
                "valid_from": head.valid_from,
                "valid_to": head.valid_to,
                "value_fingerprint": head.value_fingerprint,
            }
            for head in heads
        ),
        key=lambda item: (
            item["claim_version_id"] or "",
            item["valid_from"] or "",
            item["valid_to"] or "",
        ),
    )
    return _canonical_hash(canonical_heads)


def fingerprint_precondition(
    *,
    proposal_type: ProposalType,
    target_slot_fingerprint: str,
    expected_head_set_hash: str,
) -> str:
    return _canonical_hash(
        {
            "expected_head_set_hash": expected_head_set_hash,
            "proposal_type": proposal_type.value,
            "target_slot_fingerprint": target_slot_fingerprint,
        }
    )


def validate_proposal_binding(row: Mapping[str, object]) -> None:
    """Recompute the persisted payload and evidence bindings before review decisions."""

    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("proposal payload must be an object")
    if _storage_fingerprint(payload) != str(row.get("proposal_payload_fingerprint", "")):
        raise ValueError("proposal payload fingerprint mismatch")
    if _storage_fingerprint(list(proposal_evidence_binding(payload))) != str(
        row.get("evidence_set_fingerprint", "")
    ):
        raise ValueError("proposal evidence fingerprint mismatch")


def proposal_evidence_binding(payload: Mapping[str, object]) -> tuple[str, ...]:
    """Return the immutable effective-evidence binding for a proposal.

    New Phase 2 proposals bind effective evidence families so duplicate source
    records cannot inflate or change their evidence identity. Legacy/manual
    proposals predate that field and remain bound to their event IDs.
    """

    family_values = payload.get("evidence_family_fingerprints")
    if family_values is not None:
        if not isinstance(family_values, Sequence) or isinstance(family_values, str | bytes):
            raise ValueError("evidence_family_fingerprints must be a list")
        return tuple(str(value) for value in family_values)
    evidence_ids = payload.get("evidence_event_ids", [])
    if not isinstance(evidence_ids, Sequence) or isinstance(evidence_ids, str | bytes):
        raise ValueError("evidence_event_ids must be a list")
    return tuple(str(value) for value in evidence_ids)


def validate_proposal(
    proposal: ReviewProposal,
    current_heads: Sequence[ClaimHead],
    *,
    current_memory_revision: int,
) -> ReviewValidation:
    current_hash = fingerprint_head_set(current_heads)
    if proposal.status is not ProposalStatus.PENDING:
        return ReviewValidation(
            proposal_id=proposal.proposal_id,
            fresh=False,
            current_head_set_hash=current_hash,
            reason="proposal_not_pending",
        )
    if proposal.base_memory_revision > current_memory_revision:
        return ReviewValidation(
            proposal_id=proposal.proposal_id,
            fresh=False,
            current_head_set_hash=current_hash,
            reason="base_revision_is_in_the_future",
        )

    expected_precondition = fingerprint_precondition(
        proposal_type=proposal.proposal_type,
        target_slot_fingerprint=proposal.target_slot_fingerprint,
        expected_head_set_hash=proposal.expected_head_set_hash,
    )
    if proposal.precondition_hash != expected_precondition:
        return ReviewValidation(
            proposal_id=proposal.proposal_id,
            fresh=False,
            current_head_set_hash=current_hash,
            reason="precondition_hash_mismatch",
        )

    empty_head_set = fingerprint_head_set(())
    if proposal.proposal_type is ProposalType.ADD:
        fresh = proposal.expected_head_set_hash == empty_head_set and current_hash == empty_head_set
        reason = None if fresh else "add_target_is_no_longer_empty"
    else:
        fresh = current_hash == proposal.expected_head_set_hash
        reason = None if fresh else "target_head_set_changed"
    return ReviewValidation(
        proposal_id=proposal.proposal_id,
        fresh=fresh,
        current_head_set_hash=current_hash,
        reason=reason,
    )


def validate_review_batch(
    proposals: Sequence[ReviewProposal],
    heads_by_slot: Mapping[str, Sequence[ClaimHead]],
    *,
    current_memory_revision: int,
) -> tuple[ReviewValidation, ...]:
    if not proposals:
        return ()
    batch_ids = {proposal.batch_id for proposal in proposals}
    if len(batch_ids) != 1:
        raise ValueError("all proposals must belong to the same batch")
    slots = [proposal.target_slot_fingerprint for proposal in proposals]
    if len(set(slots)) != len(slots):
        raise ValueError("a review batch cannot modify the same target slot twice")
    return tuple(
        validate_proposal(
            proposal,
            heads_by_slot.get(proposal.target_slot_fingerprint, ()),
            current_memory_revision=current_memory_revision,
        )
        for proposal in proposals
    )


def assert_review_batch_fresh(
    proposals: Sequence[ReviewProposal],
    heads_by_slot: Mapping[str, Sequence[ClaimHead]],
    *,
    current_memory_revision: int,
) -> tuple[ReviewValidation, ...]:
    validations = validate_review_batch(
        proposals,
        heads_by_slot,
        current_memory_revision=current_memory_revision,
    )
    stale = [validation for validation in validations if not validation.fresh]
    if stale:
        details = ", ".join(f"{validation.proposal_id}:{validation.reason}" for validation in stale)
        raise StaleProposalError(f"review batch is stale: {details}")
    return validations


class ReviewStore(Protocol):
    def current_revision(self) -> int: ...

    def load_review_batch(self, batch_id: str) -> Mapping[str, object]: ...

    def load_heads(self, identity: str) -> list[ClaimHead]: ...

    def semantic_context_hash(self, candidate_ids: Sequence[str]) -> str: ...

    def mark_review_stale(
        self,
        batch_id: str,
        *,
        proposal_ids: Sequence[str] | None = None,
        note: str = "review precondition changed",
    ) -> int: ...

    def record_review_decisions(
        self,
        batch_id: str,
        decisions: Mapping[str, str],
        *,
        note: str | None = None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class PersistedBatchValidation:
    batch_id: str
    proposals: tuple[ReviewProposal, ...]
    validations: tuple[ReviewValidation, ...]

    @property
    def fresh(self) -> bool:
        return all(validation.fresh for validation in self.validations)


class ReviewService:
    """Coordinate persisted review intent while keeping canonical apply separate."""

    def __init__(self, store: ReviewStore) -> None:
        self._store = store

    def validate_batch(self, batch_id: str) -> PersistedBatchValidation:
        record = self._store.load_review_batch(batch_id)
        if record.get("status") != "pending":
            raise ValueError(f"review batch is {record.get('status')!r}, not pending")
        rows = record.get("proposals")
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise ValueError("review batch proposals are malformed")
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError("review batch proposal is malformed")
            validate_proposal_binding(row)
        proposals = tuple(_proposal_from_row(cast(Mapping[str, object], row)) for row in rows)
        heads_by_slot = {
            proposal.target_slot_fingerprint: self._store.load_heads(
                proposal.target_slot_fingerprint
            )
            for proposal in proposals
        }
        validations = validate_review_batch(
            proposals,
            heads_by_slot,
            current_memory_revision=self._store.current_revision(),
        )
        semantic_stale: set[str] = set()
        for row in rows:
            assert isinstance(row, Mapping)
            payload = row.get("payload")
            if not isinstance(payload, Mapping):
                continue
            expected_semantic_hash = payload.get("semantic_context_hash")
            if not isinstance(expected_semantic_hash, str) or not expected_semantic_hash:
                continue
            candidate_ids = payload.get("candidate_ids")
            if not isinstance(candidate_ids, Sequence) or isinstance(candidate_ids, str | bytes):
                raise ValueError("proposal candidate_ids must be a list")
            current_semantic_hash = self._store.semantic_context_hash(
                tuple(str(item) for item in candidate_ids)
            )
            if current_semantic_hash != expected_semantic_hash:
                semantic_stale.add(str(row["proposal_id"]))
        validations = tuple(
            replace(validation, fresh=False, reason="semantic_context_changed")
            if validation.proposal_id in semantic_stale
            else validation
            for validation in validations
        )
        return PersistedBatchValidation(
            batch_id=batch_id,
            proposals=proposals,
            validations=validations,
        )

    def record_approve_all(
        self, batch_id: str, *, note: str | None = None
    ) -> PersistedBatchValidation:
        checked = self.validate_batch(batch_id)
        if not checked.fresh:
            self._store.mark_review_stale(batch_id, note="review batch contains a stale proposal")
            stale = [validation for validation in checked.validations if not validation.fresh]
            details = ", ".join(
                f"{validation.proposal_id}:{validation.reason}" for validation in stale
            )
            raise StaleProposalError(f"review batch is stale: {details}")
        self._store.record_review_decisions(
            batch_id,
            {proposal.proposal_id: "approved" for proposal in checked.proposals},
            note=note,
        )
        return checked


def _proposal_from_row(row: Mapping[str, object]) -> ReviewProposal:
    return ReviewProposal(
        proposal_id=str(row["proposal_id"]),
        batch_id=str(row["batch_id"]),
        proposal_type=ProposalType(str(row["proposal_type"])),
        base_memory_revision=int(cast(int, row["base_memory_revision"])),
        target_slot_fingerprint=str(row["target_slot_fingerprint"]),
        expected_head_set_hash=str(row["expected_head_set_hash"]),
        precondition_hash=str(row["precondition_hash"]),
        proposal_payload_fingerprint=str(row["proposal_payload_fingerprint"]),
        evidence_set_fingerprint=str(row["evidence_set_fingerprint"]),
        status=ProposalStatus(str(row["status"])),
    )


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _storage_fingerprint(value: object) -> str:
    """Match ``storage.fingerprint`` without introducing a circular import."""

    return _canonical_hash(["" if value is None else value])


def _normalize_text(value: str) -> str:
    return " ".join(value.casefold().split())

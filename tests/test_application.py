from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from local_dreaming.application import CanonicalApplicationService
from local_dreaming.errors import StaleProposalError
from local_dreaming.forgetting import ForgottenLedger
from local_dreaming.models import ProposalType
from local_dreaming.review import (
    ReviewService,
    fingerprint_head_set,
    fingerprint_precondition,
    fingerprint_target_slot,
)
from local_dreaming.storage import (
    ClaimVersionInput,
    MemoryStore,
    ReviewProposalInput,
    fingerprint,
    identity_fingerprint,
    value_fingerprint,
)


def _batch(store: MemoryStore, *, expected: str) -> str:
    slot = fingerprint_target_slot(
        subject_text="Leo", predicate="preference.language", scope="user_profile"
    )
    payload = {
        "subject_text": "Leo",
        "predicate": "preference.language",
        "scope": "user_profile",
        "value": "zh-TW",
        "summary": "Leo偏好繁體中文",
        "evidence_event_ids": [],
    }
    return store.create_review_batch(
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
                proposal_payload_fingerprint=fingerprint(payload),
                evidence_set_fingerprint=fingerprint([]),
                payload=payload,
            )
        ]
    )


def test_approved_batch_applies_canonical_claim_atomically(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory)
    batch_id = _batch(store, expected=fingerprint_head_set(()))
    ReviewService(store).record_approve_all(batch_id)

    revision, versions = CanonicalApplicationService(memory).apply_batch(batch_id)

    assert revision == 1
    assert len(versions) == 1
    assert store.search("language")[0]["summary"] == "Leo偏好繁體中文"
    assert store.load_review_batch(batch_id)["status"] == "applied"


def test_approved_batch_rechecks_head_set_before_apply(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory)
    batch_id = _batch(store, expected=fingerprint_head_set(()))
    ReviewService(store).record_approve_all(batch_id)
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value="en",
            summary="newer correction",
        )
    )

    with pytest.raises(StaleProposalError):
        CanonicalApplicationService(memory).apply_batch(batch_id)

    assert store.load_review_batch(batch_id)["status"] == "approved"


def test_approved_batch_cannot_reintroduce_forgotten_fact(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    forgotten_path = tmp_path / "forgotten.jsonl"
    store = MemoryStore(memory)
    batch_id = _batch(store, expected=fingerprint_head_set(()))
    ReviewService(store).record_approve_all(batch_id)
    identity = identity_fingerprint("Leo", "preference.language", "user_profile")
    value = value_fingerprint("zh-TW")
    ledger = ForgottenLedger(forgotten_path)
    ledger.prepare(
        target_kind="claim_exact",
        target_fingerprint=fingerprint("suppressed", identity, value),
        identity_fingerprint=identity,
        value_fingerprint=value,
    )

    with pytest.raises(ValueError, match="suppressed"):
        CanonicalApplicationService(memory, forgotten_path).apply_batch(batch_id)

    assert store.current_revision() == 0


def test_application_rolls_back_entire_batch_on_mid_transaction_failure(
    tmp_path: Path,
) -> None:
    memory = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory)
    empty = fingerprint_head_set(())
    proposals: list[ReviewProposalInput] = []
    for predicate, evidence_ids in (
        ("project.first", []),
        ("project.second", ["missing-event"]),
    ):
        slot = fingerprint_target_slot(
            subject_text="Leo", predicate=predicate, scope="project_state"
        )
        payload = {
            "subject_text": "Leo",
            "predicate": predicate,
            "scope": "project_state",
            "value": predicate,
            "summary": predicate,
            "evidence_event_ids": evidence_ids,
        }
        proposals.append(
            ReviewProposalInput(
                proposal_type="add",
                target_slot_fingerprint=slot,
                expected_head_set_hash=empty,
                precondition_hash=fingerprint_precondition(
                    proposal_type=ProposalType.ADD,
                    target_slot_fingerprint=slot,
                    expected_head_set_hash=empty,
                ),
                proposal_payload_fingerprint=fingerprint(payload),
                evidence_set_fingerprint=fingerprint(evidence_ids),
                payload=payload,
            )
        )
    batch_id = store.create_review_batch(proposals)
    ReviewService(store).record_approve_all(batch_id)

    with pytest.raises(ValueError, match="unknown evidence event"):
        CanonicalApplicationService(memory).apply_batch(batch_id)

    assert store.current_revision() == 0
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM claims").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM claim_versions").fetchone()[0] == 0
    assert store.load_review_batch(batch_id)["status"] == "approved"


def test_review_payload_binding_cannot_be_mutated_after_approval(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    store = MemoryStore(memory)
    batch_id = _batch(store, expected=fingerprint_head_set(()))
    ReviewService(store).record_approve_all(batch_id)

    with (
        store.connection() as connection,
        pytest.raises(sqlite3.IntegrityError, match="review proposal binding is immutable"),
    ):
        connection.execute(
            "UPDATE review_proposals SET payload_json = '{}' WHERE batch_id = ?",
            (batch_id,),
        )

    revision, _ = CanonicalApplicationService(memory).apply_batch(batch_id)
    assert revision == 1

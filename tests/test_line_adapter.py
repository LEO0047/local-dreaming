from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from local_dreaming.errors import PrivacyBoundaryError
from local_dreaming.line_adapter import SyntheticLineAdapter, SyntheticLineMessage
from local_dreaming.models import Sensitivity, SourceKind

NOW = datetime(2026, 7, 19, 9, tzinfo=UTC)


def _message(
    message_id: str,
    *,
    contact_id: str = "synthetic-contact:alice",
    occurred_at: datetime = NOW,
) -> SyntheticLineMessage:
    return SyntheticLineMessage(
        dataset_id="synthetic-line:fixture-1",
        contact_id=contact_id,
        message_id=f"synthetic-message:{message_id}",
        occurred_at=occurred_at,
        content=f"message {message_id}",
    )


def test_rejects_non_synthetic_line_data() -> None:
    with pytest.raises(PrivacyBoundaryError, match="synthetic datasets only"):
        SyntheticLineMessage(
            dataset_id="real-line-export",
            contact_id="synthetic-contact:alice",
            message_id="synthetic-message:1",
            occurred_at=NOW,
            content="not allowed",
        )


def test_requires_exact_contact_opt_in_and_never_discovers_contacts() -> None:
    adapter = SyntheticLineAdapter(
        dataset_id="synthetic-line:fixture-1",
        messages=(_message("1"),),
        opted_in_contact_ids=frozenset({"synthetic-contact:alice"}),
    )

    with pytest.raises(PrivacyBoundaryError, match="not opted in"):
        adapter.events_for_contact("synthetic-contact:bob")
    assert not hasattr(adapter, "list_contacts")


def test_returns_only_selected_contact_in_stable_order_and_private_policy() -> None:
    adapter = SyntheticLineAdapter(
        dataset_id="synthetic-line:fixture-1",
        messages=(
            _message("2", occurred_at=NOW + timedelta(minutes=2)),
            _message("bob", contact_id="synthetic-contact:bob"),
            _message("1"),
        ),
        opted_in_contact_ids=frozenset({"synthetic-contact:alice"}),
    )

    events = adapter.events_for_contact("synthetic-contact:alice")
    policy = adapter.policy_for_contact("synthetic-contact:alice")

    assert [event.external_event_id for event in events] == [
        "synthetic-message:1",
        "synthetic-message:2",
    ]
    assert all(event.source_kind is SourceKind.LINE_SYNTHETIC for event in events)
    assert all(event.sensitivity is Sensitivity.PRIVATE for event in events)
    assert not policy.allow_private_model_egress

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from local_dreaming.errors import PrivacyBoundaryError
from local_dreaming.models import (
    IngestEventInput,
    Sensitivity,
    SourceKind,
    SourcePolicy,
)

_DATASET_PREFIX = "synthetic-line:"
_CONTACT_PREFIX = "synthetic-contact:"
_MESSAGE_PREFIX = "synthetic-message:"


@dataclass(frozen=True, slots=True)
class SyntheticLineMessage:
    dataset_id: str
    contact_id: str
    message_id: str
    occurred_at: datetime
    content: str

    def __post_init__(self) -> None:
        if not self.dataset_id.startswith(_DATASET_PREFIX):
            raise PrivacyBoundaryError("LINE v2.2 accepts synthetic datasets only")
        if not self.contact_id.startswith(_CONTACT_PREFIX):
            raise PrivacyBoundaryError("LINE v2.2 accepts synthetic contacts only")
        if not self.message_id.startswith(_MESSAGE_PREFIX):
            raise PrivacyBoundaryError("LINE v2.2 accepts synthetic messages only")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")


class SyntheticLineAdapter:
    """Read an explicitly selected contact from an in-memory synthetic fixture.

    There is intentionally no contact discovery API. Callers must already possess
    the exact synthetic contact ID and list it in ``opted_in_contact_ids``.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        messages: tuple[SyntheticLineMessage, ...],
        opted_in_contact_ids: frozenset[str],
    ) -> None:
        if not dataset_id.startswith(_DATASET_PREFIX):
            raise PrivacyBoundaryError("LINE v2.2 accepts synthetic datasets only")
        if any(message.dataset_id != dataset_id for message in messages):
            raise PrivacyBoundaryError("all messages must belong to the selected dataset")
        if any(not contact_id.startswith(_CONTACT_PREFIX) for contact_id in opted_in_contact_ids):
            raise PrivacyBoundaryError("opt-in contains a non-synthetic contact")
        self._dataset_id = dataset_id
        self._messages = messages
        self._opted_in_contact_ids = opted_in_contact_ids

    def events_for_contact(self, contact_id: str) -> tuple[IngestEventInput, ...]:
        if contact_id not in self._opted_in_contact_ids:
            raise PrivacyBoundaryError("the exact synthetic LINE contact is not opted in")
        selected = sorted(
            (message for message in self._messages if message.contact_id == contact_id),
            key=lambda message: (message.occurred_at, message.message_id),
        )
        source_id = self.source_id(contact_id)
        return tuple(
            IngestEventInput(
                source_id=source_id,
                partition_id=f"{self._dataset_id}:{contact_id}",
                source_kind=SourceKind.LINE_SYNTHETIC,
                external_event_id=message.message_id,
                occurred_at=message.occurred_at,
                content=message.content,
                sensitivity=Sensitivity.PRIVATE,
                metadata={"synthetic": True},
            )
            for message in selected
        )

    def policy_for_contact(
        self, contact_id: str, *, allow_private_model_egress: bool = False
    ) -> SourcePolicy:
        if contact_id not in self._opted_in_contact_ids:
            raise PrivacyBoundaryError("the exact synthetic LINE contact is not opted in")
        return SourcePolicy(
            source_id=self.source_id(contact_id),
            source_kind=SourceKind.LINE_SYNTHETIC,
            opted_in=True,
            sensitivity=Sensitivity.PRIVATE,
            allow_model_egress=False,
            allow_private_model_egress=allow_private_model_egress,
        )

    def source_id(self, contact_id: str) -> str:
        if not contact_id.startswith(_CONTACT_PREFIX):
            raise PrivacyBoundaryError("LINE v2.2 accepts synthetic contacts only")
        suffix = contact_id.removeprefix(_CONTACT_PREFIX)
        return f"line-synthetic:{self._dataset_id.removeprefix(_DATASET_PREFIX)}:{suffix}"

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol

from local_dreaming.errors import OversizedInputError, PrivacyBoundaryError
from local_dreaming.evidence import MAX_MODEL_INPUT_CHARS
from local_dreaming.handoff import parse_dreaming_handoff, serialize_dreaming_handoff
from local_dreaming.models import (
    ClaimScope,
    EpisodeDraft,
    IngestEventInput,
    JsonScalar,
    PreparedEvent,
    Sensitivity,
    SourceKind,
    SourcePolicy,
)
from local_dreaming.redaction import REDACTED_SECRET, redact_secrets


@dataclass(frozen=True, slots=True)
class IngestResult:
    event: PreparedEvent
    persisted: bool


@dataclass(frozen=True, slots=True)
class ModelInputBundle:
    payload: str
    source_ids: tuple[str, ...]
    event_ids: tuple[str, ...]
    input_bytes: int
    redaction_count: int
    maximum_sensitivity: Sensitivity
    truncated: bool


@dataclass(frozen=True, slots=True)
class PersistableEvent:
    event_id: str
    source_id: str
    partition_id: str
    external_event_id: str
    event_type: str
    content_text: str
    content_fingerprint: str
    source_locator: str | None
    sensitivity: str
    occurred_at: str
    parser_version: str
    redactor_version: str
    metadata: dict[str, object]


@dataclass(frozen=True, slots=True)
class PersistableEpisode:
    episode_id: str
    source_id: str
    partition_id: str
    episode_type: str
    content_text: str
    content_fingerprint: str
    segmenter_version: str
    segmentation_reason: str
    event_ids: tuple[str, ...]
    sensitivity: str
    occurred_from: str
    occurred_to: str


class IngestSink(Protocol):
    def create_event(self, event: object) -> str: ...

    def create_episode(self, episode: object) -> str: ...


class IngestService:
    def __init__(self, sink: IngestSink | None = None) -> None:
        self._sink = sink

    def ingest_event(
        self, event: IngestEventInput, policy: SourcePolicy, *, dry_run: bool = False
    ) -> IngestResult:
        prepared = prepare_event(event, policy)
        if dry_run:
            return IngestResult(event=prepared, persisted=False)
        if self._sink is None:
            raise RuntimeError("an EventSink is required when dry_run is false")
        stored_id = self._sink.create_event(event_for_storage(prepared))
        if stored_id != prepared.event_id:
            raise RuntimeError("EventSink returned an unexpected event_id")
        return IngestResult(event=prepared, persisted=True)

    def persist_episode(self, episode: EpisodeDraft, *, dry_run: bool = False) -> bool:
        if dry_run:
            return False
        if self._sink is None:
            raise RuntimeError("an IngestSink is required when dry_run is false")
        stored_id = self._sink.create_episode(episode_for_storage(episode))
        if stored_id != episode.episode_id:
            raise RuntimeError("IngestSink returned an unexpected episode_id")
        return True


def prepare_event(event: IngestEventInput, policy: SourcePolicy) -> PreparedEvent:
    if event.source_id != policy.source_id or event.source_kind is not policy.source_kind:
        raise PrivacyBoundaryError("event source does not match its source policy")
    if not policy.opted_in:
        raise PrivacyBoundaryError(f"source {event.source_id!r} is not opted in")

    handoff_redactions = 0
    source_metadata = dict(event.metadata)
    source_content = event.content
    if event.source_kind is SourceKind.DREAMING_HANDOFF:
        handoff = parse_dreaming_handoff(event.content)
        if handoff is None:
            raise PrivacyBoundaryError("dreaming handoff is malformed or not terminal")
        source_content = serialize_dreaming_handoff(handoff)
        source_metadata["claim_scope"] = ClaimScope.PROJECT_STATE.value
        handoff_redactions = handoff.redaction_count

    normalized = normalize_content(source_content)
    redaction = redact_secrets(normalized)
    locator_result = redact_secrets(event.source_locator or "")
    safe_metadata, metadata_redactions = _redact_metadata(source_metadata)
    redaction_count = (
        redaction.redaction_count
        + locator_result.redaction_count
        + metadata_redactions
        + handoff_redactions
    )
    explicitly_secret = (
        event.sensitivity is Sensitivity.SECRET or policy.sensitivity is Sensitivity.SECRET
    )
    if explicitly_secret:
        safe_content = REDACTED_SECRET
        safe_locator = None
        safe_metadata = {"redacted_secret": True}
        redaction_count += 1
    else:
        safe_content = redaction.text
        safe_locator = locator_result.text or None
        if redaction_count and REDACTED_SECRET not in safe_content:
            safe_content = f"{safe_content}\n{REDACTED_SECRET}" if safe_content else REDACTED_SECRET
    effective_sensitivity = _maximum_sensitivity(
        Sensitivity.SECRET if redaction_count else Sensitivity.NORMAL,
        event.sensitivity,
        policy.sensitivity,
    )
    fingerprint = hashlib.sha256(safe_content.encode()).hexdigest()
    identity_payload = {
        "content_fingerprint": fingerprint,
        "external_event_id": event.external_event_id,
        "partition_id": event.partition_id,
        "source_id": event.source_id,
    }
    encoded_identity = json.dumps(
        identity_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    event_id = "ev_" + hashlib.sha256(encoded_identity).hexdigest()[:32]
    return PreparedEvent(
        event_id=event_id,
        source_id=event.source_id,
        partition_id=event.partition_id,
        source_kind=event.source_kind,
        external_event_id=event.external_event_id,
        occurred_at=event.occurred_at,
        content=safe_content,
        content_fingerprint=fingerprint,
        sensitivity=effective_sensitivity,
        redaction_count=redaction_count,
        eligible_for_model=policy.permits_model_egress(effective_sensitivity),
        source_locator=safe_locator,
        metadata=safe_metadata,
    )


def event_for_storage(event: PreparedEvent) -> PersistableEvent:
    return PersistableEvent(
        event_id=event.event_id,
        source_id=event.source_id,
        partition_id=event.partition_id,
        external_event_id=event.external_event_id,
        event_type=event.source_kind.value,
        content_text=event.content,
        content_fingerprint=event.content_fingerprint,
        source_locator=event.source_locator,
        sensitivity=event.sensitivity.value,
        occurred_at=event.occurred_at.isoformat(),
        parser_version="ingest-v1",
        redactor_version="redactor-v1",
        metadata=dict(event.metadata),
    )


def episode_for_storage(episode: EpisodeDraft) -> PersistableEpisode:
    return PersistableEpisode(
        episode_id=episode.episode_id,
        source_id=episode.source_id,
        partition_id=episode.partition_id,
        episode_type="source_episode",
        content_text=episode.content,
        content_fingerprint=episode.content_fingerprint,
        segmenter_version=episode.segmenter_version,
        segmentation_reason=episode.segmentation_reason,
        event_ids=episode.event_ids,
        sensitivity=episode.sensitivity.value,
        occurred_from=episode.started_at.isoformat(),
        occurred_to=episode.ended_at.isoformat(),
    )


def build_bounded_model_input(
    events: Iterable[PreparedEvent],
    policies: Mapping[str, SourcePolicy],
    *,
    max_chars: int = MAX_MODEL_INPUT_CHARS,
) -> ModelInputBundle:
    if max_chars <= 0:
        raise ValueError("max_chars must be positive")

    accepted: list[PreparedEvent] = []
    chunks: list[str] = []
    current_chars = 0
    truncated = False
    for event in sorted(events, key=lambda item: (item.occurred_at, item.event_id)):
        policy = policies.get(event.source_id)
        if (
            policy is None
            or event.sensitivity is Sensitivity.SECRET
            or not event.eligible_for_model
            or not policy.permits_model_egress(event.sensitivity)
        ):
            continue
        chunk = json.dumps(
            {
                "content": event.content,
                "event_id": event.event_id,
                "occurred_at": event.occurred_at.isoformat(),
                "source_kind": event.source_kind.value,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        separator_size = 1 if chunks else 0
        if len(event.content) > max_chars:
            raise OversizedInputError(
                f"event {event.event_id} exceeds the {max_chars}-character model boundary"
            )
        if current_chars + separator_size + len(chunk) > max_chars:
            raise OversizedInputError(
                f"model input exceeds the {max_chars}-character model boundary"
            )
        chunks.append(chunk)
        accepted.append(event)
        current_chars += separator_size + len(chunk)

    payload = "\n".join(chunks)
    maximum_sensitivity = (
        Sensitivity.PRIVATE
        if any(event.sensitivity is Sensitivity.PRIVATE for event in accepted)
        else Sensitivity.NORMAL
    )
    return ModelInputBundle(
        payload=payload,
        source_ids=tuple(dict.fromkeys(event.source_id for event in accepted)),
        event_ids=tuple(event.event_id for event in accepted),
        input_bytes=len(payload.encode()),
        redaction_count=sum(event.redaction_count for event in accepted),
        maximum_sensitivity=maximum_sensitivity,
        truncated=truncated,
    )


def normalize_content(content: str) -> str:
    normalized = unicodedata.normalize("NFC", content.replace("\r\n", "\n").replace("\r", "\n"))
    return "\n".join(line.rstrip() for line in normalized.strip().splitlines())


def _maximum_sensitivity(*values: Sensitivity) -> Sensitivity:
    order = {
        Sensitivity.NORMAL: 0,
        Sensitivity.PRIVATE: 1,
        Sensitivity.SECRET: 2,
    }
    return max(values, key=order.__getitem__)


def _redact_metadata(metadata: Mapping[str, JsonScalar]) -> tuple[dict[str, JsonScalar], int]:
    sanitized: dict[str, JsonScalar] = {}
    count = 0
    for index, key in enumerate(sorted(metadata)):
        key_result = redact_secrets(key)
        safe_key = key_result.text
        if key_result.redaction_count:
            safe_key = f"redacted_secret_key_{index}"
        value = metadata[key]
        if isinstance(value, str):
            value_result = redact_secrets(value)
            safe_value: JsonScalar = value_result.text
            count += value_result.redaction_count
        else:
            safe_value = value
        count += key_result.redaction_count
        sanitized[safe_key] = safe_value
    return sanitized, count

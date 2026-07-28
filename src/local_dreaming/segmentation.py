from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from local_dreaming.episode_identity import episode_id_for
from local_dreaming.errors import OversizedInputError
from local_dreaming.models import EpisodeDraft, PreparedEvent, Sensitivity


@dataclass(frozen=True, slots=True)
class SegmenterConfig:
    version: str = "v1"
    max_gap: timedelta = timedelta(hours=2)
    max_chars: int = 12_000
    max_events: int = 50

    def __post_init__(self) -> None:
        if self.max_gap.total_seconds() < 0:
            raise ValueError("max_gap must not be negative")
        if self.max_chars <= 0 or self.max_events <= 0:
            raise ValueError("max_chars and max_events must be positive")


def segment_events(
    events: list[PreparedEvent], config: SegmenterConfig | None = None
) -> list[EpisodeDraft]:
    """Group events deterministically by partition, time gap and size limits."""

    active_config = config or SegmenterConfig()
    if not events:
        return []
    oversized = [event for event in events if len(event.content) > active_config.max_chars]
    if oversized:
        raise OversizedInputError(
            f"event {oversized[0].event_id} exceeds the "
            f"{active_config.max_chars}-character episode boundary"
        )

    partitions: dict[tuple[str, str], list[PreparedEvent]] = defaultdict(list)
    for event in events:
        partitions[(event.source_id, event.partition_id)].append(event)

    episodes: list[EpisodeDraft] = []
    for partition_events in partitions.values():
        current: list[PreparedEvent] = []
        for event in sorted(partition_events, key=lambda item: (item.occurred_at, item.event_id)):
            reason = _boundary_reason(current, event, active_config)
            if reason is not None:
                episodes.append(_build_episode(current, active_config, reason))
                current = []
            current.append(event)
        episodes.append(_build_episode(current, active_config, "end_of_input"))
    return sorted(episodes, key=lambda episode: (episode.started_at, episode.episode_id))


def _boundary_reason(
    current: list[PreparedEvent], event: PreparedEvent, config: SegmenterConfig
) -> str | None:
    if not current:
        return None
    previous = current[-1]
    if event.source_id != previous.source_id or event.partition_id != previous.partition_id:
        return "source_partition_boundary"
    if event.occurred_at - previous.occurred_at > config.max_gap:
        return "time_gap"
    projected_chars = sum(len(item.content) for item in current) + len(event.content)
    projected_chars += 2 * len(current)
    if projected_chars > config.max_chars:
        return "size_limit"
    if len(current) >= config.max_events:
        return "event_limit"
    return None


def _build_episode(
    events: list[PreparedEvent], config: SegmenterConfig, reason: str
) -> EpisodeDraft:
    first = events[0]
    content = "\n\n".join(event.content for event in events)
    content_fingerprint = hashlib.sha256(content.encode()).hexdigest()
    episode_id = episode_id_for(
        source_id=first.source_id,
        partition_id=first.partition_id,
        event_ids=[event.event_id for event in events],
        segmenter_version=config.version,
    )
    sensitivity = (
        Sensitivity.SECRET
        if any(event.sensitivity is Sensitivity.SECRET for event in events)
        else Sensitivity.PRIVATE
        if any(event.sensitivity is Sensitivity.PRIVATE for event in events)
        else Sensitivity.NORMAL
    )
    return EpisodeDraft(
        episode_id=episode_id,
        source_id=first.source_id,
        partition_id=first.partition_id,
        event_ids=tuple(event.event_id for event in events),
        started_at=events[0].occurred_at,
        ended_at=events[-1].occurred_at,
        content=content,
        content_fingerprint=content_fingerprint,
        sensitivity=sensitivity,
        segmentation_reason=reason,
        segmenter_version=config.version,
    )

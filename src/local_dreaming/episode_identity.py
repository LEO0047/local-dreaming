from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence


def event_sequence_fingerprint(event_ids: Sequence[str]) -> str:
    """Hash an ordered event sequence without flattening it into SQLite columns."""

    normalized = tuple(str(event_id) for event_id in event_ids)
    if not normalized or any(not event_id.strip() for event_id in normalized):
        raise ValueError("episode identity requires a non-empty ordered event sequence")
    encoded = json.dumps(
        list(normalized), ensure_ascii=False, separators=(",", ":"), sort_keys=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def episode_id_for(
    *,
    source_id: str,
    partition_id: str,
    event_ids: Sequence[str],
    segmenter_version: str,
) -> str:
    """Return the stable public ID for one logical episode.

    Keep this encoding compatible with the v1 segmenter so existing episode IDs
    can be validated and preserved during the v2 schema migration.
    """

    normalized_event_ids = tuple(str(event_id) for event_id in event_ids)
    event_sequence_fingerprint(normalized_event_ids)
    identity_payload = {
        "event_ids": list(normalized_event_ids),
        "partition_id": partition_id,
        "segmenter_version": segmenter_version,
        "source_id": source_id,
    }
    identity = json.dumps(
        identity_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    return "ep_" + hashlib.sha256(identity.encode()).hexdigest()[:32]

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from local_dreaming.errors import PrivacyBoundaryError
from local_dreaming.ingest import IngestService, build_bounded_model_input, prepare_event
from local_dreaming.models import (
    ClaimScope,
    IngestEventInput,
    Sensitivity,
    SourceKind,
    SourcePolicy,
)
from local_dreaming.redaction import REDACTED_SECRET
from local_dreaming.segmentation import SegmenterConfig, segment_events
from local_dreaming.storage import MemoryStore

NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)


def _input(
    content: str,
    *,
    external_event_id: str = "message-1",
    occurred_at: datetime = NOW,
    sensitivity: Sensitivity = Sensitivity.NORMAL,
) -> IngestEventInput:
    return IngestEventInput(
        source_id="codex:task-1",
        partition_id="task-1",
        source_kind=SourceKind.CODEX_TASK,
        external_event_id=external_event_id,
        occurred_at=occurred_at,
        content=content,
        sensitivity=sensitivity,
    )


def _policy(
    *,
    opted_in: bool = True,
    sensitivity: Sensitivity = Sensitivity.NORMAL,
    allow_model: bool = True,
    allow_private: bool = False,
) -> SourcePolicy:
    return SourcePolicy(
        source_id="codex:task-1",
        source_kind=SourceKind.CODEX_TASK,
        opted_in=opted_in,
        sensitivity=sensitivity,
        allow_model_egress=allow_model,
        allow_private_model_egress=allow_private,
    )


def test_prepare_event_is_stable_normalized_and_redacts_before_identity() -> None:
    original = _input(" Hello  \r\napi_key=abcdefghijklmnop\r\n")

    first = prepare_event(original, _policy())
    second = prepare_event(original, _policy())

    assert first == second
    assert first.content == f"Hello\n{REDACTED_SECRET}"
    assert first.sensitivity is Sensitivity.SECRET
    assert not first.eligible_for_model
    assert "abcdefghijklmnop" not in repr(first)


def test_prepare_event_redacts_locator_and_metadata_before_persistence() -> None:
    token = "abcdefghijklmnop"
    event = IngestEventInput(
        source_id="codex:task-1",
        partition_id="task-1",
        source_kind=SourceKind.CODEX_TASK,
        external_event_id="metadata-secret",
        occurred_at=NOW,
        content="safe body",
        source_locator=f"https://example.test/?token={token}",
        metadata={"credential": f"password={token}"},
    )

    prepared = prepare_event(event, _policy())

    assert prepared.sensitivity is Sensitivity.SECRET
    assert prepared.content == f"safe body\n{REDACTED_SECRET}"
    assert token not in repr(prepared)
    assert prepared.redaction_count == 2


def test_explicit_secret_classification_keeps_only_marker_and_opaque_identity() -> None:
    prepared = prepare_event(
        IngestEventInput(
            source_id="codex:task-1",
            partition_id="task-1",
            source_kind=SourceKind.CODEX_TASK,
            external_event_id="explicit-secret",
            occurred_at=NOW,
            content="confidential content that must not persist",
            sensitivity=Sensitivity.SECRET,
            source_locator="/private/location",
            metadata={"topic": "confidential"},
        ),
        _policy(),
    )

    assert prepared.content == REDACTED_SECRET
    assert prepared.source_locator is None
    assert prepared.metadata == {"redacted_secret": True}
    assert "confidential" not in repr(prepared)


def test_event_identity_changes_with_content_but_not_input_ordering() -> None:
    first = prepare_event(_input("one"), _policy())
    changed = prepare_event(_input("two"), _policy())

    assert first.event_id != changed.event_id
    assert first.content_fingerprint != changed.content_fingerprint


def test_source_policy_requires_opt_in_and_controls_private_egress() -> None:
    with pytest.raises(PrivacyBoundaryError, match="not opted in"):
        prepare_event(_input("hello"), _policy(opted_in=False))

    private = prepare_event(
        _input("family context", sensitivity=Sensitivity.PRIVATE),
        _policy(allow_private=False),
    )
    allowed_private = prepare_event(
        _input("approved context", external_event_id="message-2", sensitivity=Sensitivity.PRIVATE),
        _policy(allow_private=True),
    )
    bundle = build_bounded_model_input(
        [private, allowed_private], {"codex:task-1": _policy(allow_private=True)}
    )

    assert not private.eligible_for_model
    assert allowed_private.eligible_for_model
    assert private.event_id not in bundle.event_ids
    assert bundle.event_ids == (allowed_private.event_id,)
    assert bundle.maximum_sensitivity is Sensitivity.PRIVATE

    inherited_private = prepare_event(
        _input("source-level privacy"),
        _policy(sensitivity=Sensitivity.PRIVATE, allow_private=False),
    )
    assert inherited_private.sensitivity is Sensitivity.PRIVATE
    assert not inherited_private.eligible_for_model


def test_source_trust_rules_limit_advisory_and_generated_sources() -> None:
    assert not SourcePolicy("chronicle", SourceKind.CHRONICLE, opted_in=True).permits_claim_scope(
        ClaimScope.PROJECT_STATE
    )
    handoff = SourcePolicy("handoff", SourceKind.DREAMING_HANDOFF, opted_in=True)
    assert handoff.permits_claim_scope(ClaimScope.PROJECT_STATE)
    assert not handoff.permits_claim_scope(ClaimScope.USER_PROFILE)


def test_handoff_ingest_keeps_only_valid_terminal_project_state_payload() -> None:
    final = """Untrusted narrative outside the handoff.

[DREAMING_HANDOFF]
workspace: /tmp/project
state: in_progress
completed: storage
verified: pytest
leo_corrections: token=abcdefghijklmnop
pending: compiler
[/DREAMING_HANDOFF]"""
    policy = SourcePolicy(
        source_id="handoff:task-1",
        source_kind=SourceKind.DREAMING_HANDOFF,
        opted_in=True,
        allow_model_egress=True,
    )
    event = IngestEventInput(
        source_id=policy.source_id,
        partition_id="task-1",
        source_kind=policy.source_kind,
        external_event_id="handoff-1",
        occurred_at=NOW,
        content=final,
    )

    prepared = prepare_event(event, policy)

    assert "Untrusted narrative" not in prepared.content
    assert '"classification":"project_state"' in prepared.content
    assert REDACTED_SECRET in prepared.content
    assert prepared.sensitivity is Sensitivity.SECRET
    assert prepared.metadata["claim_scope"] == "project_state"

    with pytest.raises(PrivacyBoundaryError, match="malformed or not terminal"):
        prepare_event(
            IngestEventInput(
                source_id=policy.source_id,
                partition_id="task-1",
                source_kind=policy.source_kind,
                external_event_id="handoff-2",
                occurred_at=NOW,
                content=final + "\nextra",
            ),
            policy,
        )


def test_segmentation_is_stable_and_obeys_time_gap() -> None:
    first = prepare_event(_input("one", external_event_id="one"), _policy())
    second = prepare_event(
        _input("two", external_event_id="two", occurred_at=NOW + timedelta(minutes=10)),
        _policy(),
    )
    third = prepare_event(
        _input("three", external_event_id="three", occurred_at=NOW + timedelta(hours=4)),
        _policy(),
    )
    config = SegmenterConfig(max_gap=timedelta(hours=2))

    ordered = segment_events([first, second, third], config)
    shuffled = segment_events([third, first, second], config)

    assert ordered == shuffled
    assert len(ordered) == 2
    assert ordered[0].event_ids == (first.event_id, second.event_id)
    assert ordered[0].segmentation_reason == "time_gap"
    assert ordered[1].segmentation_reason == "end_of_input"


def test_ingest_service_persists_storage_shaped_event_and_episode(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    store.create_source(
        source_type="codex_task",
        source_fingerprint="source-fingerprint",
        source_id="codex:task-1",
    )
    store.create_partition(
        source_id="codex:task-1",
        partition_fingerprint="partition-fingerprint",
        external_partition_id="task-1",
        partition_id="task-1",
    )
    service = IngestService(store)

    result = service.ingest_event(_input("persisted"), _policy())
    secret = service.ingest_event(
        _input("note token=abcdefghijklmnop", external_event_id="message-secret"),
        _policy(),
    )
    episode = segment_events([result.event])[0]

    assert result.persisted
    assert service.persist_episode(episode)
    with store.connection() as connection:
        event_rows = connection.execute(
            "SELECT content_text, sensitivity FROM events ORDER BY external_event_id"
        ).fetchall()
        assert [tuple(row) for row in event_rows] == [
            ("persisted", "normal"),
            (f"note {REDACTED_SECRET}", "secret"),
        ]
        assert "abcdefghijklmnop" not in repr(event_rows)
        assert secret.event.sensitivity is Sensitivity.SECRET
        assert connection.execute("SELECT content_text FROM episodes").fetchone()[0] == "persisted"


def _register_partition(store: MemoryStore, source_id: str, partition_id: str) -> None:
    store.create_source(
        source_type="codex_task",
        source_fingerprint=f"source:{source_id}",
        source_id=source_id,
    )
    store.create_partition(
        source_id=source_id,
        partition_fingerprint=f"partition:{source_id}:{partition_id}",
        partition_id=partition_id,
    )


def _episode_event(
    *,
    source_id: str,
    partition_id: str,
    external_event_id: str,
    occurred_at: datetime,
    content: str = "same words",
) -> tuple[IngestEventInput, SourcePolicy]:
    return (
        IngestEventInput(
            source_id=source_id,
            partition_id=partition_id,
            source_kind=SourceKind.CODEX_TASK,
            external_event_id=external_event_id,
            occurred_at=occurred_at,
            content=content,
        ),
        SourcePolicy(
            source_id=source_id,
            source_kind=SourceKind.CODEX_TASK,
            opted_in=True,
            allow_model_egress=True,
        ),
    )


def test_same_session_replay_converges_to_one_event_and_episode(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _register_partition(store, "source-a", "partition-a")
    service = IngestService(store)
    event, policy = _episode_event(
        source_id="source-a",
        partition_id="partition-a",
        external_event_id="event-a",
        occurred_at=NOW,
    )

    first = service.ingest_event(event, policy).event
    second = service.ingest_event(event, policy).event
    first_episode = segment_events([first])[0]
    second_episode = segment_events([second])[0]

    assert first == second
    assert first_episode == second_episode
    assert service.persist_episode(first_episode)
    assert service.persist_episode(second_episode)
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM episode_events").fetchone()[0] == 1


def test_identical_content_in_different_sources_or_partitions_does_not_merge(
    tmp_path: Path,
) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    for source_id, partition_id in (
        ("source-a", "partition-a"),
        ("source-b", "partition-b"),
        ("source-a", "partition-c"),
    ):
        _register_partition(store, source_id, partition_id)
    service = IngestService(store)
    episodes = []
    for index, (source_id, partition_id) in enumerate(
        (
            ("source-a", "partition-a"),
            ("source-b", "partition-b"),
            ("source-a", "partition-c"),
        )
    ):
        event, policy = _episode_event(
            source_id=source_id,
            partition_id=partition_id,
            external_event_id=f"event-{index}",
            occurred_at=NOW,
        )
        prepared = service.ingest_event(event, policy).event
        episode = segment_events([prepared])[0]
        service.persist_episode(episode)
        episodes.append(episode)

    assert len({episode.episode_id for episode in episodes}) == 3
    assert len({episode.content_fingerprint for episode in episodes}) == 1
    with store.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM episodes").fetchone()[0] == 3


def test_identical_text_with_different_event_ids_and_times_remains_distinct(
    tmp_path: Path,
) -> None:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _register_partition(store, "source-a", "partition-a")
    service = IngestService(store)
    prepared = []
    for external_event_id, occurred_at in (
        ("event-early", NOW),
        ("event-late", NOW + timedelta(hours=4)),
    ):
        event, policy = _episode_event(
            source_id="source-a",
            partition_id="partition-a",
            external_event_id=external_event_id,
            occurred_at=occurred_at,
        )
        prepared.append(service.ingest_event(event, policy).event)

    episodes = segment_events(prepared, SegmenterConfig(max_gap=timedelta(hours=2)))
    assert len(episodes) == 2
    assert episodes[0].content_fingerprint == episodes[1].content_fingerprint
    assert episodes[0].episode_id != episodes[1].episode_id
    for episode in episodes:
        service.persist_episode(episode)
    with store.connection() as connection:
        rows = connection.execute(
            "SELECT event_sequence_fingerprint FROM episodes ORDER BY occurred_from"
        ).fetchall()
    assert len(rows) == 2
    assert rows[0][0] != rows[1][0]

from __future__ import annotations

from pathlib import Path

import pytest

from local_dreaming.config import ModelSettings
from local_dreaming.orchestration import enqueue_phase1_jobs
from local_dreaming.queue_reconciliation import (
    Phase1ReconciliationDisposition,
    plan_phase1_queue_reconciliation,
    reactivate_phase1_barriers,
    reconcile_phase1_queue,
)
from local_dreaming.storage import (
    EpisodeInput,
    EventInput,
    MemoryStore,
    OperationsStore,
    fingerprint,
)


def _episode(memory: MemoryStore, *, content: str, suffix: str) -> str:
    source_id = memory.create_source(
        source_type="codex_task",
        source_fingerprint=f"source-{suffix}",
        trust_level="conversation",
        sensitivity="normal",
        model_egress_allowed=True,
        source_id=f"source-{suffix}",
    )
    partition_id = memory.create_partition(
        source_id=source_id,
        partition_fingerprint=f"partition-{suffix}",
        external_partition_id=f"partition-{suffix}",
        partition_id=f"partition-{suffix}",
    )
    event_id = memory.create_event(
        EventInput(
            source_id=source_id,
            partition_id=partition_id,
            event_type="user",
            content_text=content,
            content_fingerprint=fingerprint("content", content),
            parser_version="test-v1",
            redactor_version="test-v1",
            external_event_id=f"event-{suffix}",
            event_id=f"event-{suffix}",
        )
    )
    return memory.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id=partition_id,
            episode_type="conversation",
            title=None,
            content_text=content,
            content_fingerprint=fingerprint("episode-content", content),
            segmenter_version="v1",
            segmentation_reason="test",
            sensitivity="normal",
            occurred_from=None,
            occurred_to=None,
            event_ids=(event_id,),
        )
    )


def _legacy_job(
    memory: MemoryStore,
    operations: OperationsStore,
    episode_id: str,
    *,
    suffix: str,
    last_error: str | None = None,
) -> str:
    job_id = operations.enqueue_job(
        job_type="phase1",
        dedupe_key=fingerprint("legacy-prompt-v2.2", episode_id),
        payload={"episode_id": episode_id},
        priority=10,
        job_id=f"legacy-{suffix}",
    )
    if last_error is not None:
        with operations.transaction() as connection:
            connection.execute(
                "UPDATE jobs SET attempts = 1, last_error = ? WHERE job_id = ?",
                (last_error, job_id),
            )
    return job_id


def test_worker_protocol_job_is_deferred_with_current_identity_barrier(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id = _episode(memory, content="bounded user evidence", suffix="protocol")
    legacy = _legacy_job(
        memory,
        operations,
        episode_id,
        suffix="protocol",
        last_error="WorkerProtocolError",
    )

    dry_run = reconcile_phase1_queue(
        memory,
        operations,
        job_allowlist=(legacy,),
        dry_run=True,
    )
    assert dry_run.items[0].disposition is Phase1ReconciliationDisposition.DEFERRED_PROTOCOL
    with operations.connection() as connection:
        assert (
            connection.execute("SELECT status FROM jobs WHERE job_id = ?", (legacy,)).fetchone()[0]
            == "queued"
        )

    applied = reconcile_phase1_queue(
        memory,
        operations,
        job_allowlist=(legacy,),
        dry_run=False,
    )
    barrier = applied.barrier_job_ids[0]
    with operations.connection() as connection:
        rows = connection.execute(
            "SELECT job_id, status, attempts, last_error FROM jobs ORDER BY job_id"
        ).fetchall()
    assert {str(row["job_id"]): str(row["status"]) for row in rows} == {
        legacy: "cancelled",
        barrier: "cancelled",
    }
    assert next(row for row in rows if row["job_id"] == legacy)["attempts"] == 1
    assert all(str(row["last_error"]).startswith("queue-reconciliation-v1") for row in rows)
    assert enqueue_phase1_jobs(memory, operations).queued == 0

    replay = plan_phase1_queue_reconciliation(
        memory,
        operations,
        job_allowlist=(legacy,),
    )
    assert replay.items[0].disposition is Phase1ReconciliationDisposition.ALREADY_RECONCILED
    with pytest.raises(ValueError, match="live doctor"):
        reactivate_phase1_barriers(
            operations,
            barrier_job_allowlist=(barrier,),
            live_doctor_verified=False,
        )
    assert reactivate_phase1_barriers(
        operations,
        barrier_job_allowlist=(barrier,),
        live_doctor_verified=True,
        dry_run=False,
    ) == (barrier,)
    with operations.connection() as connection:
        activated = connection.execute(
            "SELECT status, attempts FROM jobs WHERE job_id = ?", (barrier,)
        ).fetchone()
    assert dict(activated) == {"status": "queued", "attempts": 0}


def test_oversized_job_gets_permanent_barrier_and_cannot_reactivate(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id = _episode(memory, content="x" * 12_001, suffix="oversized")
    legacy = _legacy_job(memory, operations, episode_id, suffix="oversized")

    report = reconcile_phase1_queue(
        memory,
        operations,
        job_allowlist=(legacy,),
        dry_run=False,
    )
    assert report.items[0].disposition is Phase1ReconciliationDisposition.PERMANENT_OVERSIZED
    barrier = report.barrier_job_ids[0]
    with pytest.raises(ValueError, match="permanent"):
        reactivate_phase1_barriers(
            operations,
            barrier_job_allowlist=(barrier,),
            live_doctor_verified=True,
        )


def test_exact_allowlist_and_leased_job_abort_without_partial_transition(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    first = _legacy_job(
        memory,
        operations,
        _episode(memory, content="one", suffix="one"),
        suffix="one",
    )
    second = _legacy_job(
        memory,
        operations,
        _episode(memory, content="two", suffix="two"),
        suffix="two",
    )
    leased = operations.lease_job(owner="other-worker")
    assert leased is not None and leased["job_id"] == first

    with pytest.raises(ValueError, match="leased"):
        reconcile_phase1_queue(
            memory,
            operations,
            job_allowlist=(first, second),
            dry_run=False,
        )
    with operations.connection() as connection:
        statuses = {
            str(row["job_id"]): str(row["status"])
            for row in connection.execute("SELECT job_id, status FROM jobs")
        }
    assert statuses == {first: "leased", second: "queued"}
    with pytest.raises(ValueError, match="unknown"):
        plan_phase1_queue_reconciliation(
            memory,
            operations,
            job_allowlist=("missing",),
        )


def test_current_ready_job_is_left_queued(tmp_path: Path) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    episode_id = _episode(memory, content="current bounded evidence", suffix="current")
    queued = enqueue_phase1_jobs(
        memory,
        operations,
        episode_allowlist=(episode_id,),
        models=ModelSettings(),
    )
    current = queued.job_ids[0]

    report = reconcile_phase1_queue(
        memory,
        operations,
        job_allowlist=(current,),
        dry_run=False,
    )
    assert report.items[0].disposition is Phase1ReconciliationDisposition.CURRENT_READY
    assert report.transitioned_job_ids == ()
    with operations.connection() as connection:
        row = connection.execute("SELECT status FROM jobs WHERE job_id = ?", (current,)).fetchone()
    assert row[0] == "queued"

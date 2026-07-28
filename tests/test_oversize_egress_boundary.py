from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from test_pipeline import _budget, _job, _Operations, _phase1_payload, _pipeline, _Sink, _Worker

from local_dreaming.errors import OversizedInputError
from local_dreaming.evidence import MAX_MODEL_INPUT_CHARS
from local_dreaming.orchestration import DatabaseJobLoader, enqueue_phase1_jobs
from local_dreaming.pipeline import PipelinePhase, PipelineStatus, prepare_invocation
from local_dreaming.storage import EpisodeInput, EventInput, MemoryStore, OperationsStore


@pytest.mark.parametrize("phase", (PipelinePhase.PHASE1, PipelinePhase.PHASE2))
def test_pipeline_rejects_oversized_loader_input_before_metadata_or_worker_on_retry(
    phase: PipelinePhase,
) -> None:
    operations = _Operations()
    worker = _Worker(_phase1_payload())
    pipeline = _pipeline(worker, operations, _Sink())
    oversized = replace(
        _job(),
        phase=phase,
        model_input="界" * (MAX_MODEL_INPUT_CHARS + 1),
        episode_count=0 if phase is PipelinePhase.PHASE2 else 1,
    )

    first = pipeline.run_leased_job(
        oversized,
        lease_owner="nightly-1",
        budget=_budget(),
    )
    retry = pipeline.run_leased_job(
        replace(oversized, attempt=2),
        lease_owner="nightly-1",
        budget=_budget(),
    )

    assert first.status is PipelineStatus.QUARANTINED
    assert retry.status is PipelineStatus.QUARANTINED
    assert first.error_code == retry.error_code == "OversizedInputError"
    assert not worker.requests
    assert not operations.created
    assert not operations.finished
    assert [failure[2] for failure in operations.failed] == [
        "OversizedInputError",
        "OversizedInputError",
    ]


def test_exact_char_boundary_is_allowed_and_input_bytes_are_actual_worker_stdin() -> None:
    operations = _Operations()
    worker = _Worker(_phase1_payload())
    pipeline = _pipeline(worker, operations, _Sink())
    job = replace(_job(), model_input="界" * MAX_MODEL_INPUT_CHARS)
    expected_plan = prepare_invocation(job, pipeline.models)

    outcome = pipeline.run_leased_job(
        job,
        lease_owner="nightly-1",
        budget=_budget(),
    )

    assert outcome.status is PipelineStatus.COMPLETED
    assert len(worker.requests) == 1
    assert operations.created[0]["input_bytes"] == len(expected_plan.prompt.encode("utf-8"))
    assert operations.created[0]["input_bytes"] == len(worker.requests[0].prompt.encode("utf-8"))
    assert operations.created[0]["input_bytes"] > len(job.model_input.encode("utf-8"))


def test_queued_oversized_phase1_retry_reloads_and_never_creates_model_call(
    tmp_path: Path,
) -> None:
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    operations = OperationsStore(tmp_path / "operations.sqlite3")
    source_id = memory.create_source(
        source_type="codex_task",
        source_fingerprint="oversized-retry-source",
        model_egress_allowed=True,
    )
    partition_id = memory.create_partition(
        source_id=source_id,
        partition_fingerprint="oversized-retry-partition",
    )
    event_id = memory.create_event(
        EventInput(
            source_id=source_id,
            partition_id=partition_id,
            external_event_id="oversized-retry-event",
            event_type="codex_task",
            content_text="X" * (MAX_MODEL_INPUT_CHARS + 1),
            content_fingerprint="oversized-retry-content",
            parser_version="test-v1",
            redactor_version="test-v1",
            occurred_at="2026-07-22T00:00:00Z",
            metadata={"role": "user", "source_sequence": 1},
        )
    )
    episode_id = memory.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id=partition_id,
            episode_type="task",
            content_text="X" * (MAX_MODEL_INPUT_CHARS + 1),
            content_fingerprint="oversized-retry-episode",
            segmenter_version="test-v1",
            segmentation_reason="legacy_queue",
            event_ids=(event_id,),
        )
    )
    enqueue_phase1_jobs(memory, operations, episode_allowlist=(episode_id,))
    loader = DatabaseJobLoader(memory.path)

    first = operations.lease_job(owner="nightly", job_types=("phase1",))
    assert first is not None
    with pytest.raises(OversizedInputError):
        loader.load(first)
    assert operations.fail_job(
        str(first["job_id"]),
        owner="nightly",
        error=OversizedInputError.__name__,
        retry_at="2000-01-01T00:00:00Z",
    )

    retry = operations.lease_job(owner="nightly", job_types=("phase1",))
    assert retry is not None
    assert retry["attempts"] == 2
    with pytest.raises(OversizedInputError):
        loader.load(retry)

    with operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0] == 0
        row = connection.execute(
            "SELECT last_error FROM jobs WHERE job_id = ?", (retry["job_id"],)
        ).fetchone()
        assert row is not None
        assert row[0] == "OversizedInputError"

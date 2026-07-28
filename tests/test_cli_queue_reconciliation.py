from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

import local_dreaming.cli as cli_module
from local_dreaming.cli import app
from local_dreaming.storage import (
    EpisodeInput,
    EventInput,
    MemoryStore,
    OperationsStore,
    fingerprint,
)

runner = CliRunner()


def _env(runtime_home: Path) -> dict[str, str]:
    return {"LOCAL_DREAMING_HOME": str(runtime_home)}


def _legacy_job(runtime_home: Path, *, last_error: str = "WorkerProtocolError") -> str:
    memory = MemoryStore(runtime_home / "data" / "memory.sqlite3")
    operations = OperationsStore(runtime_home / "data" / "operations.sqlite3")
    source_id = memory.create_source(
        source_type="codex_task",
        source_fingerprint="cli-reconcile-source",
        model_egress_allowed=True,
    )
    partition_id = memory.create_partition(
        source_id=source_id,
        partition_fingerprint="cli-reconcile-partition",
    )
    event_id = memory.create_event(
        EventInput(
            source_id=source_id,
            partition_id=partition_id,
            external_event_id="cli-reconcile-event",
            event_type="codex_task",
            content_text="bounded evidence",
            content_fingerprint=fingerprint("cli-reconcile-content"),
            parser_version="test-v1",
            redactor_version="test-v1",
        )
    )
    episode_id = memory.create_episode(
        EpisodeInput(
            source_id=source_id,
            partition_id=partition_id,
            episode_type="task",
            content_text="bounded evidence",
            content_fingerprint=fingerprint("cli-reconcile-episode"),
            segmenter_version="test-v1",
            segmentation_reason="legacy_queue",
            event_ids=(event_id,),
        )
    )
    job_id = operations.enqueue_job(
        job_type="phase1",
        dedupe_key=fingerprint("legacy-prompt-v2.2", episode_id),
        payload={"episode_id": episode_id},
    )
    with operations.transaction() as connection:
        connection.execute(
            "UPDATE jobs SET attempts = 1, last_error = ? WHERE job_id = ?",
            (last_error, job_id),
        )
    return job_id


def test_queue_reconcile_defaults_to_byte_stable_dry_run(tmp_path: Path) -> None:
    runtime_home = tmp_path / "runtime"
    job_id = _legacy_job(runtime_home)
    operations_path = runtime_home / "data" / "operations.sqlite3"
    before = operations_path.read_bytes()

    result = runner.invoke(
        app,
        ["queue-reconcile", "--job-id", job_id, "--json"],
        env=_env(runtime_home),
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert payload["items"][0]["disposition"] == "deferred_protocol"
    assert operations_path.read_bytes() == before


def test_queue_reconcile_apply_then_live_doctor_reactivate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime_home = tmp_path / "runtime"
    job_id = _legacy_job(runtime_home)

    applied = runner.invoke(
        app,
        ["queue-reconcile", "--job-id", job_id, "--apply", "--json"],
        env=_env(runtime_home),
    )
    assert applied.exit_code == 0, applied.output
    barrier = json.loads(applied.stdout)["barrier_job_ids"][0]

    calls: list[bool] = []

    def _certified(*_args, **kwargs):
        calls.append(bool(kwargs.get("live_probe")))
        return SimpleNamespace(certified=True)

    monkeypatch.setattr(cli_module, "run_doctor", _certified)
    reactivated = runner.invoke(
        app,
        ["queue-reactivate", "--barrier-job-id", barrier, "--apply", "--json"],
        env=_env(runtime_home),
    )

    assert reactivated.exit_code == 0, reactivated.output
    assert calls == [True]
    payload = json.loads(reactivated.stdout)
    assert payload["live_doctor_executed"] is True
    with OperationsStore(
        operations_path := runtime_home / "data" / "operations.sqlite3"
    ).connection() as connection:
        row = connection.execute(
            "SELECT status, attempts FROM jobs WHERE job_id = ?", (barrier,)
        ).fetchone()
    assert dict(row) == {"status": "queued", "attempts": 0}
    assert operations_path.is_file()


def test_queue_reactivate_apply_fails_closed_when_live_doctor_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime_home = tmp_path / "runtime"
    job_id = _legacy_job(runtime_home)
    applied = runner.invoke(
        app,
        ["queue-reconcile", "--job-id", job_id, "--apply", "--json"],
        env=_env(runtime_home),
    )
    barrier = json.loads(applied.stdout)["barrier_job_ids"][0]
    monkeypatch.setattr(
        cli_module,
        "run_doctor",
        lambda *_args, **_kwargs: SimpleNamespace(certified=False),
    )

    result = runner.invoke(
        app,
        ["queue-reactivate", "--barrier-job-id", barrier, "--apply", "--json"],
        env=_env(runtime_home),
    )

    assert result.exit_code != 0
    with OperationsStore(runtime_home / "data" / "operations.sqlite3").connection() as connection:
        assert (
            connection.execute("SELECT status FROM jobs WHERE job_id = ?", (barrier,)).fetchone()[0]
            == "cancelled"
        )


def test_runtime_provenance_includes_queue_boundary(tmp_path: Path) -> None:
    result = runner.invoke(app, ["runtime-provenance", "--json"], env=_env(tmp_path))

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["package_version"] == "0.1.0"
    assert "queue_reconciliation.py" in {record["relative_path"] for record in payload["files"]}

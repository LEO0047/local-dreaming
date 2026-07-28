from __future__ import annotations

import plistlib
import sqlite3
import subprocess
from pathlib import Path

import pytest

from local_dreaming.scheduler import (
    DOCTOR_LABEL,
    LABEL,
    ScheduleScanError,
    bootstrap_launch_agent,
    doctor_launch_agent_payload,
    find_openclaw_schedule_conflicts,
    find_schedule_conflicts,
    launch_agent_payload,
    render_launch_agent,
    write_launch_agent,
)


def test_launch_agent_uses_minimal_environment(tmp_path: Path) -> None:
    payload = launch_agent_payload(
        dream_executable=tmp_path / "dream",
        runtime_home=tmp_path / "runtime",
    )

    assert payload["StartCalendarInterval"] == {"Hour": 4, "Minute": 0}
    assert payload["ProgramArguments"][1] == "-i"
    assert payload["ProgramArguments"][-1] == "run-nightly"


def test_weekly_doctor_agent_is_separate_and_synthetic(tmp_path: Path) -> None:
    payload = doctor_launch_agent_payload(
        dream_executable=tmp_path / "dream",
        runtime_home=tmp_path / "runtime",
    )

    assert payload["Label"] == DOCTOR_LABEL
    assert payload["StartCalendarInterval"] == {"Hour": 2, "Minute": 0, "Weekday": 0}
    assert payload["ProgramArguments"][-3:] == ["doctor", "--live", "--json"]
    assert payload["StandardOutPath"].endswith("doctor.stdout.log")


def test_conflict_detection_and_install_gate(tmp_path: Path) -> None:
    agents = tmp_path / "agents"
    agents.mkdir()
    with (agents / "other.plist").open("wb") as handle:
        plistlib.dump(
            {"Label": "other", "StartCalendarInterval": {"Hour": 3, "Minute": 50}},
            handle,
        )
    conflicts = find_schedule_conflicts(agents)
    assert [conflict.label for conflict in conflicts] == ["other"]

    destination = agents / "dream.plist"
    payload = render_launch_agent(
        dream_executable=tmp_path / "dream",
        runtime_home=tmp_path / "runtime",
    )
    with pytest.raises(ValueError, match="seven"):
        write_launch_agent(
            destination,
            payload,
            verified_manual_runs=6,
            conflicts=[],
            worker_certified=True,
        )
    with pytest.raises(ValueError, match="certification"):
        write_launch_agent(
            destination,
            payload,
            verified_manual_runs=7,
            conflicts=[],
            worker_certified=False,
        )
    with pytest.raises(ValueError, match="conflict"):
        write_launch_agent(
            destination,
            payload,
            verified_manual_runs=7,
            conflicts=conflicts,
            worker_certified=True,
        )

    write_launch_agent(
        destination,
        payload,
        verified_manual_runs=7,
        conflicts=[],
        worker_certified=True,
    )
    assert destination.stat().st_mode & 0o777 == 0o600


def test_openclaw_conflict_scan_is_read_only_and_handles_cron_steps(tmp_path: Path) -> None:
    database = tmp_path / "openclaw.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE cron_jobs(
                display_name TEXT, name TEXT NOT NULL, enabled INTEGER NOT NULL,
                schedule_kind TEXT NOT NULL, schedule_expr TEXT, schedule_tz TEXT
            )
            """
        )
        connection.executemany(
            "INSERT INTO cron_jobs VALUES (?, ?, 1, 'cron', ?, ?)",
            [
                ("Memory Dreaming", "memory", "0 3 * * *", None),
                ("Nearby step", "step", "*/15 3 * * *", "Asia/Taipei"),
                ("Morning", "morning", "0 7 * * *", "Asia/Taipei"),
                ("UTC task", "utc", "30 3 * * *", "UTC"),
            ],
        )

    conflicts = find_openclaw_schedule_conflicts(database, hour=3, minute=30)

    assert [(item.label, item.hour, item.minute) for item in conflicts] == [
        ("OpenClaw: Memory Dreaming", 3, 0),
        ("OpenClaw: Nearby step", 3, 30),
    ]
    assert [
        (item.label, item.hour, item.minute)
        for item in find_openclaw_schedule_conflicts(database, hour=4, minute=0)
    ] == [("OpenClaw: Nearby step", 3, 45)]


def test_strict_scans_fail_closed(tmp_path: Path) -> None:
    agents = tmp_path / "agents"
    agents.mkdir()
    (agents / "broken.plist").write_text("not a plist", encoding="utf-8")
    with pytest.raises(ScheduleScanError, match="cannot inspect"):
        find_schedule_conflicts(agents, strict=True)

    database = tmp_path / "broken.sqlite"
    database.write_bytes(b"not sqlite")
    with pytest.raises(ScheduleScanError, match="OpenClaw"):
        find_openclaw_schedule_conflicts(database, strict=True)


def test_bootstrap_verifies_and_rolls_back_failures(tmp_path: Path) -> None:
    destination = tmp_path / "nightly.plist"
    destination.write_text("plist", encoding="utf-8")
    commands: list[tuple[str, ...]] = []

    def successful_runner(
        command: tuple[str, ...], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        commands.append(command)
        returncode = 0 if command[1] == "bootstrap" or len(commands) == 3 else 1
        return subprocess.CompletedProcess(command, returncode, "", "")

    assert bootstrap_launch_agent(
        destination,
        label=LABEL,
        runner=successful_runner,
    )
    assert [command[1] for command in commands] == ["print", "bootstrap", "print"]

    failed_destination = tmp_path / "doctor.plist"
    failed_destination.write_text("plist", encoding="utf-8")

    def failed_runner(
        command: tuple[str, ...], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(command, 1, "", "failed")

    with pytest.raises(RuntimeError, match="bootstrap"):
        bootstrap_launch_agent(
            failed_destination,
            label=DOCTOR_LABEL,
            runner=failed_runner,
        )
    assert not failed_destination.exists()

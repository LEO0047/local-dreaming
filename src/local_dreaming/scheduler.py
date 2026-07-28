from __future__ import annotations

import os
import plistlib
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LABEL = "com.leo.local-dreaming.nightly"
DOCTOR_LABEL = "com.leo.local-dreaming.doctor"
DEFAULT_HOUR = 4
DEFAULT_MINUTE = 0
DOCTOR_HOUR = 2
DOCTOR_MINUTE = 0
DOCTOR_WEEKDAY = 0


class ScheduleScanError(RuntimeError):
    """Raised when a conflict source cannot be inspected safely."""


@dataclass(frozen=True, slots=True)
class ScheduleConflict:
    path: Path
    label: str
    hour: int
    minute: int


def launch_agent_payload(
    *,
    dream_executable: Path,
    runtime_home: Path,
    hour: int = DEFAULT_HOUR,
    minute: int = DEFAULT_MINUTE,
) -> dict[str, Any]:
    """Build a minimal launchd user-agent definition without installing it."""

    return _launch_agent_payload(
        label=LABEL,
        dream_executable=dream_executable,
        runtime_home=runtime_home,
        arguments=("run-nightly",),
        log_name="nightly",
        hour=hour,
        minute=minute,
    )


def doctor_launch_agent_payload(
    *,
    dream_executable: Path,
    runtime_home: Path,
    hour: int = DOCTOR_HOUR,
    minute: int = DOCTOR_MINUTE,
    weekday: int = DOCTOR_WEEKDAY,
) -> dict[str, Any]:
    """Build the separate weekly synthetic doctor refresh job."""

    if not 0 <= weekday <= 7:
        raise ValueError("weekday must be between 0 and 7")
    return _launch_agent_payload(
        label=DOCTOR_LABEL,
        dream_executable=dream_executable,
        runtime_home=runtime_home,
        arguments=("doctor", "--live", "--json"),
        log_name="doctor",
        hour=hour,
        minute=minute,
        weekday=weekday,
    )


def _launch_agent_payload(
    *,
    label: str,
    dream_executable: Path,
    runtime_home: Path,
    arguments: tuple[str, ...],
    log_name: str,
    hour: int,
    minute: int,
    weekday: int | None = None,
) -> dict[str, Any]:
    if not dream_executable.is_absolute() or not runtime_home.is_absolute():
        raise ValueError("launchd executable and runtime paths must be absolute")
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("schedule hour or minute is out of range")
    interval = {"Hour": hour, "Minute": minute}
    if weekday is not None:
        interval["Weekday"] = weekday

    return {
        "Label": label,
        "ProgramArguments": [
            "/usr/bin/env",
            "-i",
            f"HOME={Path.home()}",
            "PATH=/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin",
            f"LOCAL_DREAMING_HOME={runtime_home}",
            str(dream_executable),
            *arguments,
        ],
        "WorkingDirectory": str(runtime_home),
        "StartCalendarInterval": interval,
        "RunAtLoad": False,
        "ProcessType": "Background",
        "LowPriorityIO": True,
        "StandardOutPath": str(runtime_home / "logs" / f"{log_name}.stdout.log"),
        "StandardErrorPath": str(runtime_home / "logs" / f"{log_name}.stderr.log"),
    }


def render_launch_agent(**kwargs: Any) -> bytes:
    return plistlib.dumps(launch_agent_payload(**kwargs), sort_keys=True)


def render_doctor_launch_agent(**kwargs: Any) -> bytes:
    return plistlib.dumps(doctor_launch_agent_payload(**kwargs), sort_keys=True)


def find_schedule_conflicts(
    launch_agents: Path,
    *,
    hour: int = DEFAULT_HOUR,
    minute: int = DEFAULT_MINUTE,
    window_minutes: int = 15,
    strict: bool = False,
    exclude_labels: set[str] | None = None,
) -> list[ScheduleConflict]:
    """Read existing plists and report calendar jobs in the requested time window."""

    if not launch_agents.exists():
        return []
    if launch_agents.is_symlink() or not launch_agents.is_dir():
        if strict:
            raise ScheduleScanError(f"invalid LaunchAgents directory: {launch_agents}")
        return []
    excluded = exclude_labels or set()
    target = hour * 60 + minute
    conflicts: list[ScheduleConflict] = []
    for path in sorted(launch_agents.glob("*.plist")):
        try:
            if path.is_symlink():
                raise OSError("symlinked plist is not trusted")
            with path.open("rb") as handle:
                payload = plistlib.load(handle)
        except (OSError, plistlib.InvalidFileException) as exc:
            if strict:
                raise ScheduleScanError(f"cannot inspect launchd plist: {path.name}") from exc
            continue
        label = str(payload.get("Label", path.stem))
        if label in excluded:
            continue
        intervals = payload.get("StartCalendarInterval")
        if isinstance(intervals, dict):
            intervals = [intervals]
        if not isinstance(intervals, list):
            if strict and intervals is not None:
                raise ScheduleScanError(f"invalid calendar interval: {path.name}")
            continue
        for interval in intervals:
            if not isinstance(interval, dict):
                if strict:
                    raise ScheduleScanError(f"invalid calendar interval: {path.name}")
                continue
            existing_hour = interval.get("Hour")
            existing_minute = interval.get("Minute", 0)
            if not isinstance(existing_hour, int) or not isinstance(existing_minute, int):
                if strict:
                    raise ScheduleScanError(f"invalid calendar time: {path.name}")
                continue
            distance = abs((existing_hour * 60 + existing_minute) - target)
            distance = min(distance, 24 * 60 - distance)
            if distance <= window_minutes:
                conflicts.append(
                    ScheduleConflict(
                        path=path,
                        label=label,
                        hour=existing_hour,
                        minute=existing_minute,
                    )
                )
    return conflicts


def _cron_values(field: str, minimum: int, maximum: int) -> set[int] | None:
    """Expand the minute/hour subset of standard five-field cron syntax."""

    values: set[int] = set()
    for raw_part in field.split(","):
        part = raw_part.strip()
        if not part:
            return None
        base, separator, step_text = part.partition("/")
        try:
            step = int(step_text) if separator else 1
        except ValueError:
            return None
        if step <= 0:
            return None
        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            try:
                start, end = int(start_text), int(end_text)
            except ValueError:
                return None
        else:
            try:
                start = end = int(base)
            except ValueError:
                return None
        if start < minimum or end > maximum or end < start:
            return None
        values.update(range(start, end + 1, step))
    return values


def find_openclaw_schedule_conflicts(
    database: Path,
    *,
    hour: int = DEFAULT_HOUR,
    minute: int = DEFAULT_MINUTE,
    window_minutes: int = 30,
    target_timezone: str = "Asia/Taipei",
    strict: bool = False,
) -> list[ScheduleConflict]:
    """Read OpenClaw's cron registry without touching payloads or runtime state."""

    if not database.is_file():
        return []
    if database.is_symlink():
        if strict:
            raise ScheduleScanError("OpenClaw schedule database is a symlink")
        return []
    uri = f"file:{database.resolve()}?mode=ro"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=2.0)
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT COALESCE(display_name, name) AS label, schedule_expr, schedule_tz
            FROM cron_jobs
            WHERE enabled = 1 AND schedule_kind = 'cron' AND schedule_expr IS NOT NULL
            ORDER BY label
            """
        ).fetchall()
    except sqlite3.Error as exc:
        if strict:
            raise ScheduleScanError("cannot inspect OpenClaw schedule database") from exc
        return []
    finally:
        if connection is not None:
            connection.close()

    target = hour * 60 + minute
    conflicts: list[ScheduleConflict] = []
    for row in rows:
        timezone = str(row["schedule_tz"] or target_timezone)
        if timezone != target_timezone:
            if strict:
                raise ScheduleScanError(f"unsupported OpenClaw schedule timezone: {timezone}")
            continue
        fields = str(row["schedule_expr"]).split()
        if len(fields) != 5:
            if strict:
                raise ScheduleScanError(f"invalid OpenClaw cron: {row['label']}")
            continue
        minutes = _cron_values(fields[0], 0, 59)
        hours = _cron_values(fields[1], 0, 23)
        if minutes is None or hours is None:
            if strict:
                raise ScheduleScanError(f"unsupported OpenClaw cron: {row['label']}")
            continue
        matching: list[tuple[int, int, int]] = []
        for existing_hour in hours:
            for existing_minute in minutes:
                scheduled = existing_hour * 60 + existing_minute
                distance = abs(scheduled - target)
                distance = min(distance, 24 * 60 - distance)
                if distance <= window_minutes:
                    matching.append((distance, existing_hour, existing_minute))
        if matching:
            _, existing_hour, existing_minute = min(matching)
            conflicts.append(
                ScheduleConflict(
                    path=database,
                    label=f"OpenClaw: {row['label']}",
                    hour=existing_hour,
                    minute=existing_minute,
                )
            )
    return conflicts


def write_launch_agent(
    destination: Path,
    payload: bytes,
    *,
    verified_manual_runs: int,
    conflicts: list[ScheduleConflict],
    worker_certified: bool,
) -> None:
    """Install the plist only after the explicit practical rollout gates pass."""

    if not worker_certified:
        raise ValueError("a current live worker certification is required")
    if verified_manual_runs < 7:
        raise ValueError("seven verified manual bounded runs are required")
    if conflicts:
        names = ", ".join(conflict.label for conflict in conflicts)
        raise ValueError(f"schedule conflict detected: {names}")
    if not destination.is_absolute() or destination.is_symlink():
        raise ValueError("launchd destination must be an absolute non-symlink path")
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination.parent.chmod(0o700)
    temporary = destination.with_suffix(f".tmp-{os.getpid()}")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, destination)
        parent_descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def bootstrap_launch_agent(
    destination: Path,
    *,
    label: str,
    runner: Any = subprocess.run,
) -> bool:
    """Bootstrap and verify one user LaunchAgent, rolling back a failed install."""

    domain = f"gui/{os.getuid()}"
    service = f"{domain}/{label}"
    common = {
        "capture_output": True,
        "text": True,
        "timeout": 10,
        "check": False,
    }
    already_loaded = runner(("/bin/launchctl", "print", service), **common)
    if already_loaded.returncode == 0:
        return False
    bootstrapped = runner(
        ("/bin/launchctl", "bootstrap", domain, str(destination)),
        **common,
    )
    if bootstrapped.returncode != 0:
        destination.unlink(missing_ok=True)
        raise RuntimeError(f"launchctl bootstrap failed for {label}")
    verified = runner(("/bin/launchctl", "print", service), **common)
    if verified.returncode == 0:
        return True
    runner(("/bin/launchctl", "bootout", service), **common)
    destination.unlink(missing_ok=True)
    raise RuntimeError(f"launchctl verification failed for {label}")

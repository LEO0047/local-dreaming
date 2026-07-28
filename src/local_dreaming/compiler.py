from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

from local_dreaming.artifacts import publish_bundle
from local_dreaming.database import connect_memory, get_memory_revision
from local_dreaming.storage import memory_maintenance_lock


def _clean(value: object) -> str:
    return " ".join(str(value).split())


def _claim_line(row: sqlite3.Row) -> str:
    valid = ""
    if row["valid_from"] or row["valid_to"]:
        valid = f" [{row['valid_from'] or '?'} → {row['valid_to'] or 'present'}]"
    flags = f"{row['status']}; {row['epistemic_status']}"
    pin = "[PINNED] " if row["is_pinned"] else ""
    return (
        f"- {pin}{_clean(row['summary'])}{valid} "
        f"(`{row['claim_id']}`; `{row['predicate']}`; {flags})"
    )


def _document(title: str, revision: int, rows: list[sqlite3.Row]) -> str:
    lines = [f"# {title}", "", f"Memory revision: `{revision}`", ""]
    if not rows:
        lines.append("_No approved memory._")
    else:
        lines.extend(_claim_line(row) for row in rows)
    return "\n".join(lines) + "\n"


def compile_memory(memory_path: Path, artifact_root: Path) -> Path:
    """Compile the canonical database into a deterministic immutable bundle."""

    # A canonical writer normally compiles just after committing.  Hold the same
    # cross-process lock used by writers/forget so an older compiler cannot publish
    # after a newer forget or correction has already committed.
    with memory_maintenance_lock(memory_path):
        return _compile_memory_locked(memory_path, artifact_root)


def _compile_memory_locked(memory_path: Path, artifact_root: Path) -> Path:
    """Compile while the caller holds the canonical maintenance lock."""

    with connect_memory(memory_path) as connection:
        revision = get_memory_revision(connection)
        rows = connection.execute(
            """
            SELECT cv.claim_version_id, cv.claim_id, c.subject_text, c.predicate,
                   c.scope, cv.summary, cv.status, cv.valid_from, cv.valid_to,
                   cv.recorded_revision, cv.epistemic_status, cv.sensitivity,
                   CASE WHEN pin.claim_id IS NULL THEN 0 ELSE 1 END AS is_pinned
            FROM current_claim_versions AS cv
            JOIN claims AS c ON c.claim_id = cv.claim_id
            LEFT JOIN current_claim_pins AS pin ON pin.claim_id = cv.claim_id
            WHERE cv.sensitivity <> 'secret'
            ORDER BY is_pinned DESC, c.scope, c.subject_text, c.predicate,
                     COALESCE(cv.valid_from, ''), cv.recorded_revision,
                     cv.claim_version_id
            """
        ).fetchall()
        revisions = connection.execute(
            """
            SELECT revision, created_at, actor, reason, details_json
            FROM revisions ORDER BY revision
            """
        ).fetchall()
        episode_rows = connection.execute(
            """
            SELECT DISTINCT ep.episode_id, ep.title, ep.occurred_from, ep.occurred_to,
                   c.claim_id, cv.summary
            FROM episodes AS ep
            JOIN episode_events AS ee ON ee.episode_id = ep.episode_id
            JOIN claim_evidence AS ce ON ce.event_id = ee.event_id
            JOIN current_claim_versions AS cv ON cv.claim_version_id = ce.claim_version_id
            JOIN claims AS c ON c.claim_id = cv.claim_id
            WHERE cv.sensitivity <> 'secret'
            ORDER BY ep.episode_id, c.claim_id, cv.summary
            """
        ).fetchall()

    all_rows = list(rows)
    profile = [row for row in all_rows if row["scope"] == "user_profile"]
    preferences = [
        row
        for row in all_rows
        if row["predicate"].startswith("preference.") or "preference" in row["predicate"]
    ]
    projects = [row for row in all_rows if row["scope"] == "project_state"]
    timeline = [row for row in all_rows if row["valid_from"] or row["valid_to"]]

    changelog = ["# Changelog", "", f"Memory revision: `{revision}`", ""]
    for row in revisions:
        details = json.loads(str(row["details_json"]))
        detail_text = (
            f" — `{json.dumps(details, ensure_ascii=False, sort_keys=True)}`" if details else ""
        )
        changelog.append(
            f"- r{row['revision']} · {row['created_at']} · {_clean(row['actor'])} · "
            f"{_clean(row['reason'])}{detail_text}"
        )

    files: dict[str, str] = {
        "PROFILE.md": _document("Profile", revision, profile),
        "PREFERENCES.md": _document("Preferences", revision, preferences),
        "PROJECTS.md": _document("Projects", revision, projects),
        "ACTIVE_CONTEXT.md": _document("Active Context", revision, projects),
        "TIMELINE.md": _document("Timeline", revision, timeline),
        "MEMORY.md": _document("Memory", revision, all_rows),
        "CHANGELOG.md": "\n".join(changelog) + "\n",
    }
    episodes: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in episode_rows:
        episodes[str(row["episode_id"])].append(row)
    for episode_id, episode_claims in sorted(episodes.items()):
        first = episode_claims[0]
        title = _clean(first["title"] or episode_id)
        lines = [f"# {title}", "", f"Episode: `{episode_id}`", ""]
        if first["occurred_from"] or first["occurred_to"]:
            lines.extend(
                [
                    f"Time: `{first['occurred_from'] or '?'}` → `{first['occurred_to'] or '?'}`",
                    "",
                ]
            )
        lines.append("## Approved claims")
        lines.append("")
        for row in episode_claims:
            lines.append(f"- {_clean(row['summary'])} (`{row['claim_id']}`)")
        files[f"episodes/{episode_id}.md"] = "\n".join(lines) + "\n"
    return publish_bundle(artifact_root, revision, files)

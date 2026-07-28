from __future__ import annotations

import json
import re

from local_dreaming.models import DreamingHandoff
from local_dreaming.redaction import redact_secrets

_FIELDS = ("workspace", "state", "completed", "verified", "leo_corrections", "pending")
_MAX_BLOCK_CHARS = 6_000
_MAX_FIELD_CHARS = 1_000
_BLOCK_RE = re.compile(
    r"(?:^|\n)\[DREAMING_HANDOFF\]\n(?P<body>.*?)\n\[/DREAMING_HANDOFF\][ \t]*\Z",
    re.DOTALL,
)


def parse_dreaming_handoff(final_text: str) -> DreamingHandoff | None:
    """Parse one terminal handoff block and classify it as project state.

    Quoted or fenced examples are intentionally ignored so that documentation and
    prompts cannot be mistaken for a real task handoff.
    """

    match = _BLOCK_RE.search(final_text)
    if match is None or len(match.group(0)) > _MAX_BLOCK_CHARS:
        return None
    if _inside_fence(final_text, match.start()):
        return None

    block_lines = match.group(0).lstrip("\n").splitlines()
    if any(line.lstrip().startswith(">") for line in block_lines):
        return None

    values: dict[str, str] = {}
    for line in match.group("body").splitlines():
        if ":" not in line:
            return None
        name, value = line.split(":", 1)
        if name not in _FIELDS or name in values:
            return None
        normalized = value.strip()
        if len(normalized) > _MAX_FIELD_CHARS:
            return None
        values[name] = normalized

    if tuple(values) != _FIELDS or not any(values.values()):
        return None

    sanitized: dict[str, str] = {}
    redaction_count = 0
    for name in _FIELDS:
        result = redact_secrets(values[name])
        sanitized[name] = result.text
        redaction_count += result.redaction_count

    return DreamingHandoff(
        workspace=sanitized["workspace"],
        state=sanitized["state"],
        completed=sanitized["completed"],
        verified=sanitized["verified"],
        leo_corrections=sanitized["leo_corrections"],
        pending=sanitized["pending"],
        redaction_count=redaction_count,
    )


def serialize_dreaming_handoff(handoff: DreamingHandoff) -> str:
    """Return the only handoff payload allowed to enter deterministic ingest."""

    return json.dumps(
        {
            "classification": handoff.classification.value,
            "completed": handoff.completed,
            "leo_corrections": handoff.leo_corrections,
            "pending": handoff.pending,
            "state": handoff.state,
            "verified": handoff.verified,
            "workspace": handoff.workspace,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _inside_fence(text: str, position: int) -> bool:
    prefix = text[:position]
    backtick_fences = len(re.findall(r"(?m)^\s*```", prefix))
    tilde_fences = len(re.findall(r"(?m)^\s*~~~", prefix))
    return backtick_fences % 2 == 1 or tilde_fences % 2 == 1

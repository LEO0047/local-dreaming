from __future__ import annotations

from local_dreaming.handoff import parse_dreaming_handoff
from local_dreaming.models import ClaimScope
from local_dreaming.redaction import REDACTED_SECRET


def _handoff(*, pending: str = "write tests") -> str:
    return f"""Task finished.

[DREAMING_HANDOFF]
workspace: /tmp/project
state: in_progress
completed: storage schema
verified: pytest passed
leo_corrections: token=abcdefghijklmnop
pending: {pending}
[/DREAMING_HANDOFF]"""


def test_parses_only_terminal_handoff_as_project_state_and_redacts_secret() -> None:
    parsed = parse_dreaming_handoff(_handoff())

    assert parsed is not None
    assert parsed.classification is ClaimScope.PROJECT_STATE
    assert parsed.workspace == "/tmp/project"
    assert parsed.leo_corrections == REDACTED_SECRET
    assert parsed.redaction_count == 1


def test_rejects_non_terminal_quoted_and_fenced_examples() -> None:
    assert parse_dreaming_handoff(_handoff() + "\nextra") is None
    assert parse_dreaming_handoff("> " + _handoff().replace("\n", "\n> ")) is None
    assert parse_dreaming_handoff("```text\n" + _handoff() + "\n```") is None


def test_rejects_missing_duplicate_and_oversized_fields() -> None:
    assert parse_dreaming_handoff(_handoff().replace("pending: write tests\n", "")) is None
    duplicate = _handoff().replace("state: in_progress", "state: one\nstate: two")
    assert parse_dreaming_handoff(duplicate) is None
    assert parse_dreaming_handoff(_handoff(pending="x" * 1_001)) is None

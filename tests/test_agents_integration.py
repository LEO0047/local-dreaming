from __future__ import annotations

from pathlib import Path

import pytest

from local_dreaming.agents_integration import (
    END_MARKER,
    START_MARKER,
    install_global_handoff,
    merged_agents_content,
)


def test_handoff_instruction_is_idempotent() -> None:
    first = merged_agents_content("# Rules\n")
    second = merged_agents_content(first)

    assert first == second
    assert first.count(START_MARKER) == first.count(END_MARKER) == 1
    assert "project_state" in first


def test_rejects_incomplete_markers() -> None:
    with pytest.raises(ValueError):
        merged_agents_content(f"# Rules\n{START_MARKER}\n")


def test_install_supports_dry_run(tmp_path: Path) -> None:
    path = tmp_path / "AGENTS.md"
    path.write_text("# Rules\n")
    assert install_global_handoff(path, dry_run=True)
    assert path.read_text() == "# Rules\n"
    assert install_global_handoff(path, dry_run=False)
    assert not install_global_handoff(path, dry_run=False)

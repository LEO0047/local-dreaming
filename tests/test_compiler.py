from __future__ import annotations

from pathlib import Path

from local_dreaming.compiler import compile_memory
from local_dreaming.storage import ClaimVersionInput, MemoryStore


def test_compiler_is_deterministic_and_separates_ops(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    artifacts = tmp_path / "artifacts"
    store = MemoryStore(memory)
    claim_id, _, revision = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value="zh-TW",
            summary="Leo偏好繁體中文",
            valid_from="2026-01-01",
        )
    )

    first = compile_memory(memory, artifacts)
    second = compile_memory(memory, artifacts)

    assert first == second
    assert first.name == f"r{revision:020d}"
    profile = (first / "PROFILE.md").read_text()
    assert "Leo偏好繁體中文" in profile
    assert claim_id in profile
    assert "Memory revision" in (first / "TIMELINE.md").read_text()
    assert "Local-Dreaming" not in (first / "MEMORY.md").read_text()

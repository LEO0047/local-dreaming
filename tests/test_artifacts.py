from __future__ import annotations

import json
from pathlib import Path

import pytest

from local_dreaming.artifacts import ArtifactPublishError, current_bundle, publish_bundle


def test_publish_bundle_is_atomic_and_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    first = publish_bundle(root, 7, {"PROFILE.md": "# Profile\n", "episodes/a.md": "A\n"})
    second = publish_bundle(root, 7, {"episodes/a.md": "A\n", "PROFILE.md": "# Profile\n"})

    assert first == second == current_bundle(root)
    assert (root / "CURRENT").read_text().strip() == "r00000000000000000007"
    manifest = json.loads((first / "MANIFEST.json").read_text())
    assert manifest["memory_revision"] == 7
    assert sorted(manifest["files"]) == ["PROFILE.md", "episodes/a.md"]


def test_rejects_changed_bundle_for_same_revision(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    publish_bundle(root, 1, {"MEMORY.md": "one\n"})

    with pytest.raises(ArtifactPublishError):
        publish_bundle(root, 1, {"MEMORY.md": "two\n"})


def test_rejects_path_traversal(tmp_path: Path) -> None:
    with pytest.raises(ArtifactPublishError):
        publish_bundle(tmp_path, 1, {"../secret": "x"})

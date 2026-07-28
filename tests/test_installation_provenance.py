from __future__ import annotations

from pathlib import Path

import pytest

from local_dreaming.installation_provenance import (
    build_installation_provenance,
    installation_provenance_mismatches,
    verify_installation_provenance,
)


def _runtime(root: Path, *, orchestration: str = "new queue logic") -> Path:
    files = {
        "orchestration.py": orchestration,
        "pipeline.py": "pipeline-v2.3",
        "prompts/phase1.txt": "phase1 prompt",
    }
    for relative, content in files.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    return root


def test_same_version_with_different_runtime_content_is_rejected(tmp_path: Path) -> None:
    expected_root = _runtime(tmp_path / "expected")
    installed_root = _runtime(tmp_path / "installed", orchestration="old queue logic")
    critical = ("orchestration.py", "pipeline.py", "prompts/phase1.txt")
    expected = build_installation_provenance(
        expected_root,
        package_version="0.1.0",
        critical_files=critical,
    )
    installed = build_installation_provenance(
        installed_root,
        package_version="0.1.0",
        critical_files=critical,
    )

    assert expected.package_version == installed.package_version
    assert installation_provenance_mismatches(expected, installed) == ("content:orchestration.py",)
    with pytest.raises(ValueError, match="content:orchestration.py"):
        verify_installation_provenance(expected, installed)


def test_identical_runtime_manifest_verifies(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path / "runtime")
    critical = ("orchestration.py", "pipeline.py", "prompts/phase1.txt")
    first = build_installation_provenance(
        runtime,
        package_version="0.1.0",
        critical_files=critical,
    )
    second = build_installation_provenance(
        runtime,
        package_version="0.1.0",
        critical_files=critical,
    )

    verify_installation_provenance(first, second)
    assert first.to_json() == second.to_json()


def test_manifest_fails_closed_when_critical_file_is_missing(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path / "runtime")
    with pytest.raises(ValueError, match="critical runtime file is missing"):
        build_installation_provenance(
            runtime,
            package_version="0.1.0",
            critical_files=("orchestration.py", "queue_reconciliation.py"),
        )

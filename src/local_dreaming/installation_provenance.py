from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

CRITICAL_RUNTIME_FILES = (
    "orchestration.py",
    "persistence.py",
    "pipeline.py",
    "queue_reconciliation.py",
    "worker.py",
    "prompts/phase1.txt",
    "prompts/phase2.txt",
    "schemas/phase1.schema.json",
    "schemas/phase2.schema.json",
)


@dataclass(frozen=True, slots=True)
class RuntimeFileProvenance:
    relative_path: str
    sha256: str
    byte_count: int


@dataclass(frozen=True, slots=True)
class InstallationProvenance:
    package_version: str
    files: tuple[RuntimeFileProvenance, ...]

    def to_json(self) -> str:
        return json.dumps(
            asdict(self),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def build_installation_provenance(
    package_root: Path,
    *,
    package_version: str,
    critical_files: tuple[str, ...] = CRITICAL_RUNTIME_FILES,
) -> InstallationProvenance:
    """Hash the runtime behavior boundary without importing the target installation."""

    root = Path(package_root).expanduser().resolve()
    if not package_version.strip():
        raise ValueError("package_version must not be empty")
    if not critical_files or len(set(critical_files)) != len(critical_files):
        raise ValueError("critical runtime file list must be non-empty and unique")

    records: list[RuntimeFileProvenance] = []
    for relative in sorted(critical_files):
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError(f"critical runtime path escapes the package root: {relative}")
        candidate = (root / relative_path).resolve()
        if not candidate.is_relative_to(root) or not candidate.is_file():
            raise ValueError(f"critical runtime file is missing: {relative}")
        records.append(
            RuntimeFileProvenance(
                relative_path=relative_path.as_posix(),
                sha256=sha256_file(candidate),
                byte_count=candidate.stat().st_size,
            )
        )
    return InstallationProvenance(
        package_version=package_version,
        files=tuple(records),
    )


def installation_provenance_mismatches(
    expected: InstallationProvenance,
    actual: InstallationProvenance,
) -> tuple[str, ...]:
    mismatches: list[str] = []
    if actual.package_version != expected.package_version:
        mismatches.append("package_version")
    expected_files = {record.relative_path: record for record in expected.files}
    actual_files = {record.relative_path: record for record in actual.files}
    for relative in sorted(expected_files.keys() | actual_files.keys()):
        expected_record = expected_files.get(relative)
        actual_record = actual_files.get(relative)
        if expected_record is None:
            mismatches.append(f"unexpected:{relative}")
        elif actual_record is None:
            mismatches.append(f"missing:{relative}")
        elif (
            actual_record.sha256 != expected_record.sha256
            or actual_record.byte_count != expected_record.byte_count
        ):
            mismatches.append(f"content:{relative}")
    return tuple(mismatches)


def verify_installation_provenance(
    expected: InstallationProvenance,
    actual: InstallationProvenance,
) -> None:
    mismatches = installation_provenance_mismatches(expected, actual)
    if mismatches:
        raise ValueError(f"installed runtime provenance mismatch: {list(mismatches)}")

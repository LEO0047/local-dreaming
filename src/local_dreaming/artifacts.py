from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from local_dreaming.errors import DreamingError


class ArtifactPublishError(DreamingError):
    """Raised when a deterministic artifact bundle cannot be published."""


def _safe_relative_path(name: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise ArtifactPublishError(f"unsafe artifact path: {name!r}")
    return path


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _bundle_manifest(revision: int, files: Mapping[str, bytes]) -> bytes:
    payload = {
        "schema_version": 1,
        "memory_revision": revision,
        "files": {
            name: {
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            for name, content in sorted(files.items())
        },
    }
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def publish_bundle(
    artifact_root: Path,
    revision: int,
    files: Mapping[str, str | bytes],
) -> Path:
    """Publish a complete immutable artifact bundle and atomically switch CURRENT."""

    os.umask(0o077)
    artifact_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    staging_root = artifact_root / ".staging"
    staging_root.mkdir(parents=True, exist_ok=True, mode=0o700)

    encoded: dict[str, bytes] = {}
    for name, content in files.items():
        safe = _safe_relative_path(name)
        normalized = safe.as_posix()
        encoded[normalized] = content.encode() if isinstance(content, str) else content
    encoded["MANIFEST.json"] = _bundle_manifest(revision, encoded)

    final_name = f"r{revision:020d}"
    final_path = artifact_root / final_name
    stage_path = staging_root / f"{final_name}-{uuid.uuid4().hex}"
    stage_path.mkdir(mode=0o700)

    try:
        for name, content in sorted(encoded.items()):
            destination = stage_path.joinpath(*PurePosixPath(name).parts)
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with destination.open("xb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            destination.chmod(0o600)
        for directory in sorted(
            (item for item in stage_path.rglob("*") if item.is_dir()),
            key=lambda item: len(item.parts),
            reverse=True,
        ):
            _fsync_directory(directory)
        _fsync_directory(stage_path)

        if final_path.exists():
            existing = (final_path / "MANIFEST.json").read_bytes()
            if existing != encoded["MANIFEST.json"]:
                raise ArtifactPublishError(
                    f"revision {revision} already exists with different artifacts"
                )
            shutil.rmtree(stage_path)
        else:
            os.replace(stage_path, final_path)
            _fsync_directory(artifact_root)

        pointer_tmp = artifact_root / f".CURRENT-{uuid.uuid4().hex}"
        with pointer_tmp.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(final_name + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        pointer_tmp.chmod(0o600)
        os.replace(pointer_tmp, artifact_root / "CURRENT")
        _fsync_directory(artifact_root)
        return final_path
    except BaseException:
        if stage_path.exists():
            shutil.rmtree(stage_path)
        raise


def current_bundle(artifact_root: Path) -> Path | None:
    pointer = artifact_root / "CURRENT"
    if not pointer.exists():
        return None
    name = pointer.read_text(encoding="utf-8").strip()
    if not name.startswith("r") or "/" in name or ".." in name:
        raise ArtifactPublishError("invalid CURRENT pointer")
    bundle = artifact_root / name
    if not bundle.is_dir() or not (bundle / "MANIFEST.json").is_file():
        raise ArtifactPublishError("CURRENT points to an incomplete artifact bundle")
    return bundle

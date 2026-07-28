from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture
def runtime_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    home = tmp_path / "runtime"
    monkeypatch.setenv("LOCAL_DREAMING_HOME", str(home))
    old_umask = os.umask(0o077)
    try:
        yield home
    finally:
        os.umask(old_umask)

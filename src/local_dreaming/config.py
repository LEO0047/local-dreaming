from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _default_home() -> Path:
    override = os.environ.get("LOCAL_DREAMING_HOME")
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / "Library" / "Application Support" / "Local-Dreaming"


@dataclass(frozen=True, slots=True)
class RuntimePaths:
    home: Path = field(default_factory=_default_home)

    @property
    def data(self) -> Path:
        return self.home / "data"

    @property
    def memory_db(self) -> Path:
        return self.data / "memory.sqlite3"

    @property
    def operations_db(self) -> Path:
        return self.data / "operations.sqlite3"

    @property
    def forgotten_log(self) -> Path:
        return self.data / "forgotten.jsonl"

    @property
    def artifacts(self) -> Path:
        return self.home / "artifacts"

    @property
    def snapshots(self) -> Path:
        return self.home / "snapshots"

    @property
    def worker(self) -> Path:
        return self.home / "worker"

    @property
    def codex_home(self) -> Path:
        return self.worker / "codex-home"

    @property
    def sterile_cwd(self) -> Path:
        return self.worker / "cwd"

    @property
    def logs(self) -> Path:
        return self.home / "logs"

    def ensure(self) -> None:
        os.umask(0o077)
        for directory in (
            self.home,
            self.data,
            self.artifacts,
            self.snapshots,
            self.worker,
            self.codex_home,
            self.sterile_cwd,
            self.logs,
        ):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory.chmod(0o700)


@dataclass(frozen=True, slots=True)
class ModelSettings:
    phase1_model: str = "gpt-5.6-sol"
    phase1_reasoning: str = "medium"
    phase2_model: str = "gpt-5.6-sol"
    phase2_reasoning: str = "high"


@dataclass(frozen=True, slots=True)
class NightlyBudget:
    max_scan_bytes: int = 250 * 1024 * 1024
    max_episodes: int = 40
    max_model_calls: int = 50
    max_input_tokens: int = 300_000
    max_output_tokens: int = 50_000
    max_wall_seconds: int = 1_200


@dataclass(frozen=True, slots=True)
class Settings:
    paths: RuntimePaths = field(default_factory=RuntimePaths)
    models: ModelSettings = field(default_factory=ModelSettings)
    nightly: NightlyBudget = field(default_factory=NightlyBudget)

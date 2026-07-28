from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field


class EvalSearch(Protocol):
    def search(self, query: str, *, limit: int, mcp: bool = False) -> dict[str, Any]: ...


class EvalCase(BaseModel):
    name: str
    category: str
    query: str
    expected_claim_ids: list[str] = Field(default_factory=list)
    forbidden_claim_ids: list[str] = Field(default_factory=list)
    limit: int = 10


class EvalResult(BaseModel):
    name: str
    category: str
    passed: bool
    returned_claim_ids: list[str]
    missing_claim_ids: list[str]
    leaked_claim_ids: list[str]


def load_cases(path: Path) -> list[EvalCase]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [EvalCase.model_validate(item) for item in raw]


def run_evals(service: EvalSearch, cases: Iterable[EvalCase]) -> list[EvalResult]:
    results: list[EvalResult] = []
    for case in cases:
        payload = service.search(case.query, limit=case.limit, mcp=False)
        returned = [
            str(item["opaque_claim_id"])
            for item in payload.get("items", [])
            if "opaque_claim_id" in item
        ]
        missing = sorted(set(case.expected_claim_ids) - set(returned))
        leaked = sorted(set(case.forbidden_claim_ids) & set(returned))
        results.append(
            EvalResult(
                name=case.name,
                category=case.category,
                passed=not missing and not leaked,
                returned_claim_ids=returned,
                missing_claim_ids=missing,
                leaked_claim_ids=leaked,
            )
        )
    return results

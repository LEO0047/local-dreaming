from __future__ import annotations

from local_dreaming.evals import EvalCase, run_evals


class FakeSearch:
    def search(self, query: str, *, limit: int, mcp: bool = False) -> dict[str, object]:
        del query, limit, mcp
        return {"items": [{"opaque_claim_id": "expected"}]}


def test_eval_reports_missing_and_leaked_claims() -> None:
    results = run_evals(
        FakeSearch(),
        [
            EvalCase(
                name="case",
                category="continuity",
                query="q",
                expected_claim_ids=["expected", "missing"],
                forbidden_claim_ids=["expected"],
            )
        ],
    )

    assert not results[0].passed
    assert results[0].missing_claim_ids == ["missing"]
    assert results[0].leaked_claim_ids == ["expected"]

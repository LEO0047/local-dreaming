from __future__ import annotations

import asyncio
from typing import Any

from local_dreaming.mcp_server import create_mcp_server, sanitize_mcp_payload


class _FlagService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    def search(self, query: str, *, limit: int, mcp: bool = True) -> dict[str, Any]:
        del query, limit
        self.calls.append(("search", mcp))
        return {"items": []}

    def context(self, query: str, *, limit: int, mcp: bool = True) -> dict[str, Any]:
        del query, limit
        self.calls.append(("context", mcp))
        return {"items": []}

    def profile(self, *, mcp: bool = True) -> dict[str, Any]:
        self.calls.append(("profile", mcp))
        return {"items": []}

    def active_context(self, *, mcp: bool = True) -> dict[str, Any]:
        self.calls.append(("active_context", mcp))
        return {"items": []}

    def timeline(self, *, limit: int, mcp: bool = True) -> dict[str, Any]:
        del limit
        self.calls.append(("timeline", mcp))
        return {"items": []}

    def explain(self, claim_id: str, *, mcp: bool = True) -> dict[str, Any]:
        del claim_id
        self.calls.append(("explain", mcp))
        return {"items": []}

    def history(self, claim_id: str, *, mcp: bool = True) -> dict[str, Any]:
        del claim_id
        self.calls.append(("history", mcp))
        return {"items": []}


def test_mcp_payload_strips_raw_fields_recursively() -> None:
    payload = {
        "memory_revision": 4,
        "items": [
            {
                "opaque_claim_id": "claim-1",
                "approved_summary": "safe",
                "value_json": {"private": True},
                "evidence": {
                    "source_class": "codex_task",
                    "bounded_excerpt": "raw",
                    "source_locator": "/private/path",
                },
            }
        ],
    }

    assert sanitize_mcp_payload(payload) == {
        "memory_revision": 4,
        "items": [
            {
                "opaque_claim_id": "claim-1",
                "approved_summary": "safe",
                "evidence": {"source_class": "codex_task"},
            }
        ],
    }


def test_mcp_payload_never_returns_secret_marker() -> None:
    assert sanitize_mcp_payload({"approved_summary": "key [REDACTED_SECRET]"}) == {
        "approved_summary": "key [REDACTED]"
    }


def test_mcp_tools_force_policy_filtered_mode() -> None:
    service = _FlagService()
    server = create_mcp_server(service)

    asyncio.run(server.call_tool("memory_search", {"query": "Leo", "limit": 5}))
    asyncio.run(server.call_tool("memory_get_profile", {}))

    assert service.calls == [("search", True), ("profile", True)]

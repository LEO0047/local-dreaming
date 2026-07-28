from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, cast

from mcp.server.fastmcp import FastMCP

BLOCKED_KEYS = {
    "bounded_excerpt",
    "content",
    "external_event_id",
    "partition_label",
    "query",
    "raw_value",
    "source_locator",
    "value_json",
}


class MCPReadService(Protocol):
    def search(self, query: str, *, limit: int, mcp: bool = True) -> dict[str, Any]: ...

    def context(self, query: str, *, limit: int, mcp: bool = True) -> dict[str, Any]: ...

    def profile(self, *, mcp: bool = True) -> dict[str, Any]: ...

    def active_context(self, *, mcp: bool = True) -> dict[str, Any]: ...

    def timeline(self, *, limit: int, mcp: bool = True) -> dict[str, Any]: ...

    def explain(self, claim_id: str, *, mcp: bool = True) -> dict[str, Any]: ...

    def history(self, claim_id: str, *, mcp: bool = True) -> dict[str, Any]: ...


def sanitize_mcp_payload(value: Any) -> Any:
    """Apply a last-mile field denylist to already policy-filtered retrieval data."""

    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            normalized = str(key)
            if normalized in BLOCKED_KEYS:
                continue
            sanitized[normalized] = sanitize_mcp_payload(item)
        return sanitized
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [sanitize_mcp_payload(item) for item in value]
    if isinstance(value, str) and "[REDACTED_SECRET]" in value:
        return value.replace("[REDACTED_SECRET]", "[REDACTED]")
    return value


def _sanitize_result(value: dict[str, Any]) -> dict[str, Any]:
    """Retain the public tool return type after the recursive safety pass."""

    return cast(dict[str, Any], sanitize_mcp_payload(value))


def create_mcp_server(service: MCPReadService) -> FastMCP:
    server = FastMCP("Local-Dreaming", json_response=True)

    @server.tool()
    def memory_search(query: str, limit: int = 10) -> dict[str, Any]:
        """Search approved memory summaries."""

        return _sanitize_result(service.search(query, limit=min(max(limit, 1), 50), mcp=True))

    @server.tool()
    def memory_context(query: str, limit: int = 12) -> dict[str, Any]:
        """Build a bounded approved-memory context packet."""

        return _sanitize_result(service.context(query, limit=min(max(limit, 1), 50), mcp=True))

    @server.tool()
    def memory_get_profile() -> dict[str, Any]:
        """Return the approved stable profile."""

        return _sanitize_result(service.profile(mcp=True))

    @server.tool()
    def memory_get_active_context() -> dict[str, Any]:
        """Return approved active projects and constraints."""

        return _sanitize_result(service.active_context(mcp=True))

    @server.tool()
    def memory_timeline(limit: int = 30) -> dict[str, Any]:
        """Return an approved bitemporal timeline."""

        return _sanitize_result(service.timeline(limit=min(max(limit, 1), 100), mcp=True))

    @server.tool()
    def memory_explain(claim_id: str) -> dict[str, Any]:
        """Return safe provenance metadata without raw evidence."""

        return _sanitize_result(service.explain(claim_id, mcp=True))

    @server.tool()
    def memory_history(claim_id: str) -> dict[str, Any]:
        """Return approved version history without raw evidence."""

        return _sanitize_result(service.history(claim_id, mcp=True))

    return server


def serve_stdio(service: MCPReadService) -> None:
    create_mcp_server(service).run(transport="stdio")

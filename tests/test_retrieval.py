from __future__ import annotations

from pathlib import Path

from local_dreaming.forgetting import ForgottenLedger
from local_dreaming.retrieval import RetrievalService
from local_dreaming.storage import (
    ClaimVersionInput,
    MemoryStore,
    fingerprint,
    identity_fingerprint,
    value_fingerprint,
)


def _seed(memory: Path) -> tuple[str, str, str]:
    store = MemoryStore(memory)
    normal_id, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="preference.language",
            scope="user_profile",
            value={"language": "zh-TW"},
            summary="Leo偏好繁體中文",
            valid_from="2026-01-01",
        )
    )
    private_hidden, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project.hidden",
            scope="project_state",
            value={"name": "private raw"},
            summary="私人專案原始摘要",
            sensitivity="private",
        )
    )
    private_safe, _, _ = store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project.safe",
            scope="project_state",
            value={"name": "safe"},
            summary="私人專案完整摘要",
            mcp_safe_summary="有一項已核准的私人專案",
            sensitivity="private",
        )
    )
    return normal_id, private_hidden, private_safe


def test_mcp_private_claim_requires_safe_summary(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    operations = tmp_path / "operations.sqlite3"
    normal_id, private_hidden, private_safe = _seed(memory)
    service = RetrievalService(memory, operations)

    local = service.active_context(mcp=False)
    mcp = service.active_context(mcp=True)

    assert {item["opaque_claim_id"] for item in local["items"]} == {
        private_hidden,
        private_safe,
    }
    assert [item["opaque_claim_id"] for item in mcp["items"]] == [private_safe]
    assert mcp["items"][0]["approved_summary"] == "有一項已核准的私人專案"
    assert service.profile(mcp=True)["items"][0]["opaque_claim_id"] == normal_id


def test_mcp_filters_private_rows_before_pagination(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    operations = tmp_path / "operations.sqlite3"
    store = MemoryStore(memory)
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project.visible",
            scope="project_state",
            value="visible",
            summary="Visible approved project",
        )
    )
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="project.hidden",
            scope="project_state",
            value="hidden",
            summary="Hidden private project",
            sensitivity="private",
        )
    )

    result = RetrievalService(memory, operations).search("project", limit=1, mcp=True)

    assert [item["predicate"] for item in result["items"]] == ["project.visible"]
    assert result["truncated"] is False


def test_search_logs_usage_without_changing_memory_revision(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    operations = tmp_path / "operations.sqlite3"
    _seed(memory)
    store = MemoryStore(memory)
    revision = store.current_revision()
    service = RetrievalService(memory, operations)

    result = service.search("繁體中文")

    assert result["items"][0]["predicate"] == "preference.language"
    assert store.current_revision() == revision
    with service.operations.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM retrieval_usage").fetchone()[0] == 1


def test_context_records_its_own_tool_name(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    operations = tmp_path / "operations.sqlite3"
    _seed(memory)
    service = RetrievalService(memory, operations)

    service.context("繁體中文")

    with service.operations.connection() as connection:
        assert (
            connection.execute("SELECT tool_name FROM retrieval_usage").fetchone()[0]
            == "memory_context"
        )


def test_pending_forget_is_suppression_active_before_database_deletion(tmp_path: Path) -> None:
    memory = tmp_path / "memory.sqlite3"
    operations = tmp_path / "operations.sqlite3"
    store = MemoryStore(memory)
    store.add_claim_version(
        ClaimVersionInput(
            subject_text="Leo",
            predicate="temporary.fact",
            scope="user_profile",
            value="forget now",
            summary="This pending-forget fact must not be readable.",
        )
    )
    identity = identity_fingerprint("Leo", "temporary.fact", "user_profile")
    value = value_fingerprint("forget now")
    ForgottenLedger(tmp_path / "forgotten.jsonl").prepare(
        target_kind="claim_exact",
        target_fingerprint=fingerprint("pending", identity, value),
        identity_fingerprint=identity,
        value_fingerprint=value,
    )

    result = RetrievalService(memory, operations).search("pending-forget")

    assert result["items"] == []

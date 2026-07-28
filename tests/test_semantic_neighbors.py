from __future__ import annotations

from local_dreaming.semantic_neighbors import (
    MAX_NEIGHBOR_SUMMARY_CHARS,
    MAX_SEMANTIC_NEIGHBORS,
    select_semantic_neighbors,
)


def _row(
    claim_id: str,
    *,
    summary: str,
    subject: str = "Local-Dreaming",
    predicate: str = "describes",
    scope: str = "project_state",
    sensitivity: str = "normal",
    mcp_safe_summary: str | None = None,
) -> dict[str, object]:
    return {
        "claim_id": claim_id,
        "claim_version_id": f"version-{claim_id}",
        "subject_text": subject,
        "predicate": predicate,
        "scope": scope,
        "summary": summary,
        "mcp_safe_summary": mcp_safe_summary,
        "sensitivity": sensitivity,
        "status": "approved",
        "valid_from": "2026-07-20",
        "valid_to": None,
    }


def test_chinese_ngrams_find_cross_scope_semantic_neighbor() -> None:
    context = select_semantic_neighbors(
        candidate_subject="user",
        candidate_predicate="wants",
        candidate_scope="project_state",
        candidate_value="希望本機做夢功能理解長期脈絡與時間軸",
        canonical_rows=(
            _row(
                "matching",
                subject="Leo",
                predicate="希望本機具備",
                scope="user_profile",
                summary="Leo 希望本機具備能延續長期脈絡與時間軸的 Dreaming 類功能。",
            ),
            _row("same-scope", summary="專案使用 SQLite 儲存資料。"),
        ),
    )

    assert [item.claim_id for item in context.neighbors] == ["matching", "same-scope"]
    assert context.neighbors[0].score > context.neighbors[1].score


def test_bounded_domain_aliases_recall_dreaming_paraphrase_without_global_fallback() -> None:
    context = select_semantic_neighbors(
        candidate_subject="user",
        candidate_predicate="wants",
        candidate_scope="project_state",
        candidate_value="像 ChatGPT App 一樣逐漸理解使用者的做夢功能",
        canonical_rows=(
            _row(
                "dreaming-goal",
                subject="Leo",
                predicate="希望本機具備",
                scope="user_profile",
                summary="Leo 希望本機具備可長期延續個人脈絡與未完成工作的 Dreaming 類功能。",
            ),
            _row(
                "unrelated",
                subject="Unrelated",
                predicate="project.status",
                summary="An unrelated project is active.",
            ),
        ),
    )

    assert [item.claim_id for item in context.neighbors] == ["dreaming-goal"]


def test_result_is_top_six_with_stable_tie_break_and_hash() -> None:
    rows = tuple(
        _row(
            f"claim-{index}",
            summary="共同關鍵詞 本機記憶功能",
            scope="other",
        )
        for index in range(8)
    )
    forward = select_semantic_neighbors(
        candidate_subject="Leo",
        candidate_predicate="希望",
        candidate_scope="user_profile",
        candidate_value={"feature": "本機記憶功能"},
        canonical_rows=rows,
    )
    reverse = select_semantic_neighbors(
        candidate_subject="Leo",
        candidate_predicate="希望",
        candidate_scope="user_profile",
        candidate_value={"feature": "本機記憶功能"},
        canonical_rows=tuple(reversed(rows)),
    )

    expected = [f"claim-{index}" for index in range(MAX_SEMANTIC_NEIGHBORS)]
    assert [item.claim_id for item in forward.neighbors] == expected
    assert forward.neighbors == reverse.neighbors
    assert forward.context_hash == reverse.context_hash
    assert forward.eligible_count == 8
    assert forward.omitted_count == 2


def test_exact_slot_is_excluded_from_cross_slot_context() -> None:
    context = select_semantic_neighbors(
        candidate_subject="Leo",
        candidate_predicate="preference.language",
        candidate_scope="user_profile",
        candidate_value="繁體中文",
        canonical_rows=(
            _row(
                "same-slot",
                subject=" leo ",
                predicate="PREFERENCE.LANGUAGE",
                scope="user_profile",
                summary="Leo 偏好繁體中文。",
            ),
        ),
    )

    assert context.neighbors == ()


def test_secret_is_excluded_and_private_uses_only_safe_summary() -> None:
    rows = (
        _row(
            "secret",
            summary="private-raw-marker 本機記憶功能",
            sensitivity="secret",
        ),
        _row(
            "private-without-safe-summary",
            summary="private-raw-marker 本機記憶功能",
            sensitivity="private",
        ),
        _row(
            "private-safe",
            summary="private-raw-marker",
            sensitivity="private",
            mcp_safe_summary="已批准的本機記憶功能摘要",
        ),
    )

    raw_query = select_semantic_neighbors(
        candidate_subject="user",
        candidate_predicate="asks",
        candidate_scope="other",
        candidate_value="private-raw-marker",
        canonical_rows=rows,
    )
    safe_query = select_semantic_neighbors(
        candidate_subject="user",
        candidate_predicate="asks",
        candidate_scope="other",
        candidate_value="已批准的本機記憶功能摘要",
        canonical_rows=rows,
    )

    assert raw_query.neighbors == ()
    assert [item.claim_id for item in safe_query.neighbors] == ["private-safe"]
    assert safe_query.neighbors[0].summary == "已批准的本機記憶功能摘要"
    assert safe_query.neighbors[0].content_policy == "mcp_safe_summary"
    assert "private-raw-marker" not in str(safe_query.as_payload())


def test_summary_is_bounded_with_explicit_marker_and_full_content_binding() -> None:
    prefix = "本機記憶功能"
    first_summary = prefix + "甲" * (MAX_NEIGHBOR_SUMMARY_CHARS + 20)
    second_summary = prefix + "甲" * MAX_NEIGHBOR_SUMMARY_CHARS + "乙" * 20
    first = select_semantic_neighbors(
        candidate_subject="user",
        candidate_predicate="wants",
        candidate_scope="other",
        candidate_value=prefix,
        canonical_rows=(_row("long", summary=first_summary, scope="user_profile"),),
    )
    second = select_semantic_neighbors(
        candidate_subject="user",
        candidate_predicate="wants",
        candidate_scope="other",
        candidate_value=prefix,
        canonical_rows=(_row("long", summary=second_summary, scope="user_profile"),),
    )

    neighbor = first.neighbors[0]
    assert len(neighbor.summary) == MAX_NEIGHBOR_SUMMARY_CHARS
    assert neighbor.summary.endswith("…")
    assert neighbor.summary_truncated
    assert first.context_hash != second.context_hash


def test_invalid_limit_fails_before_selection() -> None:
    try:
        select_semantic_neighbors(
            candidate_subject="Leo",
            candidate_predicate="asks",
            candidate_scope="other",
            candidate_value="value",
            canonical_rows=(),
            limit=MAX_SEMANTIC_NEIGHBORS + 1,
        )
    except ValueError as exc:
        assert "limit" in str(exc)
    else:
        raise AssertionError("an unbounded semantic-neighbor limit must fail")

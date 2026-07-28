# ADR-0002: Episode identity and streaming backfill

- Status: Accepted for implementation; production migration and backfill remain gated
- Date: 2026-07-20

## Context

舊 episode uniqueness 使用 `(source_id, content_fingerprint, segmenter_version)`。真實 Codex
session 可能在不同 partition、時間或 event sequence 出現相同文字，因此內容相同不代表
同一個 logical episode。大型 JSONL 也不能靠提高單檔上限解決，否則會把不必要的 tool、
reasoning 或 image payload 帶進記憶流程。

## Decision

新增順序敏感的：

```text
event_sequence_fingerprint = SHA-256(canonical ordered event_ids)
```

Episode logical uniqueness 與 deterministic ID 共同使用：

```text
source_id + partition_id + event_sequence_fingerprint + segmenter_version
```

`content_fingerprint` 保留為非識別性的比較與檢索訊號。Codex backfill 改採逐行 streaming、
先過濾與 redact，再依 raw bytes、records、characters、episodes、model usage 和 wall time
逐層限額。Cursor 只在 persistence 成功後提交。

## Migration gate

Schema v1→v2 migration 必須：

1. 保留既有 episode IDs 與所有 foreign-key references。
2. 從 ordered `episode_events` 回填 `event_sequence_fingerprint`。
3. 驗證既有 ID 與 frozen deterministic algorithm 一致。
4. 若不一致則 fail closed，要求明確 mapping，不能自動重編 ID。
5. 證明 canonical claims、claim versions 與 `memory_revision` byte／identity 不變。

## Backfill rollout gate

Production backfill 維持關閉，直到 production wheel、clone migration、restore drill、完整測試
與匿名 regression fixtures 都通過。之後先重跑先前失敗的 21-event session，再選 1–3 個
小型非 LINE sessions；每批最多約 3–5 episodes，經人工 proposal precision review 後才
逐步擴大。真人 LINE、LaunchAgent 與既有排程不在本 ADR 的授權範圍。

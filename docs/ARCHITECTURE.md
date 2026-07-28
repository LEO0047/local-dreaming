# Architecture

```text
immutable events
→ stable episodes
→ Phase 1 candidates
→ Phase 2 review proposals
→ human approval
→ canonical claim versions
→ deterministic artifacts and FTS5
→ local CLI / read-only MCP
```

`memory.sqlite3` 保存 canonical data；`operations.sqlite3` 保存 jobs、usage 與本機狀態。Operations 更新不得增加 `memory_revision`。

模型執行使用獨立 `CODEX_HOME`、sterile cwd、ephemeral JSONL 與 output schema。任何不允許的 tool event 都使整次結果失效。

## Episode logical identity

Episode 的邏輯 identity 固定為：

```text
event_sequence_fingerprint = SHA-256(canonical ordered event_ids)

source_id
+ partition_id
+ event_sequence_fingerprint
+ segmenter_version
```

`episode_id` 必須由完全相同的 inputs deterministic 生成；SQLite uniqueness、lookup 與
idempotent replay validation 也使用同一規則。`content_fingerprint` 只供內容比較、候選
去重、重複內容診斷與 retrieval／consolidation 輔助，不能作為 logical uniqueness。

舊 schema migration 會從 `episode_events` 的既有順序回填
`event_sequence_fingerprint`，並驗證既有 `episode_id`。若 deterministic ID 不相符，
migration fail closed，必須先提供明確 mapping；不得改寫既有 ID、斷開 foreign key，或
靜默建立重複 episode。

## 正式記憶界線

- `claims` 只保存 subject／predicate／scope 的邏輯 identity。
- `claim_versions` append-only，分開保存 valid time 與 recorded time。
- Leo 的 correction／approval 優先；互相衝突的 heads 可保留為 `disputed`。
- Review proposal 綁定目標 slot、head set、payload 與 evidence fingerprints；stale proposal 不會自動 rebase。
- `forgotten.jsonl` 在 restore 後先重新套用，避免 managed snapshot 復活已忘記事實。

## 衍生層

Artifacts 先編譯到 `.staging/<revision>`，完成後才原子發布並更新 `CURRENT`。
相同 canonical revision 必須產生 byte-identical Markdown。Operations snapshots 只供
`dream status` 使用，不進 FTS5、timeline 或 MCP。

# Local-Dreaming Repository Rules

- 預設使用繁體中文回報；程式碼、API、CLI、型別與錯誤保留英文。
- `memory.sqlite3` 是 canonical truth；Markdown 只可由 compiler 產生。
- 模型只能建立 candidate／proposal，不得直接寫入 canonical claim versions。
- `secret` 必須在 ingest 時 redacted，禁止進模型、MCP、logs 或 artifacts。
- MCP v2.2 僅限 read-only；correction、approval、pin、forget 只走本機 CLI。
- 不修改 `~/.codex/memories`。只有 caller 明確指定時可唯讀取用 Chronicle／Codex memory 摘要，且一律為 advisory；不得存取真人 LINE 資料或未明確 opt-in 的來源。
- 修改後執行最小相關 pytest；跨模組修改再執行完整 `uv run pytest`。

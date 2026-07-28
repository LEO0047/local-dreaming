# Practical Threat Model

## Protect against

- 模型把推論誤寫成 Leo 的事實。
- 舊 proposal 覆蓋較新的 correction。
- `secret` 或未 opt-in 的 private 來源被送到雲端。
- Restore 後把已忘記的 active memory 重新啟用。
- MCP 透過內容、錯誤或計數揭露 private raw evidence。

## Explicitly out of scope

- 已取得本機 admin／root 權限的惡意程式。
- 合規級 secure deletion。
- 清除 Time Machine、手動複製或非 Local-Dreaming 管理的備份。
- 真人 LINE 匯入與 MCP writes。

FileVault 建議啟用，但不是 runtime hard gate。

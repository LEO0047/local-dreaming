# Local-Dreaming

> **⚠️ v2.2 已退役（2026-08-03）**
>
> v2.2 已由 v3 取代並停止維護。v3 改以輕量排程任務實作：一份 SKILL.md、一支 104 行的 deterministic 閘門與一個 state.json，取代本專案的 16k 行服務與 30 張表。
>
> **v3 的架構、設計原則與復現指南見 [V3-ARCHITECTURE.md](V3-ARCHITECTURE.md)**，可執行的閘門與 SKILL 範本在 [`v3/`](v3/)。實際記憶資料與 vault 內容屬本機私人資料，不公開。以下為 v2.2 原始文件，保留作歸檔參考。

Local-Dreaming 是一套供 Codex 使用的本機長期記憶服務。它把來源事件、模型候選、人工審核、正式版本、時間軸與檢索分開，避免模型直接改寫正式記憶。

## 核心原則

- SQLite 是正式真相，Markdown 是可重建產物。
- Canonical claims 必須經人工批准。
- `secret` 不離開 deterministic local layer。
- MCP 在 v2.2 僅供唯讀安全摘要。
- Nightly 只產生候選與 review batch，不自動批准。

## 開發

```bash
RUNTIME_HOME="$HOME/Library/Application Support/Local-Dreaming"
uv sync --dev
uv run pytest
uv build
uv venv "$RUNTIME_HOME/venv" --python 3.14.6
uv pip install --python \
  "$RUNTIME_HOME/venv/bin/python" \
  --reinstall dist/local_dreaming-*.whl
"$RUNTIME_HOME/venv/bin/dream" --help
```

Build backend 使用 `uv_build`，避免 Python 3.14 跳過 hidden editable `.pth` 的問題；
release smoke test 仍會額外驗證 non-editable wheel 與 console script。

Runtime 預設位於：

```text
~/Library/Application Support/Local-Dreaming/
```

可在測試或隔離環境設定 `LOCAL_DREAMING_HOME` 覆蓋。

## 初次啟用

```bash
DREAM_BIN="$HOME/Library/Application Support/Local-Dreaming/venv/bin/dream"
"$DREAM_BIN" init
"$DREAM_BIN" doctor
"$DREAM_BIN" doctor --live
"$DREAM_BIN" audit
"$DREAM_BIN" status
```

真人 LINE 匯入、MCP writes 與自動批准不屬於 v2.2。

完整啟用與回復流程見 [docs/OPERATIONS.md](docs/OPERATIONS.md)。

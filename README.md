# Local-Dreaming

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

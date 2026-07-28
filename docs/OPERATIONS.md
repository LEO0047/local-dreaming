# Operations

## 1. 建置與初始化

```bash
RUNTIME_HOME="$HOME/Library/Application Support/Local-Dreaming"
uv sync --dev
uv run pytest
uv run ruff check .
uv run mypy
uv build
uv venv "$RUNTIME_HOME/venv" --python 3.14.6
uv pip install --python \
  "$RUNTIME_HOME/venv/bin/python" \
  --reinstall dist/local_dreaming-*.whl

DREAM_BIN="$RUNTIME_HOME/venv/bin/dream"
"$DREAM_BIN" --help
"$DREAM_BIN" init
```

Production 使用獨立 venv 內的 non-editable wheel；開發用 `.venv` 即使被後續 `uv run`
重新同步，也不會影響排程或正式 CLI。Runtime 預設為
`~/Library/Application Support/Local-Dreaming/`；要做隔離測試時可設定
`LOCAL_DREAMING_HOME`。以下指令沿用上方的 `DREAM_BIN`。

## 2. Worker 認證

```bash
"$DREAM_BIN" doctor
"$DREAM_BIN" doctor --live
```

`--live` 只送 synthetic nonce／Schema probe，不讀取互動式 `~/.codex` 的登入資料。
專用 `CODEX_HOME` 沒有可用認證時會 fail closed，不能用一般 Codex session 靜默替代。
Codex 升級後必須重跑。

第一次啟用時，由 Leo 在終端機完成一次專用 device login：

```bash
RUNTIME_HOME="$HOME/Library/Application Support/Local-Dreaming"
env -i \
  HOME="$RUNTIME_HOME/worker/os-home" \
  CODEX_HOME="$RUNTIME_HOME/worker/codex-home" \
  PATH="/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin" \
  /opt/homebrew/bin/codex login --device-auth
```

這一步不可用複製互動式 `~/.codex` 認證取代。登入後再執行
`"$DREAM_BIN" doctor --live`；只有兩個模型／Schema probe 都通過才會寫入七天效期的
certification stamp。每週日 `02:00` 的獨立 LaunchAgent 只用 synthetic probe 自動續證：

- 成功才原子更新 stamp。
- 失敗會寫入 `doctor-last-failure.json`，使較舊 stamp 立即失效。
- `04:00` nightly 看到無效 stamp 時 fail closed；不會自行略過 doctor。
- Codex version、模型、reasoning 或 permission vector 改變時，stamp 也會立即失效。

## 3. 安全匯入與人工審核

```bash
"$DREAM_BIN" ingest /absolute/path/to/opted-in.jsonl \
  --adapter manual --source-id leo-notes --allow-model-egress --dry-run --json
"$DREAM_BIN" segment --dry-run --json
"$DREAM_BIN" extract --dry-run --json
"$DREAM_BIN" consolidate --dry-run --json
"$DREAM_BIN" review list --json
"$DREAM_BIN" ingest /absolute/path/to/workspace --adapter operations --json
```

移除 `--dry-run` 前先確認來源、敏感度、egress policy 與 bounded diagnostics。Nightly
只會建立 candidate／review batch；canonical claim 仍須 Leo 透過 `review approve` 或
`review correct` 明確寫入。

`--dry-run` 的保證是「不寫入來源、事件、候選、proposal 或 canonical memory」。在全新
`LOCAL_DREAMING_HOME` 上，部分指令仍可能建立 owner-only 的空 DB／worker scaffolding；
它不是 filesystem-zero-write sandbox。

`operations` adapter 只把 workspace／automation／health 摘要寫入
`operations.sqlite3`；它不建立 memory event，也不會觸發模型。過期的 plan-like claim
只會由 temporal engine 建立 `outcome_unknown` review proposal，仍須 Leo 批准。

### Codex history streaming backfill

Codex JSONL 使用逐行 streaming parser；單檔可以超過 8 MiB，但每次執行仍受 raw-byte
budget 限制。Parser 會先排除 reasoning、非 final assistant 訊息、圖片／blob 與大型工具
payload，只保留 bounded user message、assistant final、`DREAMING_HANDOFF` 與允許的
文字型 tool result，並在建立 stable event 前執行 secret redaction。

Durable cursor 保存 byte offset、record number、檔案 identity 與 stable prefix hash；只有
來源與 events 都成功寫入後才前進。若程序在 persistence 前 crash，下次會安全重播同一段，
由 event／episode identity 收斂，不會略過資料。

```text
raw scan cursor
→ raw-byte budget
→ record filtering
→ record／character budget
→ stable events
→ episode budget
→ model call／token／wall-time budget
```

歷史 pilot 必須維持小批次，例如：

```bash
"$DREAM_BIN" segment --limit 5 --json
"$DREAM_BIN" extract --limit 5 --json
```

每批只選 1–3 個小型、非 LINE session，最多約 3–5 episodes，完成 Leo 人工 precision
review 後才擴大；不得直接處理整個約 8 GB inventory。

Schema v1 升級 v2 前，先在 snapshot clone 驗證 episode ID、candidate／evidence foreign
keys、canonical claim hash 與 `memory_revision` 全部不變。若任一 identity mismatch，升級
fail closed，先建立正式 migration mapping。

## 4. 備份、忘記與還原

```bash
"$DREAM_BIN" snapshot create --dry-run --json
"$DREAM_BIN" forget-claim CLAIM_ID --dry-run --json
"$DREAM_BIN" forget-source SOURCE_ID --dry-run --json
"$DREAM_BIN" snapshot restore \
  "$HOME/Library/Application Support/Local-Dreaming/snapshots/SNAPSHOT_ID" \
  --dry-run --json
"$DREAM_BIN" audit --json
```

Forget 會更新 active DB、FTS、artifacts 與 Local-Dreaming 管理的 snapshots；不保證清除
Time Machine、手動複製或其他非受管理備份。Restore 會先驗證兩個 DB，再重新套用目前
的 `forgotten.jsonl`。

## 5. Nightly rollout gate

```bash
"$DREAM_BIN" run-nightly --dry-run --json
"$DREAM_BIN" status --json
```

正式排程拆成兩個 LaunchAgent：

- 每週日 `02:00 Asia/Taipei`：synthetic `doctor --live --json`。
- 每日 `04:00 Asia/Taipei`：bounded `run-nightly`。

只有以下條件都成立才能安裝：

1. live worker／Schema probe 通過。
2. 至少七次人工觸發、bounded、成功的 run。
3. `dream status` 沒有 LaunchAgent 或 OpenClaw 時段衝突。

安裝前先做不寫檔的 preview，再分別安裝與驗證：

```bash
"$DREAM_BIN" schedule install --job doctor --hour 2 --minute 0 --dry-run --json
"$DREAM_BIN" schedule install --job nightly --hour 4 --minute 0 --dry-run --json
"$DREAM_BIN" schedule install --job doctor --hour 2 --minute 0 --json
"$DREAM_BIN" schedule install --job nightly --hour 4 --minute 0 --json
"$DREAM_BIN" status --json
```

安裝器本身會以 `launchctl print` 驗證 label 已註冊；日常請使用 `dream status`，不要把完整
`launchctl print` dump 貼到終端或報告，因為 macOS 可能同時顯示該 launchd domain 的既有
環境變數。LaunchAgent 的實際 payload 仍由 `/usr/bin/env -i` 建立最小環境。

未通過 gate 時不得把 dry-run、no-op 或失敗執行計為已驗證 run。安裝後仍只產生待審核
proposal，不會自動批准正式記憶。

這個 Mac worker 排程與 ChatGPT App 的已排程任務是兩件事：

- Mac worker（`04:00`）會實際執行 bounded ingest／extraction／consolidation。
- Weekly doctor（週日 `02:00`）只執行 synthetic certification refresh。
- ChatGPT 審核提醒（`08:00`）只讀取 `dream status` 與 `dream review list` 後提醒 Leo，
  不執行匯入、模型抽取、批准、forget 或排程變更。

因此可以先啟用唯讀的 `08:00` 提醒；`04:00` worker 仍須等上列 gate 全部通過，並在安裝
當下重新檢查 OpenClaw 與其他 LaunchAgent 的時段衝突。

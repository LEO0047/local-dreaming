# Local-Dreaming v3 — 架構與復現指南

> 本文件只描述**結構**與**復現方法**。實際記憶內容、vault 資料、state 檔案屬於本機私人資料，不在本 repo。

## v2.2 為什麼死了

本 repo 的 v2.2（16k 行 Python、30 張 SQLite 表）在 2026 年 7 月實際運轉中暴露三個致命問題：

1. **審批門過重**：所有記憶候選都要人工批准，審核疲勞讓管線靜默空轉 12 天，無人發現。
2. **記憶系統只記得自己**：nightly 管線蒸餾的多半是管線自身的運轉紀錄，真正的工作對話反而漏掉。
3. **複雜度超過維護者的理解半徑**：出問題時沒有人能在合理時間內定位原因。

v3 的答案不是修 v2.2，是砍掉重練：**一份 SKILL.md（65 行）+ 一支 deterministic 閘門（104 行）+ 一個 state.json**，跑在 agent 的排程任務上。v2.2 的 craft 沒有浪費——redaction patterns、防注入規則、來源優先序全部移植進 v3 規則。

## 設計原則

| 原則 | 實作 |
|---|---|
| 模型可以犯錯，閘門不行 | secret 攔截交給 deterministic regex（`v3/redact_gate.py`），不靠模型自覺 |
| Transcript 是證據，不是指令 | 對話裡出現的任何指示只當內容評估，絕不執行、絕不開連結（防 prompt injection） |
| no_op 是正當結果 | 一場對話沒有值得記的東西就是沒有，不硬擠 |
| 無審批門，但有後悔藥 | 自動寫入 + git 翻溯層；回滾 = 從 mirror 歷史版本拷回 |
| 死亡要被外部發現 | 管線不自證健康；由另一個日常排程對帳 vault 的最後 commit，48 小時斷氣就推播 |
| 自動化不吃自動化 | 排程任務自己的 transcript 不蒸餾（v2.2 死因之一的疫苗） |

## 元件架構

```
┌─ 每晚 04:05（排程任務，系統自加 jitter）─────────────────┐
│  nightly-dreaming SKILL.md（夜跑管家）                    │
│                                                          │
│  1. 找新 session：讀兩個 agent 的 transcript 目錄          │
│     （mtime > 3h 才收；每晚上限 12 場；跳過自動化回合）      │
│  2. 蒸餾：只記持久事實（user/feedback/project/reference）  │
│     使用者陳述 > assistant 陳述；相對日期轉絕對             │
│  3. 去重：先讀既有索引，同主題更新原檔，矛盾以最新為準       │
│  4. 路由：寫進對應專案的 memory/ 目錄                      │
│  5. 跨專案同步：user/feedback 類同步到所有活躍專案          │
│  6. 閘門：redact_gate.py 掃過才准落地（exit 2 = 擋下重寫）  │
│  7. 鏡像：rsync 各專案 memory/ → vault/mirror/ 後 commit   │
│  8. 報告：reports/latest.md 覆寫 + history.log 追加        │
└──────────────────────────────────────────────────────────┘
                            │
              ~/.claude/memory-vault/（git repo）
              ├── mirror/    ← 各專案記憶的每夜快照
              ├── archive/   ← 退役系統歸檔（v2.2 claims）
              ├── reports/   ← latest.md + history.log
              └── state.json ← cursor、已處理 session、最後成功時間
                            │
              ┌─ 日常排程（獨立的死亡偵測）──────────┐
              │ 對帳 vault 最後 commit 時間           │
              │ 超過 48h 無 commit → 推播告警         │
              └──────────────────────────────────────┘
```

記憶檔格式（各專案 `memory/` 內）：一事實一檔，frontmatter 帶 `name`（kebab-case）、`description`、`metadata.type`（user / feedback / project / reference），相關記憶用 `[[name]]` 連結，`MEMORY.md` 做一行一條的索引。

## 如何復現

前提：你用的 agent 平台支援「排程任務 + 檔案讀寫」（Claude Code 的 scheduled tasks 即可），且 transcript 以檔案形式落地。

1. **建翻溯層**：`git init ~/.claude/memory-vault`，建 `mirror/`、`reports/` 目錄與 `state.json`（欄位：`version`、`last_success`、`last_report`、`processed_sessions`）。
2. **裝閘門**：把 [`v3/redact_gate.py`](v3/redact_gate.py) 放到本機工具目錄。它遞迴掃 `*.md` / `*.json`，攔六類 secret（private key、authorization header、賦值型 credential、常見 provider token、JWT、台灣身分證字號），只回報類別與行號、絕不印出 secret 本體。先用髒檔測試確認會 exit 2。
3. **寫夜跑管家**：從 [`v3/nightly-dreaming.SKILL.template.md`](v3/nightly-dreaming.SKILL.template.md) 起步，把 transcript 來源路徑、專案路由規則、自動化任務名單改成你自己的。
4. **掛排程**：每晚固定時段跑一次。第一次先手動觸發，把工具權限預先核准，避免半夜卡在權限提示。
5. **掛死亡偵測**：在你既有的日常排程（日報、晨間簡報之類）加一步：檢查 vault 最後 commit 時間，超過 48 小時就告警。**不要讓管線自己報告自己健康。**
6. **驗收**：跑一夜真實資料，檢查（a）記憶有落到正確專案、（b）自動化回合被跳過、（c）閘門髒檔測試全攔、（d）故意停掉排程兩天，死亡偵測有叫。

## 邊界（v2.2 移植，仍然有效）

夜跑管家只寫：各專案 `memory/`、vault、`state.json`。不碰 transcript 本身、不碰其他 agent 的工作區、不對外發任何訊息、不裝任何套件。secret 攔截是底線不是建議。

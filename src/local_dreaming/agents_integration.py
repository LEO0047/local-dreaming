from __future__ import annotations

import os
from pathlib import Path

START_MARKER = "<!-- LOCAL_DREAMING_HANDOFF_V1 START -->"
END_MARKER = "<!-- LOCAL_DREAMING_HANDOFF_V1 END -->"


def handoff_instruction() -> str:
    return f"""{START_MARKER}
## Local-Dreaming 精簡交接

涉及程式碼／檔案修改、跨 session 延續或尚未完成工作的 task，final 末尾輸出一個精簡 handoff。
簡單問答不輸出；不得放入 secrets、私人訊息或完整 logs。
實際 handoff 必須是 final 最後方的純文字 block，不得放在 Markdown code fence 或 quote 中。

```text
[DREAMING_HANDOFF]
workspace:
state:
completed:
verified:
leo_corrections:
pending:
[/DREAMING_HANDOFF]
```

此 block 只代表 `project_state`，不得推論或寫入 `user_profile`。
{END_MARKER}"""


def merged_agents_content(existing: str) -> str:
    block = handoff_instruction()
    if START_MARKER not in existing and END_MARKER not in existing:
        return existing.rstrip() + "\n\n" + block + "\n"
    if START_MARKER not in existing or END_MARKER not in existing:
        raise ValueError("incomplete Local-Dreaming marker pair in AGENTS.md")
    start = existing.index(START_MARKER)
    end = existing.index(END_MARKER, start) + len(END_MARKER)
    return existing[:start] + block + existing[end:]


def install_global_handoff(path: Path, *, dry_run: bool = True) -> bool:
    """Install the bounded global instruction atomically; return whether it changed."""

    existing = path.read_text(encoding="utf-8")
    updated = merged_agents_content(existing)
    if updated == existing:
        return False
    if dry_run:
        return True
    temporary = path.with_name(f".{path.name}.local-dreaming-{os.getpid()}")
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(updated)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(path.stat().st_mode & 0o777)
    os.replace(temporary, path)
    return True

#!/usr/bin/env python3
"""Local-Dreaming v3 寫入閘門:記憶檔落地前的 deterministic secret 掃描。

用法:
    python3 redact_gate.py FILE_OR_DIR [FILE_OR_DIR ...]

目錄會遞迴掃 *.md 與 *.json。輸出只報 pattern 類別、檔案與行號,
絕不印出 secret 本體。發現 secret 時 exit code 2,乾淨為 0。

Pattern 移植自退役的 Local-Dreaming v2.2 redaction.py(2026-08-03),
對應 Leo 批准的邊界:secret 必須經 deterministic 手段攔截,不靠模型自覺。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "private_key",
        re.compile(
            r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----.*?"
            r"-----END (?:[A-Z0-9 ]+ )?PRIVATE KEY-----",
            re.DOTALL,
        ),
    ),
    (
        "authorization_header",
        re.compile(r"(?im)^(authorization\s*:\s*)(?:bearer|basic)\s+[^\s]+\s*$"),
    ),
    (
        "assigned_credential",
        re.compile(
            r"\b(api[_ -]?key|access[_ -]?token|refresh[_ -]?token|auth[_ -]?token|token|"
            r"client[_ -]?secret|password|passwd|secret)\b(\s*[:=]\s*)"
            r"(?:\"[^\"\r\n]{4,}\"|'[^'\r\n]{4,}'|[^\s,;]{4,})",
            re.IGNORECASE,
        ),
    ),
    (
        "provider_token",
        re.compile(
            r"\b(?:sk-(?:proj-|live-)?[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|"
            r"xox[baprs]-[A-Za-z0-9-]{12,}|AKIA[0-9A-Z]{16})\b"
        ),
    ),
    (
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
    ),
    ("taiwan_national_id", re.compile(r"(?<![A-Z0-9])[A-Z][12]\d{8}(?!\d)")),
)


def scan_text(text: str) -> list[tuple[str, int]]:
    """回傳 (pattern 類別, 行號) 清單,不回傳命中的內容。"""
    hits: list[tuple[str, int]] = []
    for kind, pattern in PATTERNS:
        for match in pattern.finditer(text):
            line_no = text.count("\n", 0, match.start()) + 1
            hits.append((kind, line_no))
    return hits


def collect_files(args: list[str]) -> list[Path]:
    files: list[Path] = []
    for arg in args:
        path = Path(arg)
        if path.is_dir():
            files.extend(sorted(path.rglob("*.md")))
            files.extend(sorted(path.rglob("*.json")))
        elif path.is_file():
            files.append(path)
        else:
            print(f"redact-gate: 找不到 {path}", file=sys.stderr)
            sys.exit(3)
    return files


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 3
    dirty = False
    for path in collect_files(sys.argv[1:]):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as err:
            print(f"redact-gate: 讀不到 {path}: {err}", file=sys.stderr)
            return 3
        for kind, line_no in scan_text(text):
            dirty = True
            print(f"SECRET {kind} {path}:{line_no}")
    if dirty:
        print("redact-gate: 發現 secret,擋下。", file=sys.stderr)
        return 2
    print("redact-gate: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())

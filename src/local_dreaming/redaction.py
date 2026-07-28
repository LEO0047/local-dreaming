from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

REDACTED_SECRET = "[REDACTED_SECRET]"


@dataclass(frozen=True, slots=True)
class RedactionResult:
    text: str
    redaction_count: int
    kinds: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _SecretPattern:
    kind: str
    pattern: re.Pattern[str]


_PATTERNS: tuple[_SecretPattern, ...] = (
    _SecretPattern(
        "private_key",
        re.compile(
            r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----.*?"
            r"-----END (?:[A-Z0-9 ]+ )?PRIVATE KEY-----",
            re.DOTALL,
        ),
    ),
    _SecretPattern(
        "authorization_header",
        re.compile(r"(?im)^(authorization\s*:\s*)(?:bearer|basic)\s+[^\s]+\s*$"),
    ),
    _SecretPattern(
        "assigned_credential",
        re.compile(
            r"\b(api[_ -]?key|access[_ -]?token|refresh[_ -]?token|auth[_ -]?token|token|"
            r"client[_ -]?secret|password|passwd|secret)\b(\s*[:=]\s*)"
            r"(?:\"[^\"\r\n]{4,}\"|'[^'\r\n]{4,}'|[^\s,;]{4,})",
            re.IGNORECASE,
        ),
    ),
    _SecretPattern(
        "provider_token",
        re.compile(
            r"\b(?:sk-(?:proj-|live-)?[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9]{20,}|"
            r"xox[baprs]-[A-Za-z0-9-]{12,}|AKIA[0-9A-Z]{16})\b"
        ),
    ),
    _SecretPattern(
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
    ),
    _SecretPattern("taiwan_national_id", re.compile(r"(?<![A-Z0-9])[A-Z][12]\d{8}(?!\d)")),
)


def redact_secrets(text: str) -> RedactionResult:
    """Replace supported secret forms without returning captured secret values.

    The function is intentionally idempotent and records only categories/counts,
    making the result safe to pass to logs or model-boundary code.
    """

    redacted = text
    kinds: list[str] = []
    count = 0
    for secret_pattern in _PATTERNS:
        redacted, replacements = secret_pattern.pattern.subn(REDACTED_SECRET, redacted)
        if replacements:
            count += replacements
            kinds.extend([secret_pattern.kind] * replacements)
    return RedactionResult(text=redacted, redaction_count=count, kinds=tuple(kinds))


def has_unredacted_secret(text: str) -> bool:
    return redact_secrets(text).redaction_count > 0


def has_secret_material(value: object) -> bool:
    """Detect secret-bearing structured output without returning the secret value."""

    if isinstance(value, str):
        return REDACTED_SECRET in value or has_unredacted_secret(value)
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(item, str) and has_unredacted_secret(f"{key}={item}"):
                return True
            if has_secret_material(item):
                return True
        return False
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        return any(has_secret_material(item) for item in value)
    return False

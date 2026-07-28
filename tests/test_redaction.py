from __future__ import annotations

from local_dreaming.redaction import (
    REDACTED_SECRET,
    has_secret_material,
    has_unredacted_secret,
    redact_secrets,
)


def test_redacts_credentials_tokens_and_identity_without_returning_values() -> None:
    api_key = "sk-proj-abcdefghijklmnopqrstuvwxyz123456"
    password = "correct-horse-battery-staple"
    national_id = "A123456789"
    original = f'api_key="{api_key}"\npassword={password}\nowner={national_id}'

    result = redact_secrets(original)

    assert result.redaction_count == 3
    assert result.kinds == (
        "assigned_credential",
        "assigned_credential",
        "taiwan_national_id",
    )
    assert api_key not in result.text
    assert password not in result.text
    assert national_id not in result.text
    assert result.text.count(REDACTED_SECRET) == 3


def test_redaction_is_idempotent_and_does_not_treat_email_as_secret() -> None:
    first = redact_secrets("Contact leo@example.com; token=abcdefghijklmnop")
    second = redact_secrets(first.text)

    assert "leo@example.com" in first.text
    assert first.redaction_count == 1
    assert second.redaction_count == 0
    assert second.text == first.text
    assert not has_unredacted_secret(first.text)


def test_redacts_multiline_private_key() -> None:
    text = "before\n-----BEGIN PRIVATE KEY-----\nabc123\n-----END PRIVATE KEY-----\nafter"

    result = redact_secrets(text)

    assert result.text == f"before\n{REDACTED_SECRET}\nafter"
    assert result.kinds == ("private_key",)


def test_detects_secret_material_in_structured_model_output() -> None:
    assert has_secret_material({"token": "abcdefghijklmnop"})
    assert has_secret_material({"nested": [{"password": "correct-horse"}]})
    assert has_secret_material({"value": REDACTED_SECRET})
    assert not has_secret_material({"token_count": 12, "summary": "safe"})

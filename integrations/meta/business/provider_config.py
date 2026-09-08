"""VANTA's own Meta provider-level configuration.

This is deliberately separate from :class:`MetaAuthConfig`, which describes
the shared Meta *App*. This module describes VANTA's Meta *business
portfolio*: the business that owns the System User and the extended credit
line that customer WABAs are attached to.

Nothing here is a secret except ``system_user_token``, which is read from the
environment, never persisted, never returned by ``safe_summary()``, and never
logged.

Billing model
-------------
VANTA's clients pay VANTA; VANTA receives the aggregated Meta invoice and
pays Meta. That is Meta's Solution Partner credit-sharing arrangement, so
``credit_sharing_enabled`` defaults to on. A deployment that has not yet been
granted a Meta extended credit line can set
``META_CREDIT_SHARING_ENABLED=false`` to onboard clients who pay Meta
directly -- but it must be a deliberate deployment decision, not a silent
fallback, because it changes who is billed.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

#: Currencies Meta accepts for ``waba_currency`` on
#: POST /<CREDIT_LINE_ID>/whatsapp_credit_sharing_and_attach.
#: Note for South African deployments: ZAR is NOT in this list. The client
#: WABA's own currency is read from Meta and validated against this set; it
#: is never assumed or defaulted silently.
SUPPORTED_WABA_CURRENCIES = ("AUD", "EUR", "GBP", "IDR", "INR", "USD")

#: Tasks granted to VANTA's System User on a client WABA.
#: MANAGE is admin access and is what a Solution Partner needs for full
#: management including billing. MANAGE_BILLING is required for credit-line
#: sharing when only granular tasks are available.
DEFAULT_ASSIGNED_TASKS = ("MANAGE",)

_NUMERIC_ID_RE = re.compile(r"^\d{5,25}$")
_PLACEHOLDERS = {"", "change-me", "replace-me", "your-token", "todo", "none", "null"}


class MetaProviderConfigError(RuntimeError):
    """VANTA's provider-level Meta configuration is missing or invalid."""

    def __init__(self, message: str, *, missing: tuple[str, ...] = ()):
        super().__init__(message)
        self.missing = missing


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class MetaProviderConfig:
    """VANTA's Meta business portfolio + System User credentials."""

    business_id: str
    system_user_id: str
    system_user_token: str
    credit_line_id: str = ""
    assigned_tasks: tuple[str, ...] = DEFAULT_ASSIGNED_TASKS
    credit_sharing_enabled: bool = True

    # ------------------------------------------------------------------ load

    @classmethod
    def from_env(cls) -> "MetaProviderConfig":
        tasks_raw = _env("META_WABA_ASSIGNED_TASKS")
        tasks = tuple(
            part.strip().upper() for part in tasks_raw.split(",") if part.strip()
        ) or DEFAULT_ASSIGNED_TASKS
        return cls(
            business_id=_env("META_BUSINESS_ID"),
            system_user_id=_env("META_SYSTEM_USER_ID"),
            system_user_token=_env("META_SYSTEM_USER_TOKEN"),
            credit_line_id=_env("META_CREDIT_LINE_ID"),
            assigned_tasks=tasks,
            credit_sharing_enabled=_env_bool("META_CREDIT_SHARING_ENABLED", True),
        )

    # -------------------------------------------------------------- validate

    def validate(self) -> None:
        """Structural validation only.

        This proves the deployment supplied plausible values. It does NOT
        prove they are correct at Meta -- ``META_SYSTEM_USER_ID`` in
        particular is never trusted on its own and is checked against Meta by
        :meth:`TechProviderOnboardingService.verify_provider_configuration`.
        """
        missing: list[str] = []
        if self.system_user_token.lower() in _PLACEHOLDERS:
            missing.append("META_SYSTEM_USER_TOKEN")
        if self.business_id.lower() in _PLACEHOLDERS:
            missing.append("META_BUSINESS_ID")
        if missing:
            raise MetaProviderConfigError(
                "VANTA's Meta provider configuration is incomplete. Missing "
                "environment variable(s): " + ", ".join(missing) + ".",
                missing=tuple(missing),
            )

        if not _NUMERIC_ID_RE.fullmatch(self.business_id):
            raise MetaProviderConfigError(
                "META_BUSINESS_ID must be the numeric Meta business portfolio ID."
            )
        if self.system_user_id and not _NUMERIC_ID_RE.fullmatch(self.system_user_id):
            raise MetaProviderConfigError(
                "META_SYSTEM_USER_ID must be the numeric Meta System User ID."
            )
        if self.credit_line_id and not _NUMERIC_ID_RE.fullmatch(self.credit_line_id):
            raise MetaProviderConfigError(
                "META_CREDIT_LINE_ID must be the numeric Meta extended credit line ID."
            )
        if not self.assigned_tasks:
            raise MetaProviderConfigError(
                "META_WABA_ASSIGNED_TASKS must contain at least one Meta task, e.g. MANAGE."
            )

    @property
    def configured(self) -> bool:
        try:
            self.validate()
        except MetaProviderConfigError:
            return False
        return True

    # ------------------------------------------------------------- reporting

    def safe_summary(self) -> dict[str, Any]:
        """Diagnostics payload. Contains no token material by construction."""
        return {
            "business_id": self.business_id or None,
            "system_user_id": self.system_user_id or None,
            "system_user_token_present": bool(self.system_user_token),
            "credit_line_id": self.credit_line_id or None,
            "assigned_tasks": list(self.assigned_tasks),
            "credit_sharing_enabled": self.credit_sharing_enabled,
            "supported_waba_currencies": list(SUPPORTED_WABA_CURRENCIES),
        }

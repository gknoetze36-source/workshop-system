"""Google OAuth 2.0 configuration for Google Business Profile posting.

Mirrors integrations/meta/auth/config.py's from_env() pattern -- fail
loudly and specifically at construction time if required credentials are
missing, rather than let a half-configured client fail confusingly deep
inside an API call.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class GoogleAuthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str

    @classmethod
    def from_env(cls) -> "GoogleAuthConfig":
        client_id = os.getenv("GOOGLE_CLIENT_ID", "").strip()
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "").strip()
        missing = [name for name, value in [
            ("GOOGLE_CLIENT_ID", client_id),
            ("GOOGLE_CLIENT_SECRET", client_secret),
        ] if not value]
        if missing:
            raise RuntimeError(
                "Missing Google authentication configuration: " + ", ".join(missing)
            )
        return cls(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)

"""X (Twitter) App OAuth configuration.

Mirrors GoogleAuthConfig's shape exactly (integrations/google/auth/config.py):
a small frozen dataclass, loaded from environment, validated eagerly so
a missing credential fails at from_env() rather than partway through an
OAuth exchange.

X's confidential-client token endpoint requires Basic auth
(client_id:client_secret base64-encoded) rather than the client_id/
client_secret-in-body approach Meta and Google both use -- confirmed
directly against X's current OAuth 2.0 documentation
(docs.x.com/fundamentals/authentication/oauth-2-0/user-access-token)
rather than assumed, since VANTA's X App holds a real client secret and
is a confidential client, not a public one.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class XAuthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str

    #: tweet.read and offline.access are required for posting and for
    #: getting a refresh token at all; users.read is required for
    #: discover_accounts()'s GET /2/users/me call.
    REQUIRED_SCOPES = ("tweet.read", "tweet.write", "users.read", "offline.access")

    @classmethod
    def from_env(cls) -> "XAuthConfig":
        client_id = os.getenv("X_CLIENT_ID", "").strip()
        client_secret = os.getenv("X_CLIENT_SECRET", "").strip()
        redirect_uri = os.getenv("X_REDIRECT_URI", "").strip()
        missing = [name for name, value in [
            ("X_CLIENT_ID", client_id),
            ("X_CLIENT_SECRET", client_secret),
        ] if not value]
        if missing:
            raise RuntimeError(
                f"Missing required X API environment variables: {', '.join(missing)}"
            )
        return cls(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri)

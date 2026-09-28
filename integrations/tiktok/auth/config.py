"""TikTok App OAuth configuration.

Mirrors ThreadsAuthConfig/XAuthConfig's shape. Deliberately no PKCE
fields: confirmed directly against TikTok's Login Kit overview
(developers.tiktok.com/docs/en/login-kit-overview) that PKCE applies
to desktop/iOS/Android public clients only -- the web/server flow (a
confidential client holding client_secret server-side, exactly what
VANTA's Flask backend is) authenticates with client_key + client_secret
and relies on the state parameter for CSRF protection instead, the
same as Threads' OAuth.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TikTokAuthConfig:
    client_key: str
    client_secret: str
    redirect_uri: str

    #: user.info.basic is required for account discovery (GET /v2/user/info/);
    #: video.publish is required for the Content Posting API's
    #: creator_info and PHOTO Direct Post endpoints. Nothing broader is
    #: requested -- ads, comments, DMs, and analytics scopes are all
    #: explicitly out of scope for this task.
    REQUIRED_SCOPES = ("user.info.basic", "video.publish")

    @classmethod
    def from_env(cls) -> "TikTokAuthConfig":
        client_key = os.getenv("TIKTOK_CLIENT_KEY", "").strip()
        client_secret = os.getenv("TIKTOK_CLIENT_SECRET", "").strip()
        redirect_uri = os.getenv("TIKTOK_REDIRECT_URI", "").strip()
        missing = [name for name, value in [
            ("TIKTOK_CLIENT_KEY", client_key),
            ("TIKTOK_CLIENT_SECRET", client_secret),
        ] if not value]
        if missing:
            raise RuntimeError(
                f"Missing required TikTok API environment variables: {', '.join(missing)}"
            )
        return cls(client_key=client_key, client_secret=client_secret, redirect_uri=redirect_uri)

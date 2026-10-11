"""Instagram App configuration (Instagram API with Instagram Login).

A separate Meta app from Flyer Lady's Facebook app: workshops log in with
their Instagram professional account directly, no Facebook Page needed.
Its own App ID/secret come from App Dashboard > Instagram > API setup with
Instagram login > Business login settings, and are never shared with the
Facebook (FlyerLadyMetaConfig) or WhatsApp apps.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class InstagramAuthConfig:
    app_id: str
    app_secret: str
    redirect_uri: str
    graph_api_version: str = "v25.0"

    #: Business Login for Instagram scope names (the old business_* names
    #: were retired by Meta in January 2025).
    REQUIRED_SCOPES = ("instagram_business_basic", "instagram_business_content_publish")

    @classmethod
    def from_env(cls) -> "InstagramAuthConfig":
        app_id = os.getenv("META_INSTAGRAM_APP_ID", "").strip()
        app_secret = os.getenv("META_INSTAGRAM_APP_SECRET", "").strip()
        missing = [n for n, v in (("META_INSTAGRAM_APP_ID", app_id), ("META_INSTAGRAM_APP_SECRET", app_secret)) if not v]
        if missing:
            raise RuntimeError("Missing Instagram app configuration: " + ", ".join(missing))
        return cls(
            app_id=app_id, app_secret=app_secret,
            redirect_uri=os.getenv("META_INSTAGRAM_REDIRECT_URI", "").strip(),
            graph_api_version=os.getenv("META_INSTAGRAM_GRAPH_API_VERSION", "").strip() or "v25.0",
        )

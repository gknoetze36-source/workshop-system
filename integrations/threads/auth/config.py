"""Threads App OAuth configuration.

Mirrors XAuthConfig/GoogleAuthConfig's shape. Threads is its own Meta
App, with its own App ID/secret, distinct from the existing Flyer Lady
Facebook Login App (FlyerLadyMetaConfig) -- confirmed directly against
developers.facebook.com/documentation/threads/get-started/
get-access-tokens-and-permissions ("Your Threads App ID displayed in
App Dashboard > App settings > Basic > Threads App ID"), not assumed.
This is why "do not modify Facebook or Instagram OAuth" is a real
constraint that's easy to honor here: Threads' OAuth was never sharing
FlyerLadyMetaConfig or MetaSocialConnection to begin with.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ThreadsAuthConfig:
    app_id: str
    app_secret: str
    redirect_uri: str

    #: threads_basic is required for every endpoint; threads_content_publish
    #: is required specifically for the publishing endpoints this
    #: connector uses. threads_read_replies/threads_manage_replies/
    #: threads_manage_insights are NOT requested -- reply management and
    #: analytics are both explicitly out of scope for this task.
    REQUIRED_SCOPES = ("threads_basic", "threads_content_publish")

    @classmethod
    def from_env(cls) -> "ThreadsAuthConfig":
        app_id = os.getenv("THREADS_APP_ID", "").strip()
        app_secret = os.getenv("THREADS_APP_SECRET", "").strip()
        redirect_uri = os.getenv("THREADS_REDIRECT_URI", "").strip()
        missing = [name for name, value in [
            ("THREADS_APP_ID", app_id),
            ("THREADS_APP_SECRET", app_secret),
        ] if not value]
        if missing:
            raise RuntimeError(
                f"Missing required Threads API environment variables: {', '.join(missing)}"
            )
        return cls(app_id=app_id, app_secret=app_secret, redirect_uri=redirect_uri)

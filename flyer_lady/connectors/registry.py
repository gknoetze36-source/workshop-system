"""A simple registry mapping platform key -> connector instance.

Deliberately a plain dict behind two functions, not a class, a plugin
system, or anything with discovery/auto-registration -- "small" was the
explicit instruction, and three static entries don't need more than
this. X, Threads, and TikTok were all added deliberately, each with its
own real connector calling its own real API, organic-only in every
case (no X Ads, Threads Ads, TikTok Ads, Activity API, DMs, comments,
video editor, or analytics for any of them). TikTok's is a private/
beta implementation specifically: PHOTO Direct Post only, and every
post is restricted to SELF_ONLY viewership by TikTok itself until this
API client passes their audit (see flyer_lady/connectors/
tiktok_platform.py).
"""
from __future__ import annotations

from .base import FlyerLadyConnector
from .google_business import GoogleBusinessConnector
from .meta_social import FacebookConnector, InstagramConnector
from .x_platform import XConnector
from .threads_platform import ThreadsConnector
from .tiktok_platform import TikTokConnector

_REGISTRY: dict[str, FlyerLadyConnector] = {
    "facebook": FacebookConnector(),
    "instagram": InstagramConnector(),
    "google_business": GoogleBusinessConnector(),
    "x": XConnector(),
    "threads": ThreadsConnector(),
    "tiktok": TikTokConnector(),
}


def get_connector(platform: str) -> FlyerLadyConnector:
    try:
        return _REGISTRY[platform]
    except KeyError:
        raise KeyError(
            f"No Flyer Lady connector registered for {platform!r}. "
            f"Registered: {sorted(_REGISTRY)}"
        ) from None


def registered_platforms() -> list[str]:
    return sorted(_REGISTRY)

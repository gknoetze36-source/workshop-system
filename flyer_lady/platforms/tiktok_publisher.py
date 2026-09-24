"""TikTok PHOTO Direct Post publishing -- the required flow, in order:
creator_info -> validate the connection's selected privacy level
against it -> init the photo post -> store publish_id -> poll status
-> published/failed.

Unlike FacebookFeedPublisher/XPublisher/ThreadsPublisher, publish()
here takes session and post directly, not just special: "store
publish_id" is an explicit, separate step in the required flow, not an
implementation detail folded into "publish" -- persisting it the
moment it exists (before polling even starts) is what makes that step
real rather than nominal. If a worker died mid-poll, the publish_id
would already be on the post, not lost with it.

Deliberately does NOT choose a privacy level. connection.
selected_privacy_level must already be set (by the connect flow, per
the required flow's own ordering -- creator_info, show nickname, THEN
the user picks) and must still be one of creator_info's freshly
queried privacy_level_options; either gap raises immediately, before
any post is created.
"""
from __future__ import annotations

import time

from ..link_service import build_caption
from ..models import Special, SpecialPost
from integrations.tiktok.api_client import TikTokApiClient


class TikTokPrivacyLevelNotConfirmed(ValueError):
    """A plain ValueError subclass -- retry_policy.py's existing
    classifier already treats a bare ValueError with no status_code as
    permanent, exactly right here: retrying without a human actually
    selecting a privacy level would just fail identically forever."""


class TikTokPublisher:
    platform = "tiktok_post"

    #: TikTok's own PULL_FROM_URL processing is typically fast, but no
    #: documented upper bound is given the way Meta gives Instagram's.
    #: Sized the same shape as the other pollers in this codebase
    #: (Instagram, Threads) for consistency, not because TikTok
    #: documents this specific figure.
    MAX_POLLS = 5
    POLL_SECONDS = 30

    def __init__(self, client: TikTokApiClient, *, sleep=time.sleep):
        self.client = client
        self.sleep = sleep

    def publish(self, session, post: SpecialPost, special: Special, connection, access_token: str) -> str:
        # Fresh every time, not cached on the connection: privacy_level_options
        # can change (e.g. the creator's account visibility changes),
        # and "do not silently choose a privacy level" means validating
        # against what creator_info says RIGHT NOW, not what it said
        # when the connection was first set up.
        creator_info = self.client.query_creator_info(access_token)
        allowed = creator_info.get("privacy_level_options") or []

        if not connection.selected_privacy_level:
            raise TikTokPrivacyLevelNotConfirmed(
                "No TikTok privacy level has been selected for this connection yet"
            )
        if connection.selected_privacy_level not in allowed:
            raise TikTokPrivacyLevelNotConfirmed(
                f"Selected TikTok privacy level {connection.selected_privacy_level!r} is no longer "
                f"one of the allowed options {allowed!r} for this creator"
            )

        if not special.media_url:
            raise ValueError("TikTok photo publishing requires a public image")

        publish_id = self.client.init_photo_post(
            access_token,
            title=special.text.strip()[:90],
            description=build_caption(special)[:4000],
            privacy_level=connection.selected_privacy_level,
            photo_image_urls=[special.media_url],
        )

        # Store publish_id -- its own required step, done immediately,
        # before any polling begins.
        post.external_post_id = publish_id
        session.flush()

        for attempt in range(self.MAX_POLLS):
            status_payload = self.client.fetch_publish_status(access_token, publish_id=publish_id)
            status = status_payload.get("status")
            if status == "PUBLISH_COMPLETE":
                public_ids = status_payload.get("publicaly_available_post_id") or []
                return str(public_ids[0]) if public_ids else publish_id
            if status == "FAILED":
                fail_reason = status_payload.get("fail_reason", "unknown")
                raise RuntimeError(f"TikTok photo post failed: {fail_reason}")
            if attempt < self.MAX_POLLS - 1:
                self.sleep(self.POLL_SECONDS)
        raise RuntimeError("TikTok photo post did not finish processing within the expected time")

"""Threads post publishing -- text, and image when the existing Flyer
Lady media pipeline has produced one.

Structurally the same container-create -> poll -> publish shape as
InstagramPublisher (flyer_lady/platforms/instagram_publisher.py) -- not
shared code (Threads is its own API, its own client, its own OAuth app
entirely), but the same proven pattern, since "publish status handling"
is an explicit requirement here and Instagram's publisher is the
closest existing precedent in this codebase for exactly that.

Reuses build_caption() from flyer_lady/link_service.py as-is, unlike
the X connector's build_x_text(): Threads posts have a 500-character
limit, comfortably longer than build_caption()'s typical output (a
short special's text plus one tracking URL), so no platform-specific
truncation is needed here the way X's 280-character limit required.
"""
from __future__ import annotations

import time

from ..link_service import build_caption
from ..models import Special
from integrations.threads.api_client import ThreadsApiClient


class ThreadsPublisher:
    platform = "threads_post"

    #: Meta's own guidance: "wait on average 30 seconds before
    #: publishing a Threads media container". Matches
    #: InstagramPublisher's MAX_POLLS/POLL_SECONDS shape, sized the
    #: same way (5 * 60s = comfortably past that guidance with margin
    #: for a slow container).
    MAX_POLLS = 5
    POLL_SECONDS = 60

    def __init__(self, client: ThreadsApiClient, *, sleep=time.sleep):
        self.client = client
        self.sleep = sleep

    def publish(self, special: Special, threads_user_id: str, access_token: str) -> str:
        text = build_caption(special)
        media_type = "IMAGE" if special.media_url else "TEXT"
        creation_id = self.client.create_container(
            threads_user_id, access_token, media_type=media_type,
            text=text, image_url=special.media_url,
        )

        for attempt in range(self.MAX_POLLS):
            status = self.client.get_container_status(creation_id, access_token)
            if status == "FINISHED":
                return self.client.publish_container(threads_user_id, access_token, creation_id=creation_id)
            if status in {"ERROR", "EXPIRED"}:
                raise RuntimeError(f"Threads media container {status.lower()}")
            if status == "PUBLISHED":
                return creation_id
            if attempt < self.MAX_POLLS - 1:
                self.sleep(self.POLL_SECONDS)
        raise RuntimeError("Threads media container was not ready within five minutes")

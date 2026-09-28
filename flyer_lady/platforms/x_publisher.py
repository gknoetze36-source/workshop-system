"""X (Twitter) post publishing -- text, and image when the existing
Flyer Lady media pipeline has produced one.

Deliberately does NOT reuse flyer_lady/link_service.py's
build_caption() verbatim: that function has no length limit, which is
fine for Facebook/Instagram but would silently produce an
over-280-character X post that X's API would reject outright. This
module composes the same underlying pieces (special.text, the same
tracking_url()) with X's limit respected instead -- reusing the URL
generation, not duplicating it, while changing the one thing that
genuinely has to differ per platform.
"""
from __future__ import annotations

import requests

from ..link_service import tracking_url
from ..models import Special
from integrations.x.api_client import XApiClient

X_MAX_POST_LENGTH = 280


def build_x_text(special: Special) -> str:
    url = tracking_url(special.id)
    # +2 for the blank line already in Facebook's caption format,
    # reused here for visual consistency across platforms.
    suffix = f"\n\nBook here: {url}"
    available_for_text = X_MAX_POST_LENGTH - len(suffix)
    text = special.text.strip()
    if len(text) > available_for_text:
        # Ellipsis takes one more character, so the truncation point is
        # one shorter than the raw available length.
        text = text[: max(available_for_text - 1, 0)].rstrip() + "\u2026"
    return f"{text}{suffix}"


class XPublisher:
    platform = "x_post"

    def __init__(self, client: XApiClient):
        self.client = client

    def publish(self, special: Special, access_token: str) -> str:
        text = build_x_text(special)
        media_id = None
        if special.media_url:
            media_id = self._upload_image(access_token, special.media_url)
        result = self.client.create_tweet(access_token, text=text, media_id=media_id)
        external_id = (result.get("data") or {}).get("id")
        if not external_id:
            raise RuntimeError("X post creation returned no post ID")
        return str(external_id)

    def _upload_image(self, access_token: str, media_url: str) -> str:
        """Downloads the already-normalized JPEG (see
        integrations/storage/image_processing.py -- every Flyer Lady
        image is already a JPEG capped at 1600px on its longest edge by
        the time it has a stored media_url) and uploads those exact
        bytes to X. X requires the actual image bytes on its own
        upload endpoint; it cannot fetch a URL the way Facebook's,
        Instagram's, and Google Business Profile's publishers can."""
        response = requests.get(media_url, timeout=20)
        response.raise_for_status()
        return self.client.upload_media(access_token, image_bytes=response.content, content_type="image/jpeg")

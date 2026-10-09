from __future__ import annotations
from typing import Any
from integrations.meta.services.graph_api_client import GraphApiClient

class MetaSocialGraphClient:
    """Social facade over PHANTA's existing authenticated Graph API client."""
    def __init__(self, client: GraphApiClient): self.client = client
    PAGE_FIELDS = "id,name,access_token,tasks,instagram_business_account"

    def list_pages(self, user_token: str) -> dict[str, Any]:
        result = self.client.get_with_token(user_token, "/me/accounts", params={"fields": self.PAGE_FIELDS, "limit": 100})
        if result.get("data"):
            return result
        # Facebook Login for Business: Pages owned by a business portfolio can be
        # granted yet missing from /me/accounts. The Pages the person selected in
        # the popup are recorded on the token's granular scopes, so read those.
        scopes = (self.client.debug_customer_token(user_token).get("data") or {}).get("granular_scopes") or []
        page_ids = {str(t) for s in scopes if s.get("scope") in ("pages_show_list", "pages_manage_posts")
                    for t in (s.get("target_ids") or [])}
        return {"data": [self.client.get_with_token(user_token, f"/{page_id}", params={"fields": self.PAGE_FIELDS})
                         for page_id in sorted(page_ids)]}
    def publish_feed_photo(self, page_id: str, page_token: str, media_url: str, caption: str) -> dict[str, Any]:
        return self.client.post_with_token(page_token, f"/{page_id}/photos", data={"url": media_url, "caption": caption, "published": "true"})
    def upload_unpublished_photo(self, page_id: str, page_token: str, media_url: str) -> dict[str, Any]:
        return self.client.post_with_token(page_token, f"/{page_id}/photos", data={"url": media_url, "published": "false"})
    def publish_photo_story(self, page_id: str, page_token: str, photo_id: str) -> dict[str, Any]:
        return self.client.post_with_token(page_token, f"/{page_id}/photo_stories", data={"photo_id": photo_id})
    def create_instagram_container(self, ig_id: str, token: str, media_url: str, caption: str, *, stories: bool = False) -> dict[str, Any]:
        # media_type is required by Meta for Stories/Reels/video, but a
        # normal image feed post must not send media_type=IMAGE -- Meta
        # infers a standard image container when the key is simply
        # omitted, so it is only included for the Stories case here.
        data = {"image_url": media_url, "caption": caption}
        if stories:
            data["media_type"] = "STORIES"
        return self.client.post_with_token(token, f"/{ig_id}/media", data=data)
    def get_instagram_container_status(self, token: str, creation_id: str) -> dict[str, Any]:
        return self.client.get_with_token(token, f"/{creation_id}", params={"fields": "status_code"})
    def get_instagram_content_publishing_limit(self, ig_id: str, token: str) -> dict[str, Any]:
        return self.client.get_with_token(token, f"/{ig_id}/content_publishing_limit")
    def publish_instagram_container(self, ig_id: str, token: str, creation_id: str) -> dict[str, Any]:
        return self.client.post_with_token(token, f"/{ig_id}/media_publish", data={"creation_id": creation_id})

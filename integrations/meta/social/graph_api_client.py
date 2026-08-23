from __future__ import annotations
from typing import Any
from integrations.meta.services.graph_api_client import GraphApiClient

class MetaSocialGraphClient:
    """Social facade over PHANTA's existing authenticated Graph API client."""
    def __init__(self, client: GraphApiClient): self.client = client
    def list_pages(self, user_token: str) -> dict[str, Any]:
        return self.client.get_with_token(user_token, "/me/accounts", params={"fields": "id,name,access_token,tasks,instagram_business_account", "limit": 100})
    def publish_feed_photo(self, page_id: str, page_token: str, media_url: str, caption: str) -> dict[str, Any]:
        return self.client.post_with_token(page_token, f"/{page_id}/photos", data={"url": media_url, "caption": caption, "published": "true"})
    def upload_unpublished_photo(self, page_id: str, page_token: str, media_url: str) -> dict[str, Any]:
        return self.client.post_with_token(page_token, f"/{page_id}/photos", data={"url": media_url, "published": "false"})
    def publish_photo_story(self, page_id: str, page_token: str, photo_id: str) -> dict[str, Any]:
        return self.client.post_with_token(page_token, f"/{page_id}/photo_stories", data={"photo_id": photo_id})
    def create_instagram_container(self, ig_id: str, token: str, media_url: str, caption: str, *, stories: bool = False) -> dict[str, Any]:
        return self.client.post_with_token(token, f"/{ig_id}/media", data={"image_url": media_url, "caption": caption, "media_type": "STORIES" if stories else "IMAGE"})
    def publish_instagram_container(self, ig_id: str, token: str, creation_id: str) -> dict[str, Any]:
        return self.client.post_with_token(token, f"/{ig_id}/media_publish", data={"creation_id": creation_id})

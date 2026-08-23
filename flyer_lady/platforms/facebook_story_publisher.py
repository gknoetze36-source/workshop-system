from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from ..models import Special
class FacebookStoryPublisher:
    platform = "facebook_story"
    def __init__(self, graph: MetaSocialGraphClient): self.graph = graph
    def publish(self, special: Special, page_id: str, page_token: str) -> str:
        if not special.media_url: raise ValueError("Facebook Story publishing requires an image")
        uploaded = self.graph.upload_unpublished_photo(page_id, page_token, special.media_url)
        photo_id = uploaded.get("id") or uploaded.get("photo_id")
        if not photo_id: raise RuntimeError("Facebook photo upload returned no photo ID")
        result = self.graph.publish_photo_story(page_id, page_token, str(photo_id))
        external_id = result.get("post_id") or result.get("id")
        if not external_id: raise RuntimeError("Facebook Story publish returned no post ID")
        return str(external_id)

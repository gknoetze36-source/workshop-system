from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from ..link_service import build_caption
from ..models import Special
class FacebookFeedPublisher:
    platform = "facebook_feed"
    def __init__(self, graph: MetaSocialGraphClient): self.graph = graph
    def publish(self, special: Special, page_id: str, page_token: str) -> str:
        if special.media_url:
            result = self.graph.publish_feed_photo(page_id, page_token, special.media_url, build_caption(special))
        else:
            result = self.graph.client.post_with_token(page_token, f"/{page_id}/feed", data={"message": build_caption(special)})
        external_id = result.get("id") or result.get("post_id")
        if not external_id: raise RuntimeError("Facebook feed publish returned no post ID")
        return str(external_id)

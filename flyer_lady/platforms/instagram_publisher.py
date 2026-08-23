from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from ..link_service import build_caption
from ..models import Special
class InstagramPublisher:
    def __init__(self, graph: MetaSocialGraphClient): self.graph = graph
    def publish(self, special: Special, ig_id: str, token: str, *, stories: bool = False) -> str:
        if not special.media_url: raise ValueError("Instagram publishing requires an image")
        container = self.graph.create_instagram_container(ig_id, token, special.media_url, build_caption(special), stories=stories)
        creation_id = container.get("id")
        if not creation_id: raise RuntimeError("Instagram media container returned no ID")
        result = self.graph.publish_instagram_container(ig_id, token, str(creation_id))
        external_id = result.get("id")
        if not external_id: raise RuntimeError("Instagram publish returned no media ID")
        return str(external_id)

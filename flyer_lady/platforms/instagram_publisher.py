import time

from ..link_service import build_caption


class InstagramPublisher:
    """Publish only after Meta reports the media container is FINISHED."""
    MAX_POLLS = 5
    POLL_SECONDS = 60

    def __init__(self, graph, *, sleep=time.sleep):
        self.graph = graph
        self.sleep = sleep

    def publish(self, special, ig_id, token, *, stories=False):
        if not special.media_url:
            raise ValueError("Instagram publishing requires a public image")
        container = self.graph.create_instagram_container(
            ig_id, token, special.media_url, build_caption(special), stories=stories
        )
        creation_id = container.get("id")
        if not creation_id:
            raise RuntimeError("Meta did not return an Instagram media container ID")

        for attempt in range(self.MAX_POLLS):
            status = self.graph.get_instagram_container_status(token, str(creation_id)).get("status_code")
            if status == "FINISHED":
                result = self.graph.publish_instagram_container(ig_id, token, str(creation_id))
                return str(result.get("id") or creation_id)
            if status in {"ERROR", "EXPIRED"}:
                raise RuntimeError(f"Instagram media container {status.lower()}")
            if status == "PUBLISHED":
                return str(creation_id)
            if attempt < self.MAX_POLLS - 1:
                self.sleep(self.POLL_SECONDS)
        raise RuntimeError("Instagram media container was not ready within five minutes")

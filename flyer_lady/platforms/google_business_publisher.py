from integrations.google.business.api_client import GoogleBusinessApiClient
from ..link_service import tracking_url
from ..models import Special


class GoogleBusinessPublisher:
    platform = "google_business_post"

    def __init__(self, client: GoogleBusinessApiClient):
        self.client = client

    def publish(self, special: Special, account_id: str, location_id: str, access_token: str) -> str:
        # Unlike build_caption() (used by the Facebook/Instagram publishers),
        # the booking link isn't appended as plain text here -- Google
        # Local Posts have a dedicated callToAction.url field that renders
        # as an actual clickable button ("Learn More"), a better fit than
        # a pasted URL buried in the post body.
        result = self.client.create_local_post(
            access_token, account_id, location_id,
            summary=special.text.strip(),
            media_url=special.media_url,
            call_to_action_url=tracking_url(special.id),
        )
        external_id = result.get("name")
        if not external_id:
            raise RuntimeError("Google Business Profile post creation returned no post name/ID")
        return str(external_id)

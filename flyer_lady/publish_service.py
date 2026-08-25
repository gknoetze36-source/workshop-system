from datetime import datetime, timedelta, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.services.graph_api_client import GraphApiClient
from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from integrations.meta.social.repositories.connection_repo import MetaSocialConnectionRepository
from integrations.google.auth.config import GoogleAuthConfig
from integrations.google.auth.token_store import GoogleTokenStore
from integrations.google.business.api_client import GoogleBusinessApiClient
from .approval_service import SpecialApprovalService
from .models import Special, SpecialPost
from .platforms.facebook_feed_publisher import FacebookFeedPublisher
from .platforms.facebook_story_publisher import FacebookStoryPublisher
from .platforms.instagram_publisher import InstagramPublisher
from .platforms.google_business_publisher import GoogleBusinessPublisher
from .platforms.whatsapp_status_asset import prepare as prepare_whatsapp_status

class FlyerLadyPublishService:
    def __init__(self, config=None, graph=None, token_store=None, connection_repo=None):
        # Meta config construction is deferred to _meta_graph()/_meta_token_store()
        # below, not done here -- a workshop that has only connected Google
        # Business Profile (and hasn't done the Meta developer setup yet,
        # entirely plausible: Google's connection has no Meta prerequisite
        # at all) would otherwise be unable to construct this service to
        # publish to Google alone, since __init__ previously required full
        # Meta credentials unconditionally regardless of which platform
        # was actually being published to.
        self._config = config
        self._graph = graph
        self._token_store = token_store
        self.connection_repo = connection_repo or MetaSocialConnectionRepository()
        self.approvals = SpecialApprovalService()

    def _meta_graph(self):
        if self._graph is None:
            self._graph = MetaSocialGraphClient(GraphApiClient(self._config or MetaAuthConfig.from_env()))
        return self._graph

    def _meta_token_store(self):
        if self._token_store is None:
            self._token_store = MetaTokenStore()
        return self._token_store

    def publish_post(self, session: Session, location_id: int, post: SpecialPost):
        special = session.scalar(select(Special).where(Special.id == post.special_id, Special.location_id == location_id))
        if not special: raise ValueError("special not found")
        if not self.approvals.is_approved(session, location_id, special.id): raise ValueError("special has not been approved")
        post.attempts += 1; post.status = "publishing"; session.flush()
        try:
            if post.platform == "whatsapp_status_prepared":
                prepare_whatsapp_status(special); post.status = "prepared"; post.published_at = datetime.now(timezone.utc); post.error_message = None; return post
            if post.platform == "google_business_post":
                from models.integration_models import GoogleBusinessConnection
                connection = session.scalar(select(GoogleBusinessConnection).where(GoogleBusinessConnection.location_id == location_id))
                if not connection or connection.connection_status != "connected": raise ValueError("Google Business Profile is not connected")
                google_store = GoogleTokenStore()
                google_client = GoogleBusinessApiClient(GoogleAuthConfig.from_env())
                access_token = google_client.refresh_access_token(google_store.get_refresh_token(connection))
                external_id = GoogleBusinessPublisher(google_client).publish(special, connection.google_account_id, connection.google_location_id, access_token)
                post.external_post_id = external_id; post.status = "published"; post.published_at = datetime.now(timezone.utc); post.error_message = None
                return post
            connection = self.connection_repo.get_for_location(session, location_id)
            if not connection or connection.connection_status != "connected": raise ValueError("Facebook/Instagram social connection is not connected")
            token = self._meta_token_store().get_social_token(connection)
            if post.platform == "facebook_story": external_id = FacebookStoryPublisher(self._meta_graph()).publish(special, connection.page_id, token)
            elif post.platform == "facebook_feed": external_id = FacebookFeedPublisher(self._meta_graph()).publish(special, connection.page_id, token)
            elif post.platform in {"instagram_feed", "instagram_story"}:
                if not connection.instagram_business_account_id: raise ValueError("Instagram Business Account is not connected")
                external_id = InstagramPublisher(self._meta_graph()).publish(special, connection.instagram_business_account_id, token, stories=post.platform == "instagram_story")
            else: raise ValueError(f"unsupported platform: {post.platform}")
            post.external_post_id = external_id; post.status = "published"; post.published_at = datetime.now(timezone.utc); post.error_message = None
        except Exception as exc:
            post.status = "failed"; post.error_message = str(exc)[:4000]; post.next_attempt_at = datetime.now(timezone.utc) + timedelta(minutes=min(60, 2 ** min(post.attempts, 5)))
        return post

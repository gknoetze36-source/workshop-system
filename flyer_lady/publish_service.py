from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session
from integrations.meta.auth.capability_config import FlyerLadyMetaConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.services.graph_api_client import GraphApiClient
from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from integrations.meta.social.repositories.connection_repo import MetaSocialConnectionRepository
from .approval_service import SpecialApprovalService
from .models import Special, SpecialPost
from .platforms.facebook_feed_publisher import FacebookFeedPublisher
from .platforms.facebook_story_publisher import FacebookStoryPublisher
from .platforms.instagram_publisher import InstagramPublisher
from .platforms.whatsapp_status_asset import prepare as prepare_whatsapp_status
from .retry_policy import RECONNECT_REQUIRED, apply_failure
from .billing.x_spend_guard import check_and_reserve_x_post

class FlyerLadyPublishService:
    def __init__(self, config=None, graph=None, token_store=None, connection_repo=None):
        # Meta config construction is deferred to _meta_graph()/_meta_token_store()
        # below, not done here -- a workshop publishing only to a non-Meta
        # platform (X, Threads, TikTok) must still be able to construct
        # this service without full Meta credentials.
        self._config = config
        self._graph = graph
        self._token_store = token_store
        self.connection_repo = connection_repo or MetaSocialConnectionRepository()
        self.approvals = SpecialApprovalService()

    def _meta_graph(self):
        if self._graph is None:
            # S19/S20: Facebook feed and story publishing must use a Page
            # token obtained from the Flyer Lady App's own authorization
            # flow. No WhatsApp credential may be used here.
            self._graph = MetaSocialGraphClient(GraphApiClient(self._config or FlyerLadyMetaConfig.from_env()))
        return self._graph

    def _meta_token_store(self):
        if self._token_store is None:
            self._token_store = MetaTokenStore()
        return self._token_store

    def publish_post(self, session: Session, location_id: int, post: SpecialPost):
        special = session.scalar(select(Special).where(Special.id == post.special_id, Special.location_id == location_id))
        if not special: raise ValueError("special not found")
        if not self.approvals.is_approved(session, location_id, special.id): raise ValueError("special has not been approved")
        post.attempts += 1; post.status = "publishing"; post.publishing_started_at = datetime.now(timezone.utc); session.flush()
        connection = None  # set inside the try, but read again in except -- see apply_failure() below
        try:
            if post.platform == "whatsapp_status_prepared":
                prepare_whatsapp_status(special); post.status = "prepared"; post.published_at = datetime.now(timezone.utc); post.error_message = None; return post
            if post.platform == "x_post":
                from models.integration_models import XConnection
                from integrations.x.auth.config import XAuthConfig
                from integrations.x.auth.token_store import XTokenStore
                from integrations.x.api_client import XApiClient
                from .platforms.x_publisher import XPublisher
                connection = session.scalar(select(XConnection).where(XConnection.location_id == location_id))
                if not connection or connection.connection_status != "connected": raise ValueError("X is not connected")
                # Billing check happens AFTER confirming the connection
                # exists (a "not connected" failure costs nothing and
                # should not consume a paid-quota slot for a post that
                # was never going to happen anyway) and BEFORE any
                # paid API call at all -- see
                # flyer_lady/billing/x_spend_guard.py for why this is
                # what actually prevents a retry bug from creating
                # unlimited X charges.
                check_and_reserve_x_post(session, location_id)
                x_store = XTokenStore()
                x_client = XApiClient(XAuthConfig.from_env())
                # X's refresh grant rotates the refresh token itself (the
                # old one stops working the moment a new one is issued),
                # so the response here must be saved back to
                # the connection before the new access token is used, or
                # the NEXT refresh would fail with an already-used
                # refresh token and strand the connection.
                refresh_payload = x_client.refresh_access_token(x_store.get_refresh_token(connection))
                x_store.save_connection_tokens(
                    session, connection,
                    access_token=refresh_payload["access_token"],
                    refresh_token=refresh_payload.get("refresh_token"),
                )
                external_id = XPublisher(x_client).publish(special, refresh_payload["access_token"])
                post.external_post_id = external_id; post.status = "published"; post.published_at = datetime.now(timezone.utc); post.error_message = None
                return post
            if post.platform == "threads_post":
                from models.integration_models import ThreadsConnection
                from integrations.threads.auth.config import ThreadsAuthConfig
                from integrations.threads.auth.token_store import ThreadsTokenStore
                from integrations.threads.api_client import ThreadsApiClient
                from .platforms.threads_publisher import ThreadsPublisher
                connection = session.scalar(select(ThreadsConnection).where(ThreadsConnection.location_id == location_id))
                if not connection or connection.connection_status != "connected": raise ValueError("Threads is not connected")
                # Unlike X's branch above, this does NOT
                # refresh the token before every publish. Threads' own
                # refresh endpoint requires the token be at least 24
                # hours old (developers.facebook.com/documentation/
                # threads/get-started/long-lived-tokens) -- refreshing
                # unconditionally here would routinely fail for any
                # workshop posting more than once a day. The stored
                # long-lived token (60-day validity) is used directly;
                # keeping it refreshed well before expiry is a separate,
                # scheduled concern, out of scope for this connector.
                threads_store = ThreadsTokenStore()
                threads_client = ThreadsApiClient(ThreadsAuthConfig.from_env())
                access_token = threads_store.get_long_lived_token(connection)
                external_id = ThreadsPublisher(threads_client).publish(special, connection.threads_user_id, access_token)
                post.external_post_id = external_id; post.status = "published"; post.published_at = datetime.now(timezone.utc); post.error_message = None
                return post
            if post.platform == "tiktok_post":
                from models.integration_models import TikTokConnection
                from integrations.tiktok.auth.config import TikTokAuthConfig
                from integrations.tiktok.auth.token_store import TikTokTokenStore
                from integrations.tiktok.api_client import TikTokApiClient
                from .platforms.tiktok_publisher import TikTokPublisher
                connection = session.scalar(select(TikTokConnection).where(TikTokConnection.location_id == location_id))
                if not connection or connection.connection_status != "connected": raise ValueError("TikTok is not connected")
                # Like X's branch above (and unlike Threads'): TikTok
                # access tokens expire in 24 hours, short enough that
                # refreshing before every publish is the correct,
                # documented behavior -- nothing in TikTok's refresh
                # endpoint requires the token be any particular age
                # first, unlike Threads'. Refresh token rotation is
                # handled the same way X's is: the response is saved
                # back in full before the new access token is used.
                tiktok_store = TikTokTokenStore()
                tiktok_client = TikTokApiClient(TikTokAuthConfig.from_env())
                refresh_payload = tiktok_client.refresh_token(tiktok_store.get_refresh_token(connection))
                tiktok_store.save_connection_tokens(
                    session, connection,
                    access_token=refresh_payload["access_token"],
                    refresh_token=refresh_payload["refresh_token"],
                )
                external_id = TikTokPublisher(tiktok_client).publish(
                    session, post, special, connection, refresh_payload["access_token"],
                )
                post.status = "published"; post.published_at = datetime.now(timezone.utc); post.error_message = None
                # external_post_id was already set to the publish_id by
                # TikTokPublisher.publish() itself (the required
                # "store publish_id" step, done before polling even
                # starts) -- if publishing completed with a real,
                # publicly-viewable post id, that replaces it now.
                post.external_post_id = external_id
                return post
            if post.platform in {"instagram_feed", "instagram_story"}:
                # Instagram API with Instagram Login -- its own Meta app and
                # connection, independent of the Facebook Page connection below.
                from models.integration_models import InstagramConnection
                from integrations.instagram.api_client import InstagramLoginClient
                from integrations.instagram.config import InstagramAuthConfig
                from integrations.instagram.tokens import get_token
                connection = session.scalar(select(InstagramConnection).where(InstagramConnection.location_id == location_id))
                if not connection or connection.connection_status != "connected": raise ValueError("Instagram is not connected")
                external_id = InstagramPublisher(InstagramLoginClient(InstagramAuthConfig.from_env())).publish(
                    special, connection.instagram_user_id, get_token(connection), stories=post.platform == "instagram_story")
                post.external_post_id = external_id; post.status = "published"; post.published_at = datetime.now(timezone.utc); post.error_message = None
                return post
            connection = self.connection_repo.get_for_location(session, location_id)
            if not connection or connection.connection_status != "connected": raise ValueError("Facebook social connection is not connected")
            token = self._meta_token_store().get_social_token(connection)
            if post.platform == "facebook_story": external_id = FacebookStoryPublisher(self._meta_graph()).publish(special, connection.page_id, token)
            elif post.platform == "facebook_feed": external_id = FacebookFeedPublisher(self._meta_graph()).publish(special, connection.page_id, token)
            else: raise ValueError(f"unsupported platform: {post.platform}")
            post.external_post_id = external_id; post.status = "published"; post.published_at = datetime.now(timezone.utc); post.error_message = None
        except Exception as exc:
            # apply_failure() classifies retryable vs permanent (network/
            # 429/5xx retry with backoff; invalid token, missing
            # permission, unsupported platform, bad configuration stop
            # retrying) and caps attempts at MAX_ATTEMPTS. connection is
            # None here only for whatsapp_status_prepared, which never
            # touches an external Meta/Google connection at all.
            classification = apply_failure(post, exc)
            if classification.is_auth_error and connection is not None:
                connection.connection_status = RECONNECT_REQUIRED
        return post

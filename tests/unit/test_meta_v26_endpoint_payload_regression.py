"""Regression contracts for every Meta endpoint currently used by production code.

These tests intentionally verify the application's exact HTTP path, method,
API-version base URL, token channel and payload shape. They do not pretend to
prove Meta Dashboard approval or live-token validity.
"""
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.messaging.message_client import MetaMessageClient
from integrations.meta.services.graph_api_client import GraphApiClient
from integrations.meta.social.graph_api_client import MetaSocialGraphClient


class Response:
    ok = True
    status_code = 200
    text = ""
    def __init__(self, payload=None):
        self.payload = payload or {"id": "ok"}
    def json(self):
        return self.payload


class Session:
    def __init__(self):
        self.calls = []
    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return Response()


def config():
    return MetaAuthConfig(
        app_id="1234567890123456",
        app_secret="a" * 32,
        graph_api_version="v26.0",
        system_user_token="system-token",
    )


def graph():
    session = Session()
    return GraphApiClient(config(), session=session), session


def test_all_graph_requests_use_v26_base_url_and_relative_paths():
    client, session = graph()
    client.get_with_token("token", "/me/accounts")
    method, url, _ = session.calls[-1]
    assert method == "GET"
    assert url == "https://graph.facebook.com/v26.0/me/accounts"


def test_whatsapp_text_payload_contract():
    client, session = graph()
    MetaMessageClient(client).send_text(
        access_token="wa-token", phone_number_id="123", to="27123456789", body="Hello"
    )
    method, url, kwargs = session.calls[-1]
    assert method == "POST"
    assert url == "https://graph.facebook.com/v26.0/123/messages"
    assert kwargs["headers"]["Authorization"] == "Bearer wa-token"
    assert kwargs["json"] == {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": "27123456789",
        "type": "text",
        "text": {"preview_url": False, "body": "Hello"},
    }


def test_whatsapp_template_payload_contract():
    client, session = graph()
    MetaMessageClient(client).send_template(
        access_token="wa-token", phone_number_id="123", to="27123456789",
        name="booking_confirmation", language_code="en_US",
        components=[{"type": "body", "parameters": [{"type": "text", "text": "George"}]}],
    )
    method, url, kwargs = session.calls[-1]
    assert method == "POST"
    assert url.endswith("/123/messages")
    assert kwargs["json"]["messaging_product"] == "whatsapp"
    assert kwargs["json"]["type"] == "template"
    assert kwargs["json"]["template"]["name"] == "booking_confirmation"
    assert kwargs["json"]["template"]["language"] == {"code": "en_US"}


def test_social_page_discovery_contract():
    client, session = graph()
    MetaSocialGraphClient(client).list_pages("user-token")
    method, url, kwargs = session.calls[-1]
    assert method == "GET"
    assert url.endswith("/v26.0/me/accounts")
    assert kwargs["params"]["fields"] == "id,name,access_token,tasks,instagram_business_account"


def test_facebook_feed_and_photo_contracts():
    client, session = graph()
    social = MetaSocialGraphClient(client)
    social.publish_feed_photo("page-1", "page-token", "https://cdn.example/a.jpg", "Caption")
    method, url, kwargs = session.calls[-1]
    assert method == "POST"
    assert url.endswith("/v26.0/page-1/photos")
    assert kwargs["data"] == {"url": "https://cdn.example/a.jpg", "caption": "Caption", "published": "true"}

    social.upload_unpublished_photo("page-1", "page-token", "https://cdn.example/a.jpg")
    method, url, kwargs = session.calls[-1]
    assert method == "POST"
    assert url.endswith("/v26.0/page-1/photos")
    assert kwargs["data"] == {"url": "https://cdn.example/a.jpg", "published": "false"}


def test_facebook_story_contract():
    client, session = graph()
    MetaSocialGraphClient(client).publish_photo_story("page-1", "page-token", "photo-1")
    method, url, kwargs = session.calls[-1]
    assert method == "POST"
    assert url.endswith("/v26.0/page-1/photo_stories")
    assert kwargs["data"] == {"photo_id": "photo-1"}


def test_instagram_container_status_and_publish_contracts():
    client, session = graph()
    social = MetaSocialGraphClient(client)
    social.create_instagram_container("ig-1", "ig-token", "https://cdn.example/a.jpg", "Caption")
    method, url, kwargs = session.calls[-1]
    assert method == "POST"
    assert url.endswith("/v26.0/ig-1/media")
    # A normal image feed post must not send media_type=IMAGE -- Meta
    # infers a standard image container when the key is omitted.
    assert kwargs["data"] == {"image_url": "https://cdn.example/a.jpg", "caption": "Caption"}

    social.create_instagram_container("ig-1", "ig-token", "https://cdn.example/a.jpg", "", stories=True)
    method, url, kwargs = session.calls[-1]
    assert kwargs["data"]["media_type"] == "STORIES"

    social.get_instagram_container_status("ig-token", "creation-1")
    method, url, kwargs = session.calls[-1]
    assert method == "GET"
    assert url.endswith("/v26.0/creation-1")
    assert kwargs["params"] == {"fields": "status_code"}

    social.publish_instagram_container("ig-1", "ig-token", "creation-1")
    method, url, kwargs = session.calls[-1]
    assert method == "POST"
    assert url.endswith("/v26.0/ig-1/media_publish")
    assert kwargs["data"] == {"creation_id": "creation-1"}


def test_instagram_image_container_omits_media_type():
    """Focused regression guard for the exact bug: a normal Instagram
    image feed post must NOT send media_type=IMAGE -- Meta does not
    require it for a standard image container, only for Stories/Reels/
    video. Checked as its own test, separate from the broader contract
    test above, specifically so this one property can never silently
    regress again."""
    client, session = graph()
    social = MetaSocialGraphClient(client)

    social.create_instagram_container("ig-1", "ig-token", "https://cdn.example/photo.jpg", "A caption")
    _, _, kwargs = session.calls[-1]
    assert "media_type" not in kwargs["data"]
    assert kwargs["data"] == {"image_url": "https://cdn.example/photo.jpg", "caption": "A caption"}

    # Stories must be unaffected -- media_type is genuinely required there.
    social.create_instagram_container("ig-1", "ig-token", "https://cdn.example/photo.jpg", "A caption", stories=True)
    _, _, kwargs = session.calls[-1]
    assert kwargs["data"]["media_type"] == "STORIES"


def test_token_exchange_and_debug_token_contracts():
    client, session = graph()
    # The special methods use Session.get, so provide a compatible fake dynamically.
    def get(url, **kwargs):
        session.calls.append(("GET", url, kwargs))
        return Response({"access_token": "token"})
    session.get = get

    client.exchange_embedded_signup_code("code")
    _, url, kwargs = session.calls[-1]
    assert url.endswith("/v26.0/oauth/access_token")
    assert kwargs["params"]["client_id"] == config().app_id
    assert "client_secret" in kwargs["params"]

    client.exchange_for_long_lived_user_token("short-token")
    _, url, kwargs = session.calls[-1]
    assert url.endswith("/v26.0/oauth/access_token")
    assert kwargs["params"]["grant_type"] == "fb_exchange_token"

    client.debug_customer_token("customer-token")
    _, url, kwargs = session.calls[-1]
    assert url.endswith("/v26.0/debug_token")
    assert kwargs["params"]["input_token"] == "customer-token"
    assert kwargs["params"]["access_token"] == f"{config().app_id}|{config().app_secret}"



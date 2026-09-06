from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.services.graph_api_client import GraphApiClient


class Response:
    ok = True
    status_code = 200
    text = ""
    def json(self):
        return {"success": True}


class Session:
    def __init__(self):
        self.last = None
    def request(self, method, url, **kwargs):
        self.last = (method, url, kwargs)
        return Response()


def cfg():
    return MetaAuthConfig(
        app_id="1234567890123456",
        app_secret="a" * 32,
        graph_api_version="v26.0",
        system_user_token="system-token",
        app_domains=("https://phanta.example",),
        embedded_signup_config_id="123456",
    )


def test_customer_token_is_used_for_customer_scoped_request():
    session = Session()
    client = GraphApiClient(cfg(), session=session)
    client.post_with_token("customer-token", "/phone/register", data={"pin": "123456"})
    assert session.last[2]["headers"]["Authorization"] == "Bearer customer-token"

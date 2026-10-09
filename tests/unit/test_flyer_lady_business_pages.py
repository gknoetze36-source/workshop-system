"""Business-portfolio Pages missing from /me/accounts are found via the token's granular scopes."""
from integrations.meta.social.graph_api_client import MetaSocialGraphClient


class FakeClient:
    def __init__(self, accounts):
        self.accounts = accounts

    def get_with_token(self, token, path, params=None):
        if path == "/me/accounts":
            return {"data": self.accounts}
        return {"id": path.strip("/"), "name": f"Page {path.strip('/')}", "access_token": "page-token"}

    def debug_customer_token(self, token):
        return {"data": {"granular_scopes": [
            {"scope": "pages_show_list", "target_ids": ["111", "222"]},
            {"scope": "pages_manage_posts", "target_ids": ["111"]},
            {"scope": "public_profile"},
        ]}}


def test_uses_me_accounts_when_it_has_pages():
    pages = MetaSocialGraphClient(FakeClient([{"id": "9", "name": "Direct"}])).list_pages("t")["data"]
    assert [p["id"] for p in pages] == ["9"]


def test_falls_back_to_granted_business_pages():
    pages = MetaSocialGraphClient(FakeClient([])).list_pages("t")["data"]
    assert [p["id"] for p in pages] == ["111", "222"]
    assert all(p["access_token"] for p in pages)

"""Paid boosts of published Flyer Lady Facebook posts."""
import json
import re
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from flyer_lady.boost_service import BoostError, BoostService, validate
from integrations.meta.services.graph_api_client import MetaGraphAPIError
from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)

KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


class FakeGraph:
    def __init__(self, fail_on=None):
        self.calls, self.fail_on = [], fail_on

    def get_with_token(self, token, path, params=None):
        self.calls.append(("GET", path, params))
        if path == "/search":
            return {"data": [{"key": "2420605", "name": "Pretoria"}]}
        return {"page_story_id": "111_222"}

    def post_with_token(self, token, path, data=None, json_data=None):
        self.calls.append(("POST", path, data))
        if self.fail_on and path.endswith(self.fail_on):
            raise MetaGraphAPIError("bad", status_code=400, error={"error_user_msg": "No payment method on this ad account."})
        return {"id": path.rsplit("/", 1)[-1] + "-id"}

    def delete_with_token(self, token, path, params=None):
        self.calls.append(("DELETE", path, None))
        return {}


def _boost(graph):
    return BoostService(graph).boost(token="t", ad_account_id="999", page_token="p", external_post_id="555",
                                     name="R999 service", budget=200, days=5, area="Pretoria", radius_km=25,
                                     now=datetime(2026, 10, 11, tzinfo=timezone.utc))


def test_boost_builds_campaign_adset_creative_ad_then_activates():
    graph = FakeGraph()
    result = _boost(graph)
    posts = [(c[1], c[2]) for c in graph.calls if c[0] == "POST"]
    assert [p for p, _ in posts] == ["/act_999/campaigns", "/act_999/adsets", "/act_999/adcreatives", "/act_999/ads", "/campaigns-id"]
    campaign, adset, creative = posts[0][1], posts[1][1], posts[2][1]
    assert campaign["status"] == "PAUSED" and campaign["objective"] == "OUTCOME_AWARENESS"
    assert adset["lifetime_budget"] == "20000"  # R200 in cents
    city = json.loads(adset["targeting"])["geo_locations"]["cities"][0]
    assert city == {"key": "2420605", "radius": 25, "distance_unit": "kilometer"}
    assert creative["object_story_id"] == "111_222"  # photo ID resolved to the Page post
    assert posts[-1][1] == {"status": "ACTIVE"}
    assert result["area"] == "Pretoria" and result["ad_id"] == "ads-id"


def test_meta_refusal_is_shown_and_half_built_campaign_removed():
    graph = FakeGraph(fail_on="adsets")
    with pytest.raises(BoostError, match="No payment method"):
        _boost(graph)
    assert ("DELETE", "/campaigns-id", None) in graph.calls


@pytest.mark.parametrize("args,msg", [((10, 5, "Pretoria", 25), "Budget"), ((200, 0, "Pretoria", 25), "Days"), ((200, 5, " ", 25), "town")])
def test_validate_rejects_bad_input(args, msg):
    with pytest.raises(BoostError, match=msg):
        validate(*args)


def test_radius_is_clamped_to_meta_city_limits():
    assert validate(200, 5, "Pretoria", 5)[3] == 17 and validate(200, 5, "Pretoria", 500)[3] == 80


def _token(ctx):
    page = ctx["client"].get("/customers/manual-booking").get_data(as_text=True)
    return re.search(r'name="csrf-token" content="([^"]+)"', page).group(1)


def test_boost_route_end_to_end(client_with_roles, monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", KEY)
    from database import SessionLocal
    from flyer_lady.models import Special, SpecialPost
    from integrations.meta.auth.token_store import MetaTokenStore
    from integrations.meta.social.repositories.connection_repo import MetaSocialConnectionRepository
    from flyer_lady.models import FlyerBoost
    from models.integration_models import MetaAdAccountConnection
    ctx = client_with_roles
    loc = ctx["location_id"]
    ctx["login_as"](ctx["owner_email"])
    db = SessionLocal()
    special = Special(location_id=loc, created_by="t", text="R999 major service", booking_link="https://x/book", status="queued")
    db.add(special); db.flush()
    post = SpecialPost(special_id=special.id, location_id=loc, platform="facebook_feed", status="pending")
    db.add(post); db.commit()
    sid = special.id
    db.close()

    def boost_now():
        return ctx["client"].post(f"/dashboard/flyer-lady/specials/{sid}/boost", json={"budget": 200, "days": 5, "area": "Pretoria", "radius_km": 25}, headers={"X-CSRFToken": _token(ctx)})

    resp = boost_now()
    assert resp.status_code == 400 and "Publish this special" in resp.get_json()["error"]

    db = SessionLocal()
    db.query(SpecialPost).filter_by(special_id=sid).update({"status": "published", "external_post_id": "555"})
    conn = MetaSocialConnectionRepository().upsert(db, loc, meta_user_id="1", page_id="111", page_name="Demo", instagram_business_account_id=None, permissions_json={}, connection_status="connected")
    MetaTokenStore().save_social_token(db, conn, "page-token")
    db.add(MetaAdAccountConnection(location_id=loc, ad_account_id="999", ad_account_name="Demo ads", currency="ZAR", encrypted_access_token=MetaTokenStore().encrypt("ads-token")))
    db.commit(); db.close()

    with patch("flyer_lady.boost_service.BoostService.boost", return_value={"campaign_id": "c1", "adset_id": "s1", "ad_id": "a1", "ends_at": datetime.now(timezone.utc), "area": "Pretoria"}) as boost, \
         patch("routes.flyer_lady._ads_graph", return_value=None):
        resp = boost_now()
    assert resp.status_code == 201, resp.get_data(as_text=True)
    kw = boost.call_args.kwargs
    assert (kw["token"], kw["page_token"], kw["ad_account_id"], kw["budget"]) == ("ads-token", "page-token", "999", 200)
    db = SessionLocal()
    try:
        row = db.query(FlyerBoost).filter_by(location_id=loc).one()
        assert (row.campaign_id, row.budget_minor, row.area) == ("c1", 20000, "Pretoria")
    finally:
        db.close()
    listing = ctx["client"].get("/dashboard/flyer-lady").get_json()
    mine = next(s for s in listing["specials"] if s["id"] == sid)
    assert mine["facebook_published"] and mine["boosts"][0]["budget"] == 200
    assert listing["ads_connection"]["connected"]

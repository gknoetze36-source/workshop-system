"""Flyer Lady special -> Google Search ad (flyer_lady/google_ad_service.py, routes)."""
import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from flyer_lady.boost_service import BoostError
from flyer_lady.google_ad_service import GoogleAdService, ad_text, build_operations
from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)


def test_ad_text_respects_google_limits_and_strips_emojis():
    headlines, descriptions = ad_text("Knoetze Motors Pretoria East Branch", "🔥 R999 major service this week only!!! Includes oil, filters and a 40-point check on any sedan or hatchback.")
    assert 3 <= len(headlines) <= 15 and 2 <= len(descriptions) <= 4
    assert all(0 < len(h) <= 30 for h in headlines) and all(0 < len(d) <= 90 for d in descriptions)
    assert len({h.lower() for h in headlines}) == len(headlines)
    assert not any("🔥" in t or "!" in t for t in headlines + descriptions)


def test_operations_are_one_linked_search_campaign():
    ops = build_operations("1234567890", name="R999 service", final_url="https://app.test/l/7", headlines=["a", "b", "c"],
                           descriptions=["d", "e"], budget_rand=200, days=3, area="Pretoria", radius_km=25, today=datetime(2026, 10, 13))
    budget = ops[0]["campaignBudgetOperation"]["create"]
    campaign = ops[1]["campaignOperation"]["create"]
    assert budget["amountMicros"] == "66660000" and budget["explicitlyShared"] is False  # R66.66/day, never over R200 total
    assert campaign["campaignBudget"] == budget["resourceName"] == "customers/1234567890/campaignBudgets/-1"
    assert campaign["containsEuPoliticalAdvertising"] == "DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING"
    assert (campaign["startDateTime"], campaign["endDateTime"]) == ("2026-10-13 00:00:00", "2026-10-15 23:59:59")
    assert campaign["advertisingChannelType"] == "SEARCH" and campaign["networkSettings"]["targetContentNetwork"] is False
    assert ops[2]["campaignCriterionOperation"]["create"]["proximity"]["address"] == {"cityName": "Pretoria", "countryCode": "ZA"}
    ad = ops[-1]["adGroupAdOperation"]["create"]
    assert ad["adGroup"] == ops[3]["adGroupOperation"]["create"]["resourceName"]
    assert ad["ad"]["finalUrls"] == ["https://app.test/l/7"]
    assert any("keyword" in (o.get("adGroupCriterionOperation") or {}).get("create", {}) for o in ops)


def test_non_rand_account_is_refused_before_anything_is_created():
    client = MagicMock()
    client.search.return_value = [{"customer": {"currencyCode": "USD"}}]
    with pytest.raises(BoostError, match="USD"):
        GoogleAdService(client).create("tok", "123", workshop_name="W", special_text="R999 service", final_url="https://x",
                                       budget=200, days=2, area="Pretoria", radius_km=25, today=datetime(2026, 10, 13))
    client.mutate.assert_not_called()


def _headers(ctx):
    page = ctx["client"].get("/customers/manual-booking").get_data(as_text=True)
    return {"X-CSRFToken": re.search(r'name="csrf-token" content="([^"]+)"', page).group(1)}


def test_route_creates_google_ad_and_records_it(client_with_roles, monkeypatch):
    from database import execute_db, query_db
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "secret")
    monkeypatch.delenv("GOOGLE_ADS_DEVELOPER_TOKEN", raising=False)  # sunset by Google; must not be needed
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    headers = _headers(ctx)
    special_id = ctx["client"].post("/dashboard/flyer-lady/specials", json={"location_id": ctx["location_id"], "text": "R999 major service this week"}, headers=headers).get_json()["id"]
    base = f"/dashboard/flyer-lady/specials/{special_id}"
    ctx["client"].post(f"{base}/approval", json={"decision": "approved"}, headers=headers)
    from integrations.google.auth.token_store import GoogleTokenStore
    execute_db("INSERT INTO google_ads_connections (location_id, encrypted_refresh_token, customer_id, token_key_version, connection_status, connected_at) VALUES (%s, %s, %s, 'v1', 'connected', CURRENT_TIMESTAMP)",
               (ctx["location_id"], GoogleTokenStore().encrypt("refresh"), "1234567890"))

    body = {"budget": 200, "days": 5, "area": "Pretoria", "radius_km": 25}
    with patch("integrations.google.ads.api_client.GoogleAdsApiClient.refresh_access_token", return_value="access"), \
         patch("integrations.google.ads.api_client.GoogleAdsApiClient.search", return_value=[{"customer": {"currencyCode": "ZAR"}}]), \
         patch("integrations.google.ads.api_client.GoogleAdsApiClient.mutate", return_value=[
             {"campaignBudgetResult": {"resourceName": "customers/1234567890/campaignBudgets/11"}},
             {"campaignResult": {"resourceName": "customers/1234567890/campaigns/22"}},
             {"adGroupResult": {"resourceName": "customers/1234567890/adGroups/33"}},
             {"adGroupAdResult": {"resourceName": "customers/1234567890/adGroupAds/33~44"}}]) as mutate:
        resp = ctx["client"].post(f"{base}/google-ad", json=body, headers=headers)
        assert resp.status_code == 201, resp.get_data(as_text=True)
        assert mutate.call_args.args[1] == "1234567890"
        again = ctx["client"].post(f"{base}/google-ad", json=body, headers=headers)
    assert again.status_code == 400 and "already" in again.get_json()["error"]
    row = query_db("SELECT platform, ad_account_id, campaign_id, adset_id, ad_id, budget_minor FROM flyer_lady_boosts WHERE special_id=%s", (special_id,), one=True)
    assert dict(row) == {"platform": "google", "ad_account_id": "1234567890", "campaign_id": "22", "adset_id": "33", "ad_id": "44", "budget_minor": 20000}
    data = ctx["client"].get("/dashboard/flyer-lady").get_json()
    assert data["google_ads_connection"]["ready"] is True
    assert next(s for s in data["specials"] if s["id"] == special_id)["boosts"][0]["platform"] == "google"


def test_route_refuses_unapproved_special_and_missing_connection(client_with_roles, monkeypatch):
    monkeypatch.delenv("GOOGLE_ADS_DEVELOPER_TOKEN", raising=False)  # sunset by Google; must not be needed
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    headers = _headers(ctx)
    special_id = ctx["client"].post("/dashboard/flyer-lady/specials", json={"location_id": ctx["location_id"], "text": "R999 service"}, headers=headers).get_json()["id"]
    body = {"budget": 200, "days": 5, "area": "Pretoria", "radius_km": 25}
    resp = ctx["client"].post(f"/dashboard/flyer-lady/specials/{special_id}/google-ad", json=body, headers=headers)
    assert resp.status_code == 400 and "Approve" in resp.get_json()["error"]
    ctx["client"].post(f"/dashboard/flyer-lady/specials/{special_id}/approval", json={"decision": "approved"}, headers=headers)
    resp = ctx["client"].post(f"/dashboard/flyer-lady/specials/{special_id}/google-ad", json=body, headers=headers)
    assert resp.status_code == 400 and "Connect Google Ads" in resp.get_json()["error"]


class _SpendClient:
    def __init__(self, cost_micros):
        self.cost_micros, self.mutations = cost_micros, []

    def search(self, token, customer_id, query):
        return [{"metrics": {"costMicros": str(self.cost_micros), "impressions": "10", "clicks": "2"}}]

    def mutate(self, token, customer_id, operations):
        self.mutations.append(operations)
        return []


def test_google_ad_is_paused_once_the_workshops_amount_is_spent():
    from flyer_lady.google_ad_service import GoogleAdService
    under = _SpendClient(399_990_000)
    assert GoogleAdService(under).pause_if_spent("t", "123", "77", 400) is False and not under.mutations
    over = _SpendClient(412_000_000)  # Google overspent on a busy day
    assert GoogleAdService(over).pause_if_spent("t", "123", "77", 400) is True
    op = over.mutations[0][0]["campaignOperation"]
    assert op == {"update": {"resourceName": "customers/123/campaigns/77", "status": "PAUSED"}, "updateMask": "status"}


def test_spend_guard_job_marks_paused_ads(client_with_roles):
    from datetime import timedelta, timezone
    from database import SessionLocal
    from flyer_lady.models import FlyerBoost, Special
    from jobs.flyer_lady import run_google_ad_spend_guard
    loc = client_with_roles["location_id"]
    db = SessionLocal()
    special = Special(location_id=loc, created_by="t", text="R999 service", booking_link="https://x/book", status="approved")
    db.add(special); db.flush()
    db.add(FlyerBoost(location_id=loc, special_id=special.id, platform="google", ad_account_id="123", campaign_id="77",
                      budget_minor=40000, currency="ZAR", days=7, area="Pretoria", radius_km=25, status="active",
                      ends_at=datetime.now(timezone.utc) + timedelta(days=3)))
    db.commit(); db.close()
    with patch("flyer_lady.google_ad_service.connect", return_value=(_SpendClient(412_000_000), "t", None)):
        result = run_google_ad_spend_guard()
    assert result["paused"] >= 1
    db = SessionLocal()
    try:
        assert db.query(FlyerBoost).filter_by(location_id=loc, campaign_id="77").one().status == "budget_reached"
    finally:
        db.close()

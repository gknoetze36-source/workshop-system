"""Boost a published Flyer Lady Facebook post with the workshop's own Meta ad account.

Marketing API order: campaign -> ad set -> creative (the existing Page post) -> ad,
all created PAUSED-first so nothing spends until the whole chain exists.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from integrations.meta.services.graph_api_client import MetaGraphAPIError

logger = logging.getLogger(__name__)

MIN_BUDGET, MAX_BUDGET = 50, 20000  # Rand, whole boost
MAX_DAYS = 30
MIN_RADIUS_KM, MAX_RADIUS_KM = 17, 80  # Meta's allowed radius around a city


class BoostError(ValueError):
    """A boost problem the workshop can act on; the message is shown as-is."""


def meta_message(exc: MetaGraphAPIError) -> str:
    err = getattr(exc, "error", None) or {}
    return err.get("error_user_msg") or err.get("message") or str(exc)


def validate(budget, days, area, radius_km) -> tuple[int, int, str, int]:
    try:
        budget, days, radius_km = int(budget), int(days), int(radius_km or MIN_RADIUS_KM)
    except (TypeError, ValueError):
        raise BoostError("Budget, days and radius must be whole numbers.")
    area = (area or "").strip()
    if not MIN_BUDGET <= budget <= MAX_BUDGET:
        raise BoostError(f"Budget must be between R{MIN_BUDGET} and R{MAX_BUDGET}.")
    if not 1 <= days <= MAX_DAYS:
        raise BoostError(f"Days must be between 1 and {MAX_DAYS}.")
    if not area:
        raise BoostError("Enter the town or city the ad should reach, e.g. Pretoria.")
    return budget, days, area[:120], max(MIN_RADIUS_KM, min(MAX_RADIUS_KM, radius_km))


class BoostService:
    def __init__(self, client):
        self.client = client  # GraphApiClient for the Flyer Lady app

    def story_id(self, page_token: str, external_post_id: str) -> str:
        """object_story_id is "{page_id}_{post_id}"; photo posts store the photo ID."""
        if "_" in external_post_id:
            return external_post_id
        story = self.client.get_with_token(page_token, f"/{external_post_id}", params={"fields": "page_story_id"}).get("page_story_id")
        if not story:
            raise BoostError("Facebook didn't return the post for this special. Publish it to the Facebook feed first.")
        return story

    def city_key(self, token: str, area: str) -> tuple[str, str]:
        found = self.client.get_with_token(token, "/search", params={
            "type": "adgeolocation", "q": area, "location_types": json.dumps(["city"]), "country_code": "ZA", "limit": 1,
        }).get("data") or []
        if not found:
            raise BoostError(f"Meta couldn't find a South African town called '{area}'. Try the nearest bigger town.")
        return str(found[0]["key"]), found[0].get("name") or area

    def boost(self, *, token: str, ad_account_id: str, page_token: str, external_post_id: str,
              name: str, budget: int, days: int, area: str, radius_km: int, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        act = f"/act_{ad_account_id}"
        story = self.story_id(page_token, external_post_id)
        key, city = self.city_key(token, area)
        end = now + timedelta(days=days)
        campaign_id = None
        try:
            campaign_id = self.client.post_with_token(token, f"{act}/campaigns", data={
                "name": f"Flyer Lady: {name}"[:250], "objective": "OUTCOME_AWARENESS", "status": "PAUSED",
                "special_ad_categories": "[]", "is_adset_budget_sharing_enabled": "false",
            })["id"]
            adset_id = self.client.post_with_token(token, f"{act}/adsets", data={
                "name": f"{city} {radius_km}km", "campaign_id": campaign_id, "status": "ACTIVE",
                "lifetime_budget": str(budget * 100),  # minor units (cents)
                "start_time": now.isoformat(), "end_time": end.isoformat(),
                "billing_event": "IMPRESSIONS", "optimization_goal": "REACH", "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                "targeting": json.dumps({
                    "geo_locations": {"cities": [{"key": key, "radius": radius_km, "distance_unit": "kilometer"}]},
                    "age_min": 18, "targeting_automation": {"advantage_audience": 0},
                }),
            })["id"]
            creative_id = self.client.post_with_token(token, f"{act}/adcreatives", data={"name": f"Flyer Lady post {story}", "object_story_id": story})["id"]
            ad_id = self.client.post_with_token(token, f"{act}/ads", data={
                "name": f"Flyer Lady: {name}"[:250], "adset_id": adset_id, "status": "ACTIVE",
                "creative": json.dumps({"creative_id": creative_id}),
            })["id"]
            self.client.post_with_token(token, f"/{campaign_id}", data={"status": "ACTIVE"})
        except MetaGraphAPIError as exc:
            if campaign_id:  # don't leave a half-built campaign behind in their account
                try:
                    self.client.delete_with_token(token, f"/{campaign_id}")
                except MetaGraphAPIError:
                    logger.warning("flyer_boost_cleanup_failed campaign_id=%s", campaign_id)
            raise BoostError("Meta refused the boost: " + meta_message(exc)) from exc
        return {"campaign_id": campaign_id, "adset_id": adset_id, "ad_id": ad_id, "ends_at": end, "area": city}

    def results(self, token: str, campaign_id: str) -> dict:
        rows = self.client.get_with_token(token, f"/{campaign_id}/insights", params={"fields": "spend,reach,impressions,clicks", "date_preset": "maximum"}).get("data") or [{}]
        return {k: rows[0].get(k, "0") for k in ("spend", "reach", "impressions", "clicks")}

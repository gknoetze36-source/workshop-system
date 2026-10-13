"""Run a Flyer Lady special as a Google Search ad in the workshop's own Google
Ads account (Google bills the workshop directly, like Meta boosts).

One atomic googleAds:mutate request (v25) creates budget -> campaign ->
location -> ad group -> keywords -> responsive search ad, linked by temporary
negative resource IDs, so a failure leaves nothing half-built behind.
Maximize Clicks bidding, Google Search only, around the workshop's town.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta

from integrations.google.ads.api_client import GoogleAdsAPIError

from .boost_service import BoostError

# Phrase-match searches a car workshop wants to show up for.
KEYWORDS = ("car service", "vehicle service", "car repairs", "mechanic", "car maintenance", "brake repairs", "service my car")
_NOT_AD_TEXT = re.compile(r"[^\w\s.,&%+'/:()R-]", re.UNICODE)  # Google rejects emojis and most symbols


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", _NOT_AD_TEXT.sub(" ", text or "")).strip()


def _fit(text: str, limit: int) -> str:
    """Trim to the limit at a word boundary."""
    text = _clean(text)
    if len(text) <= limit:
        return text
    cut = text[:limit + 1].rsplit(" ", 1)[0]
    return (cut if cut else text[:limit]).rstrip(" ,.-")


def _unique(items, limit, minimum, fallbacks):
    seen, out = set(), []
    for item in list(items) + list(fallbacks):
        item = _fit(item, limit)
        if item and item.lower() not in seen:
            seen.add(item.lower()); out.append(item)
    if len(out) < minimum:
        raise BoostError("Couldn't write enough ad text from this special. Make the special text longer.")
    return out


def ad_text(workshop_name: str, special_text: str) -> tuple[list[str], list[str]]:
    """Responsive search ad: 3-15 headlines of <=30 chars, 2-4 descriptions of <=90."""
    first_line = (special_text or "").strip().splitlines()[0] if (special_text or "").strip() else ""
    headlines = _unique([workshop_name, first_line, special_text], 30, 3,
                        ["Book Your Car Service Online", "Car Service Specials", "Trusted Local Workshop"])[:15]
    descriptions = _unique([special_text], 90, 2,
                           [f"Book online with {workshop_name} in under a minute.", "See this week's special and book your car in online."])[:4]
    return headlines, descriptions


def build_operations(customer_id: str, *, name: str, final_url: str, headlines: list[str], descriptions: list[str],
                     budget_rand: int, days: int, area: str, radius_km: int, today: datetime) -> list[dict]:
    c = f"customers/{customer_id}"
    budget_rn, campaign_rn, ad_group_rn = f"{c}/campaignBudgets/-1", f"{c}/campaigns/-2", f"{c}/adGroups/-3"
    # Daily budget, rounded down to whole cents so the total stays within what the workshop agreed.
    daily_micros = (budget_rand * 1_000_000 // days) // 10_000 * 10_000
    end = today + timedelta(days=days - 1)
    ops = [
        {"campaignBudgetOperation": {"create": {"resourceName": budget_rn, "name": f"Flyer Lady: {name}"[:255],
                                                "amountMicros": str(daily_micros), "deliveryMethod": "STANDARD", "explicitlyShared": False}}},
        {"campaignOperation": {"create": {
            "resourceName": campaign_rn, "name": f"Flyer Lady: {name}"[:255], "status": "ENABLED",
            "advertisingChannelType": "SEARCH", "campaignBudget": budget_rn, "targetSpend": {},
            "networkSettings": {"targetGoogleSearch": True, "targetSearchNetwork": False,
                                "targetContentNetwork": False, "targetPartnerSearchNetwork": False},
            "containsEuPoliticalAdvertising": "DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING",
            "startDateTime": today.strftime("%Y-%m-%d 00:00:00"), "endDateTime": end.strftime("%Y-%m-%d 23:59:59"),
        }}},
        {"campaignCriterionOperation": {"create": {"campaign": campaign_rn, "proximity": {
            "address": {"cityName": area, "countryCode": "ZA"}, "radius": radius_km, "radiusUnits": "KILOMETERS"}}}},
        {"adGroupOperation": {"create": {"resourceName": ad_group_rn, "name": f"{area} {radius_km}km"[:255],
                                         "campaign": campaign_rn, "status": "ENABLED", "type": "SEARCH_STANDARD"}}},
    ]
    ops += [{"adGroupCriterionOperation": {"create": {"adGroup": ad_group_rn, "status": "ENABLED",
                                                      "keyword": {"text": k, "matchType": "PHRASE"}}}} for k in KEYWORDS]
    ops.append({"adGroupAdOperation": {"create": {"adGroup": ad_group_rn, "status": "ENABLED", "ad": {
        "finalUrls": [final_url],
        "responsiveSearchAd": {"headlines": [{"text": h} for h in headlines], "descriptions": [{"text": d} for d in descriptions]},
    }}}})
    return ops


def _id(resource_name: str) -> str:
    return resource_name.rsplit("/", 1)[-1].split("~")[-1]


class GoogleAdService:
    def __init__(self, client):
        self.client = client  # GoogleAdsApiClient

    def resolve_customer(self, access_token: str) -> str:
        ids = self.client.list_accessible_customers(access_token)
        if not ids:
            raise BoostError("This Google login has no Google Ads account. Create one at ads.google.com, add a payment method, then connect again.")
        if len(ids) > 1:
            raise BoostError("This Google login has more than one Google Ads account. Reconnect Google Ads and pick the workshop's account.")
        return ids[0]

    def create(self, access_token: str, customer_id: str, *, workshop_name: str, special_text: str, final_url: str,
               budget: int, days: int, area: str, radius_km: int, today: datetime) -> dict:
        try:
            account = (self.client.search(access_token, customer_id, "SELECT customer.currency_code FROM customer LIMIT 1") or [{}])[0]
            currency = (account.get("customer") or {}).get("currencyCode") or "ZAR"
            if currency != "ZAR":
                raise BoostError(f"Your Google Ads account uses {currency}; Flyer Lady budgets are in Rand, so it must be a ZAR account.")
            headlines, descriptions = ad_text(workshop_name, special_text)
            results = self.client.mutate(access_token, customer_id, build_operations(
                customer_id, name=_fit(special_text, 60) or "special", final_url=final_url, headlines=headlines,
                descriptions=descriptions, budget_rand=budget, days=days, area=area, radius_km=radius_km, today=today))
        except GoogleAdsAPIError as exc:
            raise BoostError("Google refused the ad: " + str(exc).split(": ", 1)[-1]) from exc
        by_type = {k: v.get("resourceName") for r in results for k, v in r.items() if isinstance(v, dict) and v.get("resourceName")}
        return {"campaign_id": _id(by_type.get("campaignResult", "")), "ad_group_id": _id(by_type.get("adGroupResult", "")),
                "ad_id": _id(by_type.get("adGroupAdResult", "")), "ends_at": today + timedelta(days=days)}

    def results(self, access_token: str, customer_id: str, campaign_id: str) -> dict:
        try:
            rows = self.client.search(access_token, customer_id,
                                      f"SELECT metrics.cost_micros, metrics.impressions, metrics.clicks FROM campaign WHERE campaign.id = {int(campaign_id)}")
        except GoogleAdsAPIError as exc:
            raise BoostError(str(exc)) from exc
        cost = sum(int((r.get("metrics") or {}).get("costMicros") or 0) for r in rows)
        total = lambda key: str(sum(int((r.get("metrics") or {}).get(key) or 0) for r in rows))
        return {"spend": f"{cost / 1_000_000:.2f}", "reach": total("impressions"), "impressions": total("impressions"), "clicks": total("clicks")}

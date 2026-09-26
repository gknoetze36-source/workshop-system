"""Google Ads adapter -- PLANNED. No method below makes a real API call.

Grounded against Google's actual, current API (verified this session,
not assumed): a gRPC/protobuf API, currently v25 (released 22 July
2026), queried via GoogleAdsService using GAQL (Google Ads Query
Language) rather than a REST resource-per-endpoint shape; core
resources include Campaign, CampaignBudget, CampaignCriterion,
AdGroup; a customer_id (not an "account ID" in Meta's sense)
identifies the ad account, under a manager-account hierarchy for
agencies/multi-account setups. Two live, current constraints worth
recording precisely rather than discovering during implementation: a
Passkey authentication requirement for API access was announced 27
July 2026 (on top of, not instead of, OAuth), and demand-gen
campaigns carry a minimum daily budget floor (USD 5 or local
equivalent) enforced as a hard validation rule as of 1 April 2026 --
a budget spec below that is rejected outright, not silently clamped.

Deliberately separate from Google Business Profile's connector
(flyer_lady/connectors/google_business.py) and its OAuth config
(FlyerLadyGoogleConfig or equivalent) -- same reasoning as
meta_ads.py's own docstring: a shared vendor is not a shared
credential or permission boundary. Google Ads needs its own OAuth
client and its own developer token, entirely separate from whatever
scope Google Business Profile already has.

Why nothing is implemented yet: same reasoning as Meta Ads --
PLANNED means no concrete VANTA workshop requirement has driven this
yet, and building ahead of that risks guessing at a campaign-spec
shape the API's own fast version cadence (v23->v24->v25 inside this
session's own research window) would likely invalidate before a real
workshop ever used it.
"""
from __future__ import annotations

from typing import Any

from advertising.capabilities import PlatformCapability


class GoogleAdsConnector:
    platform_key = "google_ads"

    def capabilities(self) -> frozenset[PlatformCapability]:
        return frozenset()

    def authorize_url(self, *, redirect_uri: str, state: str) -> str:
        raise NotImplementedError(
            "Google Ads is PLANNED, not IMPLEMENTING -- requires its own "
            "OAuth client and developer token, separate from Google Business "
            "Profile's existing OAuth config. Not registered yet."
        )

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict[str, Any]:
        raise NotImplementedError(
            "Google Ads is PLANNED -- also note the Passkey authentication "
            "requirement announced 27 July 2026, on top of OAuth, which "
            "needs designing into the flow before this can move past "
            "PLANNED, not discovered mid-IMPLEMENTING."
        )

    def discover_ad_accounts(self, *, token: str) -> list[dict[str, Any]]:
        raise NotImplementedError(
            "Google Ads is PLANNED -- would query accessible customer_ids "
            "via GoogleAdsService once built. Not implemented."
        )

    def revoke(self, session, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("Google Ads is PLANNED.")

    def health(self, session, location_id: int) -> dict[str, Any]:
        return {
            "platform": self.platform_key,
            "deployment_configured": False,
            "connection_status": "not_connected",
            "activation_state": "planned",
        }

    def create_campaign(self, session, location_id: int, spec: dict[str, Any]) -> Any:
        raise NotImplementedError(
            "Google Ads is PLANNED -- would create a Campaign + CampaignBudget "
            "resource pair via GoogleAdsService.Mutate once built. Note: a "
            "demand-gen campaign's budget below USD 5/day (or local "
            "equivalent) is a hard validation failure as of 1 April 2026, "
            "not a value to silently clamp -- whichever adapter eventually "
            "implements this must surface that rejection to the caller."
        )

    def set_budget(self, session, location_id: int, campaign_id: str, spec: dict[str, Any]) -> Any:
        raise NotImplementedError("Google Ads is PLANNED.")

    def fetch_spend(self, session, location_id: int, campaign_id: str) -> Any:
        raise NotImplementedError("Google Ads is PLANNED.")

    def fetch_analytics(self, session, location_id: int, campaign_id: str) -> Any:
        raise NotImplementedError("Google Ads is PLANNED.")

    def fetch_leads(self, session, location_id: int, campaign_id: str) -> Any:
        raise NotImplementedError("Google Ads is PLANNED.")

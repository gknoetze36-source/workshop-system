"""Meta Ads adapter -- PLANNED. No method below makes a real API call.

Grounded against Meta's actual, current Marketing API structure (not
assumed): Business Manager -> Ad Account (act_{id}) -> Campaign -> Ad
Set -> Ad -> Ad Creative, in that strict creation order; ODAX
("outcome-driven ad experiences") simplified objectives
(OUTCOME_AWARENESS/TRAFFIC/ENGAGEMENT/LEADS/SALES/APP_PROMOTION) as the
only currently-creatable objective family, legacy objectives long
since deprecated across API versions; Advantage+ campaign structures
as Meta's current direction, with Advantage+ Shopping/App campaign
creation itself deprecated as of v25 (Feb 2026, fully sunset by May
2026) in favor of the unified Advantage+ structure. This adapter does
not target a specific Graph API version yet -- picking one is
implementation work, not architecture, and version pinning belongs in
IMPLEMENTING, not PLANNED.

Deliberately separate from Meta's ORGANIC connector
(flyer_lady/connectors/meta_social.py) and from its own OAuth App
(integrations/meta/auth/capability_config.py's FlyerLadyMetaConfig) --
per the explicit rule to keep Meta products architecturally related
without sharing credentials or permission assumptions "merely because
they are all Meta products." A Meta Ads integration needs its own
Business Manager-level OAuth grant (ads_management permission scope)
and its own App configuration entirely, once built -- not a reuse of
Flyer Lady's Facebook Login App or WhatsApp's Business App.

Why nothing is implemented yet: the Marketing API is under continuous,
fast-moving deprecation cycles (confirmed this session -- three major
version bumps' worth of breaking changes to campaign creation in the
last two years, most recently the Advantage+ Shopping/App sunset
completing May 2026). Building against it prematurely, without a
concrete campaign-management requirement from an actual VANTA
workshop to build toward, risks shipping code against an API shape
that's already scheduled for deprecation. Per the explicit instruction
not to implement unsupported platform behaviour: this stays PLANNED.
"""
from __future__ import annotations

from typing import Any

from advertising.capabilities import PlatformCapability


class MetaAdsConnector:
    platform_key = "meta_ads"

    def capabilities(self) -> frozenset[PlatformCapability]:
        # Empty, not aspirational: nothing below actually works yet.
        return frozenset()

    def authorize_url(self, *, redirect_uri: str, state: str) -> str:
        raise NotImplementedError(
            "Meta Ads is PLANNED, not IMPLEMENTING -- no OAuth App has been "
            "registered for it yet. Requires its own Meta App with the "
            "ads_management permission, separate from Flyer Lady's Facebook "
            "Login App and WhatsApp's Business App."
        )

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict[str, Any]:
        raise NotImplementedError("Meta Ads is PLANNED -- see authorize_url().")

    def discover_ad_accounts(self, *, token: str) -> list[dict[str, Any]]:
        raise NotImplementedError(
            "Meta Ads is PLANNED -- would call GET /me/adaccounts once built, "
            "not implemented."
        )

    def revoke(self, session, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("Meta Ads is PLANNED.")

    def health(self, session, location_id: int) -> dict[str, Any]:
        return {
            "platform": self.platform_key,
            "deployment_configured": False,
            "connection_status": "not_connected",
            "activation_state": "planned",
        }

    def create_campaign(self, session, location_id: int, spec: dict[str, Any]) -> Any:
        raise NotImplementedError(
            "Meta Ads is PLANNED -- would create act_{ad_account_id}/campaigns "
            "with an ODAX objective once built. Not implemented."
        )

    def set_budget(self, session, location_id: int, campaign_id: str, spec: dict[str, Any]) -> Any:
        raise NotImplementedError("Meta Ads is PLANNED.")

    def fetch_spend(self, session, location_id: int, campaign_id: str) -> Any:
        raise NotImplementedError("Meta Ads is PLANNED.")

    def fetch_analytics(self, session, location_id: int, campaign_id: str) -> Any:
        raise NotImplementedError("Meta Ads is PLANNED.")

    def fetch_leads(self, session, location_id: int, campaign_id: str) -> Any:
        raise NotImplementedError("Meta Ads is PLANNED.")

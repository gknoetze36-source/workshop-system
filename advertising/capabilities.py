"""The shared capability vocabulary every platform in the Marketing &
Advertising Platform Engine (Layer 8) is described through.

This is deliberately the ONE thing genuinely shared across all eight
platforms -- Facebook, Instagram, Threads, Meta Ads, Google Business
Profile, Google Ads, X, TikTok. Everything else that differs (auth
flow, object model, API shape) stays in each platform's own adapter.
A platform declares which of these it supports; nothing here assumes
a platform supports all of them, and nothing here implements any of
them -- this module is vocabulary, not behaviour.

Eleven capabilities, exactly as specified. Not extended, not
abbreviated.
"""
from __future__ import annotations

from enum import Enum


class PlatformCapability(str, Enum):
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    AUTHENTICATE = "authenticate"
    PUBLISH = "publish"
    SCHEDULE = "schedule"
    ANALYTICS = "analytics"
    CAMPAIGNS = "campaigns"
    BUDGET = "budget"
    SPEND = "spend"
    LEADS = "leads"
    ATTRIBUTION = "attribution"


#: The four capabilities every organic Flyer Lady platform (Facebook,
#: Instagram, Threads, X, TikTok, Google Business Profile) already
#: implements, confirmed against flyer_lady/connectors/base.py's own
#: FlyerLadyConnector Protocol -- CONNECT/AUTHENTICATE via
#: authorize_url()+exchange_code(), DISCONNECT via revoke(), PUBLISH
#: via publish_step(). None of the six currently implement SCHEDULE
#: as a platform-native capability (Flyer Lady's own queue is what
#: schedules publishing; the platforms themselves just accept a
#: publish call when it arrives), and ANALYTICS is explicitly
#: NOT_IMPLEMENTED on every one of them (fetch_metrics() -- see each
#: connector's own docstring). CAMPAIGNS/BUDGET/SPEND/LEADS/ATTRIBUTION
#: do not apply to organic posting at all.
ORGANIC_CAPABILITIES = frozenset({
    PlatformCapability.CONNECT,
    PlatformCapability.DISCONNECT,
    PlatformCapability.AUTHENTICATE,
    PlatformCapability.PUBLISH,
})

#: What an advertising platform (Meta Ads, Google Ads) is expected to
#: eventually support once built -- not a claim that either currently
#: does. CAMPAIGNS/BUDGET/SPEND/ANALYTICS/ATTRIBUTION map onto real,
#: current concepts on both platforms (Meta's ad account -> campaign
#: -> ad set -> ad hierarchy; Google's GoogleAdsService campaign/
#: campaign_budget resources) -- LEADS depends on which objective a
#: given campaign uses (e.g. Meta's OUTCOME_LEADS) and is therefore
#: per-campaign, not a blanket platform capability; each adapter's own
#: capabilities() reports it accordingly rather than this constant
#: assuming it.
ADVERTISING_CAPABILITIES = frozenset({
    PlatformCapability.CONNECT,
    PlatformCapability.DISCONNECT,
    PlatformCapability.AUTHENTICATE,
    PlatformCapability.CAMPAIGNS,
    PlatformCapability.BUDGET,
    PlatformCapability.SPEND,
    PlatformCapability.ANALYTICS,
    PlatformCapability.ATTRIBUTION,
})

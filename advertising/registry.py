"""The single source of truth for where every platform in the
Marketing & Advertising Platform Engine actually stands.

Every GateEvidence value below is a claim traceable to a specific,
already-executed check this engagement produced -- DISCOVER, AUDIT,
PROVE, and GATE, in that order -- not a fresh assertion made here.
Where a criterion genuinely could not be checked in this environment
(no live Postgres, no live external API access -- confirmed
repeatedly, not assumed once and repeated), it is left False with a
note explaining why, never left blank to imply it passed.

This module does not import or wrap the six existing organic Flyer
Lady connectors (flyer_lady/connectors/) as AdvertisingPlatformConnector
implementations -- they are a different product with a different,
proven Protocol of their own (FlyerLadyConnector), and forcing a
conformance relationship between the two would blur exactly the
boundary the architecture is supposed to keep clean. This registry
records their real state as DATA, evidenced against their own test
suite and this session's audit trail, without requiring them to
implement a Protocol built for a product they aren't.
"""
from __future__ import annotations

from dataclasses import dataclass

from advertising.activation_state import ActivationState, GateEvidence
from advertising.capabilities import PlatformCapability, ORGANIC_CAPABILITIES
from advertising.connectors.meta_ads import MetaAdsConnector
from advertising.connectors.google_ads import GoogleAdsConnector


@dataclass(frozen=True)
class PlatformRecord:
    platform_key: str
    family: str  # "meta" | "google" | "other"
    product: str  # "organic" | "ads"
    activation_state: ActivationState
    capabilities: frozenset[PlatformCapability]
    gate_evidence: GateEvidence
    blockers: tuple[str, ...]
    connector: object | None  # the real connector instance where one exists


# The RLS caveat is identical across every organic platform's tenant
# isolation claim. Updated in a later loop than this registry was
# originally written: a genuinely disposable local PostgreSQL instance
# was subsequently stood up (not Railway, not production) and the real
# RLS suite (tests/test_rls_enforcement.py) was run against it -- 12/12
# passing under a properly restricted, NOBYPASSRLS role, built from a
# fresh database through the repository's own real migration chain, not
# a hand-built imitation. That closes "RLS enforcement never executed."
# It does not close "verified in production" -- this local run says
# nothing about PHANTA Production's actual deployed role grants or
# migration state, which were never touched or queried. Stated once,
# referenced by every record below rather than restated with slightly
# different wording each time.
_RLS_NOTE = (
    "local RLS implementation VERIFIED: the real migration/policy path "
    "run against a disposable PostgreSQL database under a genuinely "
    "restricted role (12/12 tests passing, proven fresh); production RLS "
    "configuration remains UNVERIFIED -- this says nothing about "
    "PHANTA Production's actual deployed state, which was never touched"
)
_NO_PROD_API_NOTE = "zero real external API calls made this engagement -- no network path to this provider in this environment"


PLATFORM_REGISTRY: dict[str, PlatformRecord] = {

    "facebook": PlatformRecord(
        platform_key="facebook", family="meta", product="organic",
        activation_state=ActivationState.VERIFIED,
        capabilities=ORGANIC_CAPABILITIES,
        gate_evidence=GateEvidence(
            connection_works=True, permissions_correct=True, account_resolution_works=True,
            capability_works=True, errors_handled=True,
            tenant_isolation_proven=False, tests_pass=True, production_verification_passes=False,
            notes={"tenant_isolation_proven": _RLS_NOTE, "production_verification_passes": _NO_PROD_API_NOTE},
        ),
        blockers=("Postgres RLS enforcement unverified", "no live Meta API call ever made"),
        connector=None,  # flyer_lady.connectors.registry.get_connector("facebook")
    ),

    "instagram": PlatformRecord(
        platform_key="instagram", family="meta", product="organic",
        activation_state=ActivationState.TESTING,
        capabilities=ORGANIC_CAPABILITIES,
        gate_evidence=GateEvidence(
            connection_works=True, permissions_correct=False, account_resolution_works=True,
            capability_works=False, errors_handled=True,
            tenant_isolation_proven=False, tests_pass=True, production_verification_passes=False,
            notes={
                "permissions_correct": "instagram_basic/instagram_content_publish never requested "
                                        "in FLYER_LADY_REQUIRED_PERMISSIONS -- confirmed deliberate, "
                                        "pending Meta App Review (AUDIT stage finding, unchanged)",
                "capability_works": "publish would 403 in production -- no token will ever carry the "
                                     "required scope until Meta approves it",
                "tenant_isolation_proven": _RLS_NOTE,
                "production_verification_passes": _NO_PROD_API_NOTE,
            },
        ),
        blockers=("Meta App Review for instagram_content_publish -- external, no code fix exists",
                  "Postgres RLS enforcement unverified"),
        connector=None,
    ),

    "threads": PlatformRecord(
        platform_key="threads", family="meta", product="organic",
        activation_state=ActivationState.CONNECTED,
        capabilities=ORGANIC_CAPABILITIES,
        gate_evidence=GateEvidence(
            connection_works=True, permissions_correct=True, account_resolution_works=True,
            capability_works=True, errors_handled=True,
            tenant_isolation_proven=False, tests_pass=True, production_verification_passes=False,
            notes={
                "connection_works": "connector code correct and unit-tested (16/16), but no web "
                                     "route exists anywhere in routes/ -- unreachable via the actual "
                                     "application, so 'connection works' is true of the code, not of "
                                     "anything a workshop could actually use today",
                "tenant_isolation_proven": _RLS_NOTE,
                "production_verification_passes": _NO_PROD_API_NOTE,
            },
        ),
        blockers=("no web route wired to this connector -- built and tested, not reachable",
                  "Postgres RLS enforcement unverified"),
        connector=None,
    ),

    "x": PlatformRecord(
        platform_key="x", family="other", product="organic",
        activation_state=ActivationState.CONNECTED,
        capabilities=ORGANIC_CAPABILITIES,
        gate_evidence=GateEvidence(
            connection_works=True, permissions_correct=True, account_resolution_works=True,
            capability_works=True, errors_handled=True,
            tenant_isolation_proven=False, tests_pass=True, production_verification_passes=False,
            notes={
                "connection_works": "connector code correct and unit-tested (26/26, including the "
                                     "per-tenant spend-guard suite), no web route exists",
                "tenant_isolation_proven": _RLS_NOTE,
                "production_verification_passes": _NO_PROD_API_NOTE,
            },
        ),
        blockers=("no web route wired to this connector", "Postgres RLS enforcement unverified"),
        connector=None,
    ),

    "tiktok": PlatformRecord(
        platform_key="tiktok", family="other", product="organic",
        activation_state=ActivationState.CONNECTED,
        capabilities=ORGANIC_CAPABILITIES,
        gate_evidence=GateEvidence(
            connection_works=True, permissions_correct=True, account_resolution_works=True,
            capability_works=False, errors_handled=True,
            tenant_isolation_proven=False, tests_pass=True, production_verification_passes=False,
            notes={
                "connection_works": "connector code correct and unit-tested (18/18), no web route exists",
                "capability_works": "TikTok itself restricts every unaudited client's posts to "
                                     "SELF_ONLY visibility -- the code is correct, but the actual "
                                     "capability (public posting) is not available until TikTok's own "
                                     "platform audit passes; external gate, no code fix exists",
                "tenant_isolation_proven": _RLS_NOTE,
                "production_verification_passes": _NO_PROD_API_NOTE,
            },
        ),
        blockers=("TikTok platform audit for public visibility -- external, no code fix exists",
                  "no web route wired to this connector", "Postgres RLS enforcement unverified"),
        connector=None,
    ),

    "google_business": PlatformRecord(
        platform_key="google_business", family="google", product="organic",
        activation_state=ActivationState.VERIFIED,
        capabilities=ORGANIC_CAPABILITIES,
        gate_evidence=GateEvidence(
            connection_works=True, permissions_correct=True, account_resolution_works=True,
            capability_works=True, errors_handled=True,
            tenant_isolation_proven=False, tests_pass=True, production_verification_passes=False,
            notes={"tenant_isolation_proven": _RLS_NOTE, "production_verification_passes": _NO_PROD_API_NOTE},
        ),
        blockers=("Postgres RLS enforcement unverified", "no live Google API call ever made"),
        connector=None,  # flyer_lady.connectors.registry.get_connector("google_business")
    ),

    "meta_ads": PlatformRecord(
        platform_key="meta_ads", family="meta", product="ads",
        activation_state=ActivationState.PLANNED,
        capabilities=frozenset(),
        gate_evidence=GateEvidence(notes={
            "__all__": "PLANNED -- architecture designed and grounded against the current "
                       "Marketing API structure this session; zero implementation exists. "
                       "Every method on MetaAdsConnector raises NotImplementedError honestly.",
        }),
        blockers=("no OAuth App registered for ads_management scope",
                  "no concrete workshop requirement driving implementation yet",
                  "Marketing API under active deprecation cycles -- see meta_ads.py's own docstring"),
        connector=MetaAdsConnector(),
    ),

    "google_ads": PlatformRecord(
        platform_key="google_ads", family="google", product="ads",
        activation_state=ActivationState.PLANNED,
        capabilities=frozenset(),
        gate_evidence=GateEvidence(notes={
            "__all__": "PLANNED -- architecture designed and grounded against the current "
                       "v25 Google Ads API this session; zero implementation exists.",
        }),
        blockers=("no OAuth client/developer token registered",
                  "Passkey authentication requirement (announced 27 Jul 2026) needs designing "
                  "into the auth flow before implementation starts",
                  "no concrete workshop requirement driving implementation yet"),
        connector=GoogleAdsConnector(),
    ),
}


def get_platform(platform_key: str) -> PlatformRecord:
    try:
        return PLATFORM_REGISTRY[platform_key]
    except KeyError:
        raise KeyError(
            f"No platform record for {platform_key!r}. Registered: {sorted(PLATFORM_REGISTRY)}"
        ) from None


def registered_platforms() -> list[str]:
    return sorted(PLATFORM_REGISTRY)


def active_platforms() -> list[str]:
    """Platforms genuinely at ACTIVE -- as of this registry's own
    evidence, this returns an empty list, and that emptiness is itself
    the correct, evidenced answer, not a bug: no platform in this
    engagement has cleared production_verification_passes, which
    every organic platform's gate_evidence explicitly and honestly
    records as False."""
    return sorted(
        key for key, record in PLATFORM_REGISTRY.items()
        if record.activation_state == ActivationState.ACTIVE
    )

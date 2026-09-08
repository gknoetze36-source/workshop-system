"""Documented Meta Graph API operations for the Tech Provider onboarding flow.

One method per Meta endpoint. Every method states, in its docstring, which
authorization context Meta requires and which business owns the object, so
the token choice is reviewable at the call site rather than buried in an
orchestrator.

All requests go through the existing :class:`GraphApiClient` chokepoint, so
the Graph API version comes from ``META_GRAPH_API_VERSION`` and is never
hardcoded here.

Two authorization contexts
--------------------------
PROVIDER  VANTA's System User token (``MetaProviderConfig.system_user_token``).
          Used for objects owned by VANTA's business portfolio (System User
          identity, extended credit line, allocation configs) and for WABA
          user administration, which Meta documents as requiring a System
          User token with admin permissions.

CLIENT    The customer's business integration token from Embedded Signup,
          decrypted per call. Used for objects owned by the client's
          business: their WABA node, phone number, webhook subscription and
          message templates.
"""
from __future__ import annotations

import json
from typing import Any, Iterator, Mapping, Sequence

from ..services.graph_api_client import GraphApiClient
from .provider_config import MetaProviderConfig


class TechProviderOperations:
    """Thin, documented wrapper over Graph API calls used during onboarding."""

    def __init__(self, client: GraphApiClient, provider: MetaProviderConfig):
        self.client = client
        self.provider = provider

    # -- helpers -----------------------------------------------------------

    @property
    def _provider_token(self) -> str:
        token = self.provider.system_user_token
        if not token:
            raise ValueError("META_SYSTEM_USER_TOKEN is required for provider operations")
        return token

    def _provider_get(self, path: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self.client.get_with_token(self._provider_token, path, params=params)

    def _provider_post(self, path: str, data: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self.client.post_with_token(self._provider_token, path, data=data)

    # =====================================================================
    # PROVIDER-SCOPED OPERATIONS (VANTA System User token)
    # =====================================================================

    def get_system_user_identity(self) -> dict[str, Any]:
        """GET /me?fields=id,name -- PROVIDER token.

        Resolves the System User the configured token actually represents.
        This is what makes ``META_SYSTEM_USER_ID`` verifiable instead of
        trusted: the configured value must match the ``id`` Meta returns.
        """
        return self._provider_get("/me", {"fields": "id,name"})

    def list_waba_system_users(self, waba_id: str) -> list[dict[str, Any]]:
        """GET /<WABA_ID>/system_users -- PROVIDER token.

        Meta: "Retrieve system user IDs". Returns the System Users visible on
        the shared WABA, each with id/name/role.
        """
        payload = self._provider_get(f"/{waba_id}/system_users")
        return list(payload.get("data") or [])

    def assign_system_user(
        self, waba_id: str, system_user_id: str, tasks: Sequence[str]
    ) -> dict[str, Any]:
        """POST /<WABA_ID>/assigned_users -- PROVIDER token.

        Meta documents this as requiring the access token of a System User
        with admin permissions. ``tasks`` is sent as a JSON array, which is
        the form Meta's documented ``tasks=['MANAGE']`` syntax expects.
        """
        return self._provider_post(
            f"/{waba_id}/assigned_users",
            data={"user": str(system_user_id), "tasks": json.dumps(list(tasks))},
        )

    def list_assigned_users(self, waba_id: str) -> list[dict[str, Any]]:
        """GET /<WABA_ID>/assigned_users?business=<WABA_ID> -- PROVIDER token.

        Meta's documented verification call for the assignment above. Each
        entry carries id/name/tasks.
        """
        payload = self._provider_get(
            f"/{waba_id}/assigned_users", {"business": str(waba_id)}
        )
        return list(payload.get("data") or [])

    def list_extended_credits(self, business_id: str) -> list[dict[str, Any]]:
        """GET /<BUSINESS_ID>/extendedcredits -- PROVIDER token.

        Returns VANTA's own extended credit line(s). Meta requires the System
        User to hold Admin or Financial Editor on the business portfolio and
        to have granted the app ``business_management``.
        """
        payload = self._provider_get(
            f"/{business_id}/extendedcredits", {"fields": "id,legal_entity_name"}
        )
        return list(payload.get("data") or [])

    def share_credit_line(
        self, credit_line_id: str, waba_id: str, waba_currency: str
    ) -> dict[str, Any]:
        """POST /<CREDIT_LINE_ID>/whatsapp_credit_sharing_and_attach -- PROVIDER token.

        Attaches VANTA's credit line to the client's WABA, which is what
        makes VANTA the "Bill To Party": clients pay VANTA, VANTA receives
        the aggregated Meta invoice.

        Meta returns ``{"success", "waba_id", "allocation_config_id"}``. The
        allocation config ID is the durable evidence of the share and is what
        later verification and revocation are performed against.
        """
        return self._provider_post(
            f"/{credit_line_id}/whatsapp_credit_sharing_and_attach",
            data={"waba_id": str(waba_id), "waba_currency": str(waba_currency).upper()},
        )

    def get_allocation_config(self, allocation_config_id: str) -> dict[str, Any]:
        """GET /<ALLOCATION_CONFIG_ID>?fields=receiving_credential -- PROVIDER token.

        Step 1 of Meta's documented "verifying shared status" procedure. The
        returned ``receiving_credential.id`` is compared against the client
        WABA's ``primary_funding_id``.
        """
        return self._provider_get(
            f"/{allocation_config_id}",
            {"fields": "receiving_credential,receiving_business"},
        )

    def find_allocation_configs(
        self, credit_line_id: str, receiving_business_id: str
    ) -> list[dict[str, Any]]:
        """GET /<CREDIT_LINE_ID>/owning_credit_allocation_configs -- PROVIDER token.

        Looks up an existing credit-sharing record for a client business.
        This is the idempotency probe: if a share already exists, the
        allocation config is recovered instead of a duplicate share being
        attempted.
        """
        payload = self._provider_get(
            f"/{credit_line_id}/owning_credit_allocation_configs",
            {
                "receiving_business_id": str(receiving_business_id),
                "fields": "id,receiving_business,receiving_credential",
            },
        )
        data = payload.get("data")
        if isinstance(data, list):
            return list(data)
        # Meta returns a bare object rather than a list in some responses.
        return [payload] if payload.get("id") else []

    # =====================================================================
    # CLIENT-SCOPED OPERATIONS (customer business token from Embedded Signup)
    # =====================================================================

    #: Fields read from the client's WABA node. ``owner_business_info`` gives
    #: the client's business portfolio ID (needed for credit verification),
    #: ``currency`` drives waba_currency, ``primary_funding_id`` proves the
    #: credit attachment landed.
    WABA_FIELDS = (
        "id,name,currency,timezone_id,message_template_namespace,"
        "account_review_status,owner_business_info,primary_funding_id"
    )

    #: Fields read from the client's business phone number node.
    #: ``platform_type`` == CLOUD_API and ``status`` == CONNECTED are the
    #: documented signals that registration has already happened.
    PHONE_FIELDS = (
        "id,display_phone_number,verified_name,quality_rating,"
        "code_verification_status,platform_type,status"
    )

    def get_waba(self, customer_token: str, waba_id: str, *, fields: str | None = None) -> dict[str, Any]:
        """GET /<WABA_ID>?fields=... -- CLIENT token.

        The WABA is owned by the client's business, so it is read with the
        client's token. A 400/404 here means the browser-supplied WABA ID
        does not belong to this Embedded Signup grant.
        """
        return self.client.get_with_token(
            customer_token, f"/{waba_id}", params={"fields": fields or self.WABA_FIELDS}
        )

    def get_waba_primary_funding_id(self, customer_token: str, waba_id: str) -> str | None:
        """GET /<WABA_ID>?fields=primary_funding_id -- CLIENT token.

        Step 2 of Meta's documented credit-share verification.
        """
        payload = self.client.get_with_token(
            customer_token, f"/{waba_id}", params={"fields": "primary_funding_id"}
        )
        value = payload.get("primary_funding_id")
        return str(value) if value else None

    def subscribe_app_to_waba(self, customer_token: str, waba_id: str) -> dict[str, Any]:
        """POST /<WABA_ID>/subscribed_apps -- CLIENT token.

        Subscribes the VANTA Meta app to webhooks on the client's WABA. Sent
        with no post body on purpose: a body would set an alternate callback
        URL and divert this client's webhooks away from PHANTA's receiver.
        """
        return self.client.post_with_token(customer_token, f"/{waba_id}/subscribed_apps")

    def list_subscribed_apps(self, customer_token: str, waba_id: str) -> list[dict[str, Any]]:
        """GET /<WABA_ID>/subscribed_apps -- CLIENT token.

        Meta's documented verification for the subscription. Each entry holds
        ``whatsapp_business_api_data`` with the subscribed app's numeric id.
        """
        payload = self.client.get_with_token(customer_token, f"/{waba_id}/subscribed_apps")
        return list(payload.get("data") or [])

    def get_phone_number(self, customer_token: str, phone_number_id: str, *, fields: str | None = None) -> dict[str, Any]:
        """GET /<PHONE_NUMBER_ID>?fields=... -- CLIENT token."""
        return self.client.get_with_token(
            customer_token,
            f"/{phone_number_id}",
            params={"fields": fields or self.PHONE_FIELDS},
        )

    def iter_message_templates(
        self, customer_token: str, waba_id: str, *, page_limit: int = 100, max_pages: int = 25
    ) -> Iterator[dict[str, Any]]:
        """GET /<WABA_ID>/message_templates -- CLIENT token.

        Yields template objects across Meta's cursor pagination. ``max_pages``
        bounds the walk so a pathological account cannot stall onboarding.
        """
        params: dict[str, Any] = {
            "fields": "id,name,language,category,status,reason,components",
            "limit": int(page_limit),
        }
        for _ in range(max_pages):
            payload = self.client.get_with_token(
                customer_token, f"/{waba_id}/message_templates", params=params
            )
            for item in payload.get("data") or []:
                if isinstance(item, dict):
                    yield item
            after = ((payload.get("paging") or {}).get("cursors") or {}).get("after")
            if not after or not (payload.get("paging") or {}).get("next"):
                return
            params = dict(params, after=after)

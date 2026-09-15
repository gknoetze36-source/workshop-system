"""Meta Tech Provider onboarding completion for VANTA Automations.

Successful Embedded Signup is the *start* of onboarding, not the end. A
client cannot send or receive WhatsApp messages through VANTA until every
step below has succeeded and been verified against Meta:

    signup_received -> token_exchanged -> waba_verified
    -> system_user_assigned -> system_user_verified
    -> phone_registered
    -> credit_line_retrieved -> credit_shared -> credit_verified
    -> webhook_subscribed -> webhook_verified
    -> templates_synced -> final_verification -> completed

Only after ``completed`` does ``connection_status`` become ``connected``.

Every step is independently idempotent and retry-safe. The pattern each one
follows is: read persisted evidence, verify against Meta where Meta exposes a
verification endpoint, perform the operation only when it has not already
happened, persist the result, then advance the recorded progress. A run that
fails at step N leaves steps 1..N-1 persisted, so ``resume_onboarding`` picks
up at N rather than repeating work or duplicating Meta-side state.

Security: access tokens are read, used and discarded. They are never
persisted outside the existing encrypted column, never returned by any method
here, and never written into ``last_onboarding_error`` -- error text is
scrubbed before it is stored.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.integration_models import MetaBusinessConnection

from ..auth.capability_config import WhatsAppMetaConfig
from ..auth.token_store import MetaTokenStore
from ..business.provider_config import (
    SUPPORTED_WABA_CURRENCIES,
    MetaProviderConfig,
    MetaProviderConfigError,
)
from ..business.tech_provider_operations import TechProviderOperations
from ..repositories.connection_repo import MetaConnectionRepository
from ..whatsapp.phone_number_service import PhoneNumberService, PhoneRegistrationError
from ..whatsapp.template_sync_service import MetaTemplateSyncService
from .graph_api_client import GraphApiClient, MetaGraphAPIError
from . import onboarding_state as state


# --------------------------------------------------------------------------
# Results and errors
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class StepOutcome:
    """What one onboarding step actually did."""

    step: str
    action: str  # "performed" | "already_done" | "skipped"
    detail: dict[str, Any] = field(default_factory=dict)


class OnboardingStepError(RuntimeError):
    """A named onboarding step failed.

    ``retryable`` distinguishes a transient Meta/network condition, where
    calling ``resume_onboarding`` again is the right response, from a
    configuration or eligibility problem that a human must fix first.
    """

    def __init__(
        self,
        step: str,
        message: str,
        *,
        retryable: bool = False,
        meta_code: Any = None,
        meta_status: int | None = None,
        remediation: str | None = None,
    ):
        super().__init__(message)
        self.step = step
        self.retryable = retryable
        self.meta_code = meta_code
        self.meta_status = meta_status
        self.remediation = remediation

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "message": str(self),
            "retryable": self.retryable,
            "meta_code": self.meta_code,
            "meta_status": self.meta_status,
            "remediation": self.remediation,
        }


class PhonePinRequired(OnboardingStepError):
    """Onboarding cannot register the phone number without a 6-digit PIN.

    This is a pause, not a failure: everything before it is persisted and the
    run resumes as soon as a PIN is supplied.
    """

    def __init__(self, message: str):
        super().__init__(
            state.STEP_PHONE_REGISTERED,
            message,
            retryable=True,
            remediation=(
                "Ask the workshop for a 6-digit WhatsApp two-step verification PIN, "
                "then resume onboarding with it."
            ),
        )


@dataclass
class OnboardingRunResult:
    location_id: int
    connection_id: int | None
    connection_status: str
    onboarding_step: str | None
    last_successful_step: str | None
    completed: bool
    outcomes: list[StepOutcome] = field(default_factory=list)
    failed_step: str | None = None
    error: dict[str, Any] | None = None
    awaiting_input: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "location_id": self.location_id,
            "connection_id": self.connection_id,
            "connection_status": self.connection_status,
            "onboarding_step": self.onboarding_step,
            "last_successful_step": self.last_successful_step,
            "completed": self.completed,
            "awaiting_input": self.awaiting_input,
            "steps": [
                {"step": o.step, "action": o.action, "detail": o.detail}
                for o in self.outcomes
            ],
            "failed_step": self.failed_step,
            "error": self.error,
        }


# --------------------------------------------------------------------------
# Error text scrubbing
# --------------------------------------------------------------------------

# Meta access tokens are long opaque strings that begin EAA... Any run of
# token-shaped characters is replaced before an error is persisted or
# returned, so a Meta error that happens to echo a token cannot leak into the
# database, an API response or a log line.
_TOKEN_SHAPED = re.compile(r"\b(?:EAA|EAB)[A-Za-z0-9_\-]{20,}\b")
_LONG_OPAQUE = re.compile(r"\b[A-Za-z0-9_\-]{60,}\b")


def scrub(text: Any, *, secrets: tuple[str, ...] = ()) -> str:
    """Remove token material from text destined for storage or a response."""
    value = "" if text is None else str(text)
    for secret in secrets:
        if secret and len(secret) >= 8:
            value = value.replace(secret, "[redacted]")
    value = _TOKEN_SHAPED.sub("[redacted]", value)
    value = _LONG_OPAQUE.sub("[redacted]", value)
    return value[:1000]


# --------------------------------------------------------------------------
# The service
# --------------------------------------------------------------------------


class TechProviderOnboardingService:
    """Completes Meta Tech Provider onboarding for one location's connection."""

    def __init__(
        self,
        config: WhatsAppMetaConfig | None = None,
        provider: MetaProviderConfig | None = None,
        client: GraphApiClient | None = None,
        operations: TechProviderOperations | None = None,
        token_store: MetaTokenStore | None = None,
        connection_repo: MetaConnectionRepository | None = None,
        phone_service: PhoneNumberService | None = None,
        template_sync: MetaTemplateSyncService | None = None,
    ):
        # S6/S13: Tech Provider operations -- WABA assignment, System User
        # assignment and credit-line sharing -- are WhatsApp App work.
        self.config = config or WhatsAppMetaConfig.from_env()
        self.provider = provider or MetaProviderConfig.from_env()
        self.client = client or GraphApiClient(self.config)
        self.operations = operations or TechProviderOperations(self.client, self.provider)
        self.token_store = token_store or MetaTokenStore()
        self.connection_repo = connection_repo or MetaConnectionRepository()
        self.phone_service = phone_service or PhoneNumberService(
            config=self.config, client=self.client, token_store=self.token_store,
            connection_repo=self.connection_repo,
        )
        self.template_sync = template_sync or MetaTemplateSyncService(self.operations)

    # -- small helpers -----------------------------------------------------

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _secrets(self, customer_token: str | None = None) -> tuple[str, ...]:
        return tuple(
            s for s in (self.provider.system_user_token, self.config.app_secret, customer_token) if s
        )

    def _connection(self, session: Session, location_id: int) -> MetaBusinessConnection:
        connection = self.connection_repo.get_for_location(session, location_id)
        if connection is None:
            raise OnboardingStepError(
                state.STEP_SIGNUP_RECEIVED,
                "No Meta WhatsApp connection exists for this workshop. Complete Embedded Signup first.",
            )
        return connection

    def _customer_token(self, connection: MetaBusinessConnection, step: str) -> str:
        if not connection.encrypted_access_token:
            raise OnboardingStepError(
                step,
                "The client's Meta business token is not stored. Reconnect WhatsApp.",
                remediation="Ask the workshop to run Connect WhatsApp again.",
            )
        try:
            return self.token_store.get_customer_token(connection)
        except ValueError as exc:
            raise OnboardingStepError(
                step,
                "The client's Meta business token cannot be decrypted with the configured key.",
                remediation=(
                    "Check META_TOKEN_ENCRYPTION_KEY matches the key the token was stored with, "
                    "then reconnect WhatsApp."
                ),
            ) from exc

    def _graph_error(
        self,
        step: str,
        exc: MetaGraphAPIError,
        message: str,
        *,
        customer_token: str | None = None,
        remediation: str | None = None,
    ) -> OnboardingStepError:
        status = exc.status_code
        # 5xx and rate limiting are transient; 4xx generally is not.
        retryable = status is None or status >= 500 or status == 429
        return OnboardingStepError(
            step,
            f"{message} Meta said: {scrub(exc, secrets=self._secrets(customer_token))}",
            retryable=retryable,
            meta_code=exc.error.get("code"),
            meta_status=status,
            remediation=remediation,
        )

    @staticmethod
    def _advance(connection: MetaBusinessConnection, step: str) -> None:
        """Record ``step`` as reached, never moving progress backwards."""
        if state.step_reached(connection.last_successful_onboarding_step, step):
            connection.onboarding_step = connection.last_successful_onboarding_step
            return
        connection.last_successful_onboarding_step = step
        connection.onboarding_step = step

    # =====================================================================
    # STEP: provider configuration
    # =====================================================================

    def verify_provider_configuration(self) -> StepOutcome:
        """Validate VANTA's own Meta credentials against Meta.

        META_SYSTEM_USER_ID is never trusted on its own. The System User the
        configured token actually represents is read from Meta, and a
        configured ID that disagrees is a hard configuration error rather
        than something to work around.
        """
        step = state.STEP_WABA_VERIFIED  # this runs as part of reaching waba_verified
        try:
            self.provider.validate()
        except MetaProviderConfigError as exc:
            raise OnboardingStepError(
                step,
                str(exc),
                remediation="Set the missing Meta provider environment variables and redeploy.",
            ) from exc

        try:
            identity = self.operations.get_system_user_identity()
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step,
                exc,
                "VANTA's Meta System User token could not authenticate.",
                remediation=(
                    "Regenerate META_SYSTEM_USER_TOKEN in Meta Business Settings > System Users "
                    "with whatsapp_business_management and business_management, and confirm the "
                    "System User holds Admin or Financial Editor on the business portfolio."
                ),
            ) from exc

        resolved_id = str(identity.get("id") or "")
        if not resolved_id:
            raise OnboardingStepError(
                step, "Meta did not return an identity for VANTA's System User token."
            )
        configured = self.provider.system_user_id
        if configured and configured != resolved_id:
            raise OnboardingStepError(
                step,
                "META_SYSTEM_USER_ID does not match the System User that META_SYSTEM_USER_TOKEN "
                "represents at Meta.",
                remediation=(
                    "Correct META_SYSTEM_USER_ID, or generate the token from the intended "
                    "System User."
                ),
            )
        return StepOutcome(
            "provider_configuration",
            "performed",
            {"system_user_id": resolved_id, "system_user_name": identity.get("name")},
        )

    # =====================================================================
    # STEP: WABA verification
    # =====================================================================

    def verify_waba(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Prove the browser-supplied WABA ID is real and belongs to this grant.

        The WABA ID arrives from the Embedded Signup window in the client's
        browser, so it is untrusted input. Three things are established here:
        the WABA exists; the client's own Embedded Signup token can read it
        (which is what ties it to this onboarding relationship); and no other
        PHANTA location is already mapped to it.
        """
        step = state.STEP_WABA_VERIFIED
        waba_id = (connection.waba_id or "").strip()
        if not waba_id:
            raise OnboardingStepError(step, "Embedded Signup did not return a WhatsApp Business Account ID.")
        if not connection.phone_number_id:
            raise OnboardingStepError(step, "Embedded Signup did not return a business phone number ID.")

        self._assert_tenant_isolation(session, connection)

        token = self._customer_token(connection, step)

        # Strongest available proof of the grant: the token's granular scopes
        # name the WABAs it was issued for. Meta does not always populate
        # target_ids, so this is enforced only when present.
        self._assert_token_targets_waba(step, token, waba_id)

        try:
            waba = self.operations.get_waba(token, waba_id)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step,
                exc,
                "The WhatsApp Business Account returned by Embedded Signup could not be read "
                "with the client's own token.",
                customer_token=token,
                remediation="Ask the workshop to run Connect WhatsApp again and select their business account.",
            ) from exc

        returned_id = str(waba.get("id") or "")
        if returned_id and returned_id != waba_id:
            raise OnboardingStepError(
                step,
                "Meta returned a different WhatsApp Business Account than the one Embedded Signup reported.",
            )

        owner = waba.get("owner_business_info") or {}
        owner_id = str(owner.get("id")) if owner.get("id") else None
        currency = str(waba.get("currency")).upper() if waba.get("currency") else None

        connection.owner_business_id = owner_id or connection.owner_business_id
        connection.waba_currency = currency or connection.waba_currency
        if not connection.business_id and owner_id:
            connection.business_id = owner_id
        self._advance(connection, step)
        session.flush()

        return StepOutcome(
            step,
            "performed",
            {
                "waba_id": waba_id,
                "waba_name": waba.get("name"),
                "owner_business_id": owner_id,
                "currency": currency,
                "account_review_status": waba.get("account_review_status"),
            },
        )

    def _assert_tenant_isolation(self, session: Session, connection: MetaBusinessConnection) -> None:
        """No WABA or phone number may be mapped to two PHANTA locations."""
        step = state.STEP_WABA_VERIFIED
        clash = session.scalar(
            select(MetaBusinessConnection).where(
                MetaBusinessConnection.waba_id == connection.waba_id,
                MetaBusinessConnection.location_id != connection.location_id,
            )
        )
        if clash is not None:
            raise OnboardingStepError(
                step,
                "This WhatsApp Business Account is already connected to a different workshop.",
                remediation="Disconnect it from the other workshop before connecting it here.",
            )
        clash = session.scalar(
            select(MetaBusinessConnection).where(
                MetaBusinessConnection.phone_number_id == connection.phone_number_id,
                MetaBusinessConnection.location_id != connection.location_id,
            )
        )
        if clash is not None:
            raise OnboardingStepError(
                step,
                "This WhatsApp business phone number is already connected to a different workshop.",
                remediation="Disconnect it from the other workshop before connecting it here.",
            )

    def _assert_token_targets_waba(self, step: str, token: str, waba_id: str) -> None:
        try:
            payload = self.client.debug_customer_token(token)
        except MetaGraphAPIError:
            # debug_token being unavailable must not block onboarding; the
            # WABA read below is still an authorization check.
            return
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return
        for scope in data.get("granular_scopes") or []:
            if not isinstance(scope, dict):
                continue
            if scope.get("scope") != "whatsapp_business_management":
                continue
            targets = scope.get("target_ids")
            if not targets:
                return  # Meta did not scope the grant to specific WABAs.
            if str(waba_id) not in {str(t) for t in targets}:
                raise OnboardingStepError(
                    step,
                    "The WhatsApp Business Account reported by the browser is not one this "
                    "Embedded Signup authorization covers.",
                    remediation="Ask the workshop to run Connect WhatsApp again.",
                )
            return

    # =====================================================================
    # STEP: System User assignment + verification
    # =====================================================================

    def _find_assigned_system_user(
        self, step: str, waba_id: str, system_user_id: str
    ) -> dict[str, Any] | None:
        try:
            assigned = self.operations.list_assigned_users(waba_id)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step, exc, "VANTA's assigned users on the client's WABA could not be read."
            ) from exc
        for entry in assigned:
            if isinstance(entry, dict) and str(entry.get("id")) == str(system_user_id):
                return entry
        return None

    def assign_system_user(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Assign VANTA's System User to the client's WABA.

        Idempotent: the existing assignment list is read first, and Meta
        reporting the user as already assigned is treated as success rather
        than as an error, so a retry after a later step failed never
        duplicates the assignment.
        """
        step = state.STEP_SYSTEM_USER_ASSIGNED
        waba_id = str(connection.waba_id)
        system_user_id = self._resolve_system_user_id(step)

        existing = self._find_assigned_system_user(step, waba_id, system_user_id)
        if existing is not None:
            connection.system_user_id = system_user_id
            connection.system_user_assigned_at = connection.system_user_assigned_at or self._now()
            self._advance(connection, step)
            session.flush()
            return StepOutcome(step, "already_done", {"system_user_id": system_user_id, "tasks": existing.get("tasks")})

        try:
            self.operations.assign_system_user(waba_id, system_user_id, self.provider.assigned_tasks)
        except MetaGraphAPIError as exc:
            if self._is_already_assigned(exc):
                connection.system_user_id = system_user_id
                connection.system_user_assigned_at = connection.system_user_assigned_at or self._now()
                self._advance(connection, step)
                session.flush()
                return StepOutcome(step, "already_done", {"system_user_id": system_user_id})
            raise self._graph_error(
                step,
                exc,
                "VANTA's System User could not be assigned to the client's WhatsApp Business Account.",
                remediation=(
                    "Confirm the System User has Admin access on VANTA's business portfolio and that "
                    "the client granted VANTA management of this WABA during Embedded Signup."
                ),
            ) from exc

        connection.system_user_id = system_user_id
        connection.system_user_assigned_at = self._now()
        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"system_user_id": system_user_id, "tasks": list(self.provider.assigned_tasks)})

    def verify_system_user_assignment(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Confirm at Meta that the assignment actually took effect."""
        step = state.STEP_SYSTEM_USER_VERIFIED
        waba_id = str(connection.waba_id)
        system_user_id = connection.system_user_id or self._resolve_system_user_id(step)

        entry = self._find_assigned_system_user(step, waba_id, system_user_id)
        if entry is None:
            raise OnboardingStepError(
                step,
                "Meta does not report VANTA's System User as assigned to the client's WhatsApp "
                "Business Account.",
                retryable=True,
                remediation="Retry onboarding; if it persists, check the WABA sharing in Meta Business Manager.",
            )
        connection.system_user_id = system_user_id
        connection.system_user_assigned_at = connection.system_user_assigned_at or self._now()
        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"system_user_id": system_user_id, "tasks": entry.get("tasks")})

    def _resolve_system_user_id(self, step: str) -> str:
        if self.provider.system_user_id:
            return self.provider.system_user_id
        try:
            identity = self.operations.get_system_user_identity()
        except MetaGraphAPIError as exc:
            raise self._graph_error(step, exc, "VANTA's System User identity could not be read from Meta.") from exc
        resolved = str(identity.get("id") or "")
        if not resolved:
            raise OnboardingStepError(step, "Meta did not return VANTA's System User ID.")
        return resolved

    @staticmethod
    def _is_already_assigned(exc: MetaGraphAPIError) -> bool:
        text = f"{exc} {exc.error.get('error_user_msg', '')}".lower()
        return "already" in text and ("assign" in text or "added" in text or "user" in text)

    # =====================================================================
    # STEP: phone registration
    # =====================================================================

    def register_phone(
        self, session: Session, connection: MetaBusinessConnection, *, pin: str | None
    ) -> StepOutcome:
        """Register the client's phone number for Cloud API.

        Reuses the existing :class:`PhoneNumberService`; this method adds only
        the idempotency probe and the sequencing. The PIN is a request-only
        secret: it is passed straight through and never persisted here.
        """
        step = state.STEP_PHONE_REGISTERED
        token = self._customer_token(connection, step)
        phone_id = str(connection.phone_number_id)

        try:
            phone = self.operations.get_phone_number(token, phone_id)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step, exc, "The client's WhatsApp business phone number could not be read.",
                customer_token=token,
            ) from exc

        if self._phone_already_registered(phone):
            connection.phone_registered_at = connection.phone_registered_at or self._now()
            self._apply_phone_fields(connection, phone)
            self._advance(connection, step)
            session.flush()
            return StepOutcome(
                step,
                "already_done",
                {"platform_type": phone.get("platform_type"), "status": phone.get("status")},
            )

        if not pin:
            raise PhonePinRequired(
                "This WhatsApp number is not registered for Cloud API yet and Meta requires a "
                "6-digit two-step verification PIN to register it."
            )

        try:
            self.phone_service.register(session, int(connection.location_id), pin)
        except PhoneRegistrationError as exc:
            raise OnboardingStepError(
                step,
                scrub(exc, secrets=self._secrets(token)),
                retryable=True,
                remediation="Check the 6-digit PIN and try again.",
            ) from exc
        except MetaGraphAPIError as exc:
            if self._is_already_registered(exc):
                connection.phone_registered_at = connection.phone_registered_at or self._now()
                self._advance(connection, step)
                session.flush()
                return StepOutcome(step, "already_done", {"reason": "meta_reported_already_registered"})
            raise self._graph_error(
                step, exc, "The client's WhatsApp phone number could not be registered.",
                customer_token=token,
                remediation="Confirm the number is not registered elsewhere, then retry with a 6-digit PIN.",
            ) from exc

        connection.phone_registered_at = self._now()
        self._apply_phone_fields(connection, phone)
        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"phone_number_id": phone_id})

    @staticmethod
    def _phone_already_registered(phone: dict[str, Any]) -> bool:
        platform = str(phone.get("platform_type") or "").upper()
        status = str(phone.get("status") or "").upper()
        return platform == "CLOUD_API" or status == "CONNECTED"

    @staticmethod
    def _apply_phone_fields(connection: MetaBusinessConnection, phone: dict[str, Any]) -> None:
        if phone.get("display_phone_number"):
            connection.display_phone_number = str(phone["display_phone_number"])
        if phone.get("verified_name"):
            connection.verified_name = str(phone["verified_name"])
        if phone.get("quality_rating"):
            connection.quality_rating = str(phone["quality_rating"])

    @staticmethod
    def _is_already_registered(exc: MetaGraphAPIError) -> bool:
        code = exc.error.get("code")
        if code in (133005, 133006, 133015):
            return True
        return "already registered" in str(exc).lower()

    # =====================================================================
    # STEP: credit line
    # =====================================================================

    def retrieve_credit_line(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Find and validate VANTA's Meta extended credit line.

        A configured META_CREDIT_LINE_ID is not trusted blindly: it must
        appear in VANTA's own business portfolio's extendedcredits list.
        """
        step = state.STEP_CREDIT_LINE_RETRIEVED
        if not self.provider.credit_sharing_enabled:
            self._advance(connection, step)
            session.flush()
            return StepOutcome(step, "skipped", {"reason": "META_CREDIT_SHARING_ENABLED is false"})

        try:
            credits = self.operations.list_extended_credits(self.provider.business_id)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step,
                exc,
                "VANTA's Meta credit line could not be read from the business portfolio.",
                remediation=(
                    "Confirm META_BUSINESS_ID is VANTA's business portfolio ID and that the System "
                    "User holds Admin or Financial Editor on it with the business_management permission."
                ),
            ) from exc

        available = [str(item.get("id")) for item in credits if isinstance(item, dict) and item.get("id")]
        if not available:
            raise OnboardingStepError(
                step,
                "VANTA's Meta business portfolio has no extended credit line, so the client's "
                "WhatsApp usage cannot be billed to VANTA.",
                remediation=(
                    "Apply for a Meta line of credit for VANTA's business portfolio (Meta Business "
                    "Suite > Business settings > Billing & payments), or set "
                    "META_CREDIT_SHARING_ENABLED=false to onboard clients who pay Meta directly."
                ),
            )

        configured = self.provider.credit_line_id
        if configured and configured not in available:
            raise OnboardingStepError(
                step,
                "META_CREDIT_LINE_ID is not a credit line of the configured VANTA business portfolio.",
                remediation="Correct META_CREDIT_LINE_ID, or clear it so it is discovered automatically.",
            )

        credit_line_id = configured or available[0]
        connection.credit_line_id = credit_line_id
        self._advance(connection, step)
        session.flush()
        return StepOutcome(
            step,
            "performed",
            {"credit_line_id": credit_line_id, "available_count": len(available)},
        )

    def share_credit_line(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Attach VANTA's credit line to the client's WABA.

        This is what implements "clients pay VANTA, VANTA pays Meta": VANTA
        becomes the Bill To Party for this WABA's WhatsApp spend.

        Idempotent in two layers: a persisted allocation config ID short-
        circuits immediately, and otherwise Meta is asked whether a sharing
        record for this client business already exists before a new share is
        attempted. Meta does not allow a WABA's credit line to be changed
        once attached, so a duplicate attempt must be avoided rather than
        merely tolerated.
        """
        step = state.STEP_CREDIT_SHARED
        if not self.provider.credit_sharing_enabled:
            self._advance(connection, step)
            session.flush()
            return StepOutcome(step, "skipped", {"reason": "META_CREDIT_SHARING_ENABLED is false"})

        if connection.credit_allocation_config_id:
            self._advance(connection, step)
            session.flush()
            return StepOutcome(
                step, "already_done", {"allocation_config_id": connection.credit_allocation_config_id}
            )

        credit_line_id = connection.credit_line_id
        if not credit_line_id:
            raise OnboardingStepError(step, "No VANTA credit line has been resolved yet.", retryable=True)

        # Recover an existing share before creating one.
        if connection.owner_business_id:
            try:
                configs = self.operations.find_allocation_configs(
                    credit_line_id, connection.owner_business_id
                )
            except MetaGraphAPIError:
                configs = []
            for config in configs:
                if isinstance(config, dict) and config.get("id"):
                    connection.credit_allocation_config_id = str(config["id"])
                    connection.credit_shared_at = connection.credit_shared_at or self._now()
                    self._advance(connection, step)
                    session.flush()
                    return StepOutcome(
                        step, "already_done", {"allocation_config_id": connection.credit_allocation_config_id}
                    )

        currency = self._resolve_waba_currency(step, connection)

        try:
            payload = self.operations.share_credit_line(credit_line_id, str(connection.waba_id), currency)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step,
                exc,
                "VANTA's credit line could not be shared with the client's WhatsApp Business Account.",
                remediation=(
                    "Confirm VANTA's Meta business portfolio is approved for credit sharing and that "
                    "the System User has MANAGE_BILLING (or MANAGE) on the client's WABA."
                ),
            ) from exc

        allocation_id = payload.get("allocation_config_id")
        if not allocation_id:
            raise OnboardingStepError(
                step,
                "Meta did not return an allocation configuration ID for the credit line share.",
                retryable=True,
            )
        connection.credit_allocation_config_id = str(allocation_id)
        connection.credit_shared_at = self._now()
        self._advance(connection, step)
        session.flush()
        return StepOutcome(
            step,
            "performed",
            {"allocation_config_id": str(allocation_id), "waba_currency": currency},
        )

    def _resolve_waba_currency(self, step: str, connection: MetaBusinessConnection) -> str:
        """Use the client WABA's real currency, validated against Meta's list.

        Deliberately no default: guessing a currency here would silently bill
        a workshop in the wrong one. South African deployments should note
        that Meta does not accept ZAR for credit sharing.
        """
        currency = (connection.waba_currency or "").upper()
        if not currency:
            raise OnboardingStepError(
                step,
                "The client's WhatsApp Business Account currency is unknown, so VANTA's credit line "
                "cannot be attached.",
                retryable=True,
                remediation="Re-run WABA verification so the currency is read from Meta.",
            )
        if currency not in SUPPORTED_WABA_CURRENCIES:
            raise OnboardingStepError(
                step,
                f"Meta does not accept {currency} for credit line sharing. Supported currencies are "
                + ", ".join(SUPPORTED_WABA_CURRENCIES)
                + ".",
                remediation=(
                    "The client's WhatsApp Business Account must use a Meta-supported billing "
                    "currency. Set it in Meta Business Manager before onboarding, or set "
                    "META_CREDIT_SHARING_ENABLED=false so the client pays Meta directly."
                ),
            )
        return currency

    def verify_credit_allocation(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Meta's documented credit-share verification.

        Compares the allocation config's ``receiving_credential.id`` (read
        with VANTA's System User token) against the client WABA's
        ``primary_funding_id`` (read with the client's own token). Matching
        IDs are the proof that the credit line is actually funding this WABA.
        """
        step = state.STEP_CREDIT_VERIFIED
        if not self.provider.credit_sharing_enabled:
            self._advance(connection, step)
            session.flush()
            return StepOutcome(step, "skipped", {"reason": "META_CREDIT_SHARING_ENABLED is false"})

        allocation_id = connection.credit_allocation_config_id
        if not allocation_id:
            raise OnboardingStepError(step, "No credit allocation configuration has been recorded.", retryable=True)

        try:
            config = self.operations.get_allocation_config(allocation_id)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step, exc, "The credit allocation configuration could not be read from Meta."
            ) from exc
        receiving = (config.get("receiving_credential") or {}) if isinstance(config, dict) else {}
        receiving_id = str(receiving.get("id")) if receiving.get("id") else None

        token = self._customer_token(connection, step)
        try:
            funding_id = self.operations.get_waba_primary_funding_id(token, str(connection.waba_id))
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step, exc, "The client WABA's primary funding source could not be read.",
                customer_token=token,
            ) from exc

        if not receiving_id or not funding_id or receiving_id != funding_id:
            raise OnboardingStepError(
                step,
                "VANTA's credit line is not confirmed as the client WhatsApp Business Account's "
                "funding source.",
                retryable=True,
                remediation=(
                    "Meta can take a short while to propagate a credit share. Retry onboarding; if it "
                    "keeps failing, check the WABA's payment settings in Meta Business Manager."
                ),
            )

        connection.credit_verified_at = self._now()
        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"allocation_config_id": allocation_id})

    # =====================================================================
    # STEP: WABA webhook subscription
    # =====================================================================

    def subscribe_app_to_waba(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Subscribe the VANTA Meta app to webhooks on the client's WABA.

        This is the per-WABA subscription only. PHANTA's webhook receiver,
        signature verification, deduplication and tenant routing are existing
        systems and are untouched.
        """
        step = state.STEP_WEBHOOK_SUBSCRIBED
        token = self._customer_token(connection, step)
        waba_id = str(connection.waba_id)

        if self._app_is_subscribed(step, token, waba_id):
            connection.webhook_subscribed_at = connection.webhook_subscribed_at or self._now()
            self._advance(connection, step)
            session.flush()
            return StepOutcome(step, "already_done", {"app_id": self.config.app_id})

        try:
            self.operations.subscribe_app_to_waba(token, waba_id)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step,
                exc,
                "The VANTA app could not be subscribed to webhooks on the client's WhatsApp "
                "Business Account.",
                customer_token=token,
                remediation="Confirm the app has whatsapp_business_management on this WABA, then retry.",
            ) from exc

        connection.webhook_subscribed_at = self._now()
        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"app_id": self.config.app_id})

    def verify_app_subscription(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Confirm at Meta that the VANTA app is subscribed to this WABA.

        Without this, the client's inbound messages would never reach PHANTA
        and the workshop would see a silent, invisible failure.
        """
        step = state.STEP_WEBHOOK_VERIFIED
        token = self._customer_token(connection, step)
        if not self._app_is_subscribed(step, token, str(connection.waba_id)):
            raise OnboardingStepError(
                step,
                "Meta does not report the VANTA app as subscribed to the client's WhatsApp "
                "Business Account, so inbound messages would not be delivered.",
                retryable=True,
            )
        connection.webhook_verified_at = self._now()
        connection.webhook_subscribed_at = connection.webhook_subscribed_at or self._now()
        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"app_id": self.config.app_id})

    def _app_is_subscribed(self, step: str, token: str, waba_id: str) -> bool:
        try:
            apps = self.operations.list_subscribed_apps(token, waba_id)
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step, exc, "The client WABA's webhook subscriptions could not be read.",
                customer_token=token,
            ) from exc
        app_id = str(self.config.app_id)
        for entry in apps:
            if not isinstance(entry, dict):
                continue
            data = entry.get("whatsapp_business_api_data") or {}
            if str(data.get("id")) == app_id or str(entry.get("id")) == app_id:
                return True
        return False

    # =====================================================================
    # STEP: template synchronisation
    # =====================================================================

    def sync_message_templates(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Mirror the client's existing templates into PHANTA's storage."""
        step = state.STEP_TEMPLATES_SYNCED
        token = self._customer_token(connection, step)
        try:
            result = self.template_sync.sync(
                session,
                location_id=int(connection.location_id),
                waba_id=str(connection.waba_id),
                customer_token=token,
            )
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step, exc, "The client's WhatsApp message templates could not be synchronised.",
                customer_token=token,
            ) from exc
        connection.templates_synced_at = self._now()
        self._advance(connection, step)
        session.flush()
        return StepOutcome(
            step,
            "performed",
            {
                "synced": result.synced,
                "skipped": result.skipped,
                # A brand-new WABA legitimately has no templates yet. Record
                # the reason so final verification does not read zero as a
                # failed sync.
                "reason": None if result.synced else "waba_has_no_templates_yet",
            },
        )

    # =====================================================================
    # STEP: final verification + finalisation
    # =====================================================================

    def verify_final_connection(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Last gate before the connection may be called connected."""
        step = state.STEP_FINAL_VERIFICATION
        problems: list[str] = []

        if not connection.waba_id:
            problems.append("no WhatsApp Business Account ID")
        if not connection.phone_number_id:
            problems.append("no business phone number ID")
        if not connection.encrypted_access_token:
            problems.append("no stored client access token")
        if not connection.system_user_id or not connection.system_user_assigned_at:
            problems.append("VANTA System User access is not established")
        if not connection.phone_registered_at:
            problems.append("phone registration is not confirmed")
        if not connection.webhook_verified_at:
            problems.append("webhook subscription is not verified")
        if connection.templates_synced_at is None:
            problems.append("template synchronisation has not run")
        if self.provider.credit_sharing_enabled and not connection.credit_verified_at:
            problems.append("VANTA credit line sharing is not verified")

        # Tenant/location mapping must still hold at the end of the run.
        try:
            self._assert_tenant_isolation(session, connection)
        except OnboardingStepError as exc:
            problems.append(str(exc))

        # The stored token must still be usable, or the connection would be
        # reported connected while every message send fails.
        token = self._customer_token(connection, step)
        try:
            payload = self.client.debug_customer_token(token)
            if not (payload.get("data") or {}).get("is_valid"):
                problems.append("the stored client access token is no longer valid")
        except MetaGraphAPIError as exc:
            raise self._graph_error(
                step, exc, "The client's stored token could not be validated.", customer_token=token
            ) from exc

        if problems:
            raise OnboardingStepError(
                step,
                "Final verification failed: " + "; ".join(problems) + ".",
                retryable=True,
            )

        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"checks_passed": True})

    def finalize_connection(self, session: Session, connection: MetaBusinessConnection) -> StepOutcome:
        """Mark the connection connected. The only place this may happen."""
        step = state.STEP_COMPLETED
        now = self._now()
        connection.connection_status = state.STATUS_CONNECTED
        connection.connected_at = connection.connected_at or now
        connection.disconnected_at = None
        connection.last_onboarding_error = None
        self._advance(connection, step)
        session.flush()
        return StepOutcome(step, "performed", {"connected_at": connection.connected_at.isoformat()})

    # =====================================================================
    # Orchestration
    # =====================================================================

    def _plan(self) -> list[tuple[str, Callable[[Session, MetaBusinessConnection], StepOutcome] | None]]:
        """The documented step order.

        The phone step's callable is None because it needs the run's PIN,
        which is request-only: run_onboarding dispatches it explicitly rather
        than letting a PIN become state on a shared service instance.
        """
        return [
            (state.STEP_WABA_VERIFIED, self.verify_waba),
            (state.STEP_SYSTEM_USER_ASSIGNED, self.assign_system_user),
            (state.STEP_SYSTEM_USER_VERIFIED, self.verify_system_user_assignment),
            (state.STEP_PHONE_REGISTERED, None),
            (state.STEP_CREDIT_LINE_RETRIEVED, self.retrieve_credit_line),
            (state.STEP_CREDIT_SHARED, self.share_credit_line),
            (state.STEP_CREDIT_VERIFIED, self.verify_credit_allocation),
            (state.STEP_WEBHOOK_SUBSCRIBED, self.subscribe_app_to_waba),
            (state.STEP_WEBHOOK_VERIFIED, self.verify_app_subscription),
            (state.STEP_TEMPLATES_SYNCED, self.sync_message_templates),
            (state.STEP_FINAL_VERIFICATION, self.verify_final_connection),
            (state.STEP_COMPLETED, self.finalize_connection),
        ]

    def run_onboarding(
        self,
        session: Session,
        location_id: int,
        *,
        phone_pin: str | None = None,
    ) -> OnboardingRunResult:
        """Run every remaining onboarding step in the documented order.

        Steps already recorded as successful are skipped, so this doubles as
        the resume entry point. Progress is flushed after each step, and the
        caller commits, so a failure part-way leaves durable evidence of
        everything that did succeed.
        """
        connection = self._connection(session, location_id)
        result = OnboardingRunResult(
            location_id=int(location_id),
            connection_id=connection.id,
            connection_status=connection.connection_status,
            onboarding_step=connection.onboarding_step,
            last_successful_step=connection.last_successful_onboarding_step,
            completed=False,
        )

        # A connection that predates the orchestrator stays exactly as it is.
        if state.is_legacy(connection.onboarding_step):
            result.completed = True
            result.outcomes.append(
                StepOutcome("legacy_connection", "skipped", {"reason": "connected before Tech Provider onboarding existed"})
            )
            return result

        connection.last_onboarding_attempt_at = self._now()
        if connection.connection_status not in (state.STATUS_CONNECTED, state.STATUS_EXPIRING_SOON):
            connection.connection_status = state.STATUS_ONBOARDING
        session.flush()

        try:
            provider_outcome = self.verify_provider_configuration()
            result.outcomes.append(provider_outcome)
        except OnboardingStepError as exc:
            return self._record_failure(session, connection, result, exc)

        for target_step, operation in self._plan():
            if state.step_reached(connection.last_successful_onboarding_step, target_step):
                result.outcomes.append(StepOutcome(target_step, "already_done", {"reason": "recorded in a previous run"}))
                continue
            try:
                if operation is None:  # the phone step; see _plan()
                    outcome = self.register_phone(session, connection, pin=phone_pin)
                else:
                    outcome = operation(session, connection)
            except OnboardingStepError as exc:
                return self._record_failure(session, connection, result, exc)
            except MetaGraphAPIError as exc:
                wrapped = self._graph_error(target_step, exc, "A Meta API call failed.")
                return self._record_failure(session, connection, result, wrapped)
            result.outcomes.append(outcome)

        connection.last_onboarding_error = None
        session.flush()
        result.completed = True
        result.connection_status = connection.connection_status
        result.onboarding_step = connection.onboarding_step
        result.last_successful_step = connection.last_successful_onboarding_step
        return result

    def resume_onboarding(
        self, session: Session, location_id: int, *, phone_pin: str | None = None
    ) -> OnboardingRunResult:
        """Continue a partially completed onboarding from where it stopped."""
        return self.run_onboarding(session, location_id, phone_pin=phone_pin)

    def _record_failure(
        self,
        session: Session,
        connection: MetaBusinessConnection,
        result: OnboardingRunResult,
        exc: OnboardingStepError,
    ) -> OnboardingRunResult:
        awaiting = isinstance(exc, PhonePinRequired)
        message = scrub(exc, secrets=self._secrets())

        connection.last_onboarding_error = message
        connection.onboarding_step = (
            state.STEP_AWAITING_PHONE_PIN if awaiting else exc.step
        )
        # A partial onboarding must never look connected. An already-connected
        # legacy/reconnect case is left alone; anything else is honest about
        # being incomplete.
        if connection.connection_status not in (state.STATUS_CONNECTED, state.STATUS_EXPIRING_SOON):
            connection.connection_status = (
                state.STATUS_ONBOARDING if (awaiting or exc.retryable) else state.STATUS_FAILED
            )
        session.flush()

        result.failed_step = exc.step
        result.awaiting_input = awaiting
        result.error = dict(exc.as_dict(), message=message)
        result.completed = False
        result.connection_status = connection.connection_status
        result.onboarding_step = connection.onboarding_step
        result.last_successful_step = connection.last_successful_onboarding_step
        return result

    # =====================================================================
    # Status reporting
    # =====================================================================

    def onboarding_status(self, session: Session, location_id: int) -> dict[str, Any]:
        """Client-safe onboarding progress. Contains no provider detail."""
        connection = self.connection_repo.get_for_location(session, location_id)
        if connection is None:
            return {
                "location_id": int(location_id),
                "connected": False,
                "connection_status": "not_connected",
                "onboarding_step": None,
                "label": "Not connected",
                "progress_percent": 0,
                "awaiting_phone_pin": False,
                "error": None,
            }
        step = connection.onboarding_step
        connected = connection.connection_status in state.USABLE_STATUSES and (
            state.is_complete(step) or state.is_legacy(step)
        )
        return {
            "location_id": int(location_id),
            "connected": connected,
            "connection_status": connection.connection_status,
            "onboarding_step": step,
            "label": "Connected" if connected else state.step_label(step),
            "progress_percent": state.progress_percent(step),
            "awaiting_phone_pin": step == state.STEP_AWAITING_PHONE_PIN,
            "error": connection.last_onboarding_error,
        }

    def diagnostics(self, session: Session, location_id: int) -> dict[str, Any]:
        """Admin diagnostics. Identifiers and timestamps only, never tokens."""
        connection = self.connection_repo.get_for_location(session, location_id)
        payload: dict[str, Any] = {
            "provider": self.provider.safe_summary(),
            "app_id": self.config.app_id,
            "graph_api_version": self.config.graph_api_version,
            "connection": None,
        }
        if connection is None:
            return payload
        payload["connection"] = {
            "id": connection.id,
            "location_id": connection.location_id,
            "connection_status": connection.connection_status,
            "onboarding_step": connection.onboarding_step,
            "last_successful_onboarding_step": connection.last_successful_onboarding_step,
            "last_onboarding_attempt_at": _iso(connection.last_onboarding_attempt_at),
            "last_onboarding_error": connection.last_onboarding_error,
            "waba_id": connection.waba_id,
            "phone_number_id": connection.phone_number_id,
            "owner_business_id": connection.owner_business_id,
            "waba_currency": connection.waba_currency,
            "system_user_id": connection.system_user_id,
            "system_user_assigned_at": _iso(connection.system_user_assigned_at),
            "phone_registered_at": _iso(connection.phone_registered_at),
            "credit_line_id": connection.credit_line_id,
            "credit_allocation_config_id": connection.credit_allocation_config_id,
            "credit_shared_at": _iso(connection.credit_shared_at),
            "credit_verified_at": _iso(connection.credit_verified_at),
            "webhook_subscribed_at": _iso(connection.webhook_subscribed_at),
            "webhook_verified_at": _iso(connection.webhook_verified_at),
            "templates_synced_at": _iso(connection.templates_synced_at),
            "connected_at": _iso(connection.connected_at),
            "token_stored": bool(connection.encrypted_access_token),
        }
        return payload


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None

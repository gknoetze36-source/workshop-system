"""Persistence models for PHANTA's external integrations.

Google Calendar models are intentionally absent: PHANTA v1 does not use
Google Calendar. Customer review URLs are stored on Location in models/core.py;
they require no Google/HelloPeter API integration.

GoogleBusinessConnection (below) is a different Google product --
Google Business Profile posting (Local Posts), not Calendar -- and is
not affected by the Calendar decision above.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, JSON, String, Text, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .core import Base


class MetaBusinessConnection(Base):
    __tablename__ = "meta_business_connections"
    __table_args__ = (
        UniqueConstraint("location_id", name="uq_meta_connection_location"),
        UniqueConstraint("waba_id", name="uq_meta_connection_waba_id"),
        UniqueConstraint("phone_number_id", name="uq_meta_connection_phone_number_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    business_id: Mapped[Optional[str]] = mapped_column(String(100))
    waba_id: Mapped[Optional[str]] = mapped_column(String(100))
    phone_number_id: Mapped[Optional[str]] = mapped_column(String(100))
    display_phone_number: Mapped[Optional[str]] = mapped_column(String(50))
    verified_name: Mapped[Optional[str]] = mapped_column(String(255))
    token_type: Mapped[str] = mapped_column(String(80), default="business_integration_system_user", nullable=False)
    quality_rating: Mapped[Optional[str]] = mapped_column(String(20))
    messaging_tier: Mapped[Optional[str]] = mapped_column(String(50))
    disconnected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    connection_status: Mapped[str] = mapped_column(String(40), default="pending", nullable=False)
    token_secret_ref: Mapped[Optional[str]] = mapped_column(String(255))
    encrypted_access_token: Mapped[Optional[str]] = mapped_column(Text)
    token_key_version: Mapped[Optional[str]] = mapped_column(String(20))
    token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    connected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_health_check_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # --- Meta Tech Provider onboarding state -------------------------------
    # connection_status is the lifecycle (pending/onboarding/connected/...).
    # onboarding_step is the fine-grained progress marker so PHANTA always
    # knows exactly where a partial onboarding stopped and can resume there.
    onboarding_step: Mapped[Optional[str]] = mapped_column(String(40))
    last_successful_onboarding_step: Mapped[Optional[str]] = mapped_column(String(40))
    last_onboarding_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_onboarding_error: Mapped[Optional[str]] = mapped_column(Text)

    # Identifiers produced by the onboarding run. None of these are secrets;
    # tokens live only in encrypted_access_token / environment configuration.
    owner_business_id: Mapped[Optional[str]] = mapped_column(String(100))
    waba_currency: Mapped[Optional[str]] = mapped_column(String(10))
    system_user_id: Mapped[Optional[str]] = mapped_column(String(100))
    system_user_assigned_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    phone_registered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    credit_line_id: Mapped[Optional[str]] = mapped_column(String(100))
    credit_allocation_config_id: Mapped[Optional[str]] = mapped_column(String(100))
    credit_shared_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    credit_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    webhook_subscribed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    webhook_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    templates_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class MetaSignupSession(Base):
    """Short-lived server-side state for one Embedded Signup launch."""
    __tablename__ = "meta_signup_sessions"
    __table_args__ = (
        UniqueConstraint("state_nonce", name="uq_meta_signup_state_nonce"),
        Index("ix_meta_signup_location_status", "location_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    state_nonce: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="started", nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    business_id: Mapped[Optional[str]] = mapped_column(String(100))
    waba_id: Mapped[Optional[str]] = mapped_column(String(100))
    phone_number_id: Mapped[Optional[str]] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaBusinessVerificationStatus(Base):
    __tablename__ = "meta_business_verification_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    connection_id: Mapped[int] = mapped_column(ForeignKey("meta_business_connections.id", ondelete="CASCADE"), nullable=False)
    business_verification_status: Mapped[Optional[str]] = mapped_column(String(40))
    display_name_status: Mapped[Optional[str]] = mapped_column(String(40))
    phone_verification_status: Mapped[Optional[str]] = mapped_column(String(40))
    last_checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaPermissionGrant(Base):
    __tablename__ = "meta_permissions_grants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    connection_id: Mapped[int] = mapped_column(ForeignKey("meta_business_connections.id", ondelete="CASCADE"), nullable=False)
    permission: Mapped[str] = mapped_column(String(100), nullable=False)
    access_level: Mapped[Optional[str]] = mapped_column(String(50))
    granted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaAppReviewStatus(Base):
    __tablename__ = "meta_app_review_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    permission: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    submitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaWebhookEvent(Base):
    __tablename__ = "meta_webhook_events"
    __table_args__ = (
        Index("ix_meta_webhook_external_id", "external_event_id"),
        Index("ix_meta_webhook_received", "received_at"),
        UniqueConstraint("external_event_id", name="uq_meta_webhook_external_event_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("locations.id", ondelete="SET NULL"))
    waba_id: Mapped[Optional[str]] = mapped_column(String(100))
    phone_number_id: Mapped[Optional[str]] = mapped_column(String(100))
    external_event_id: Mapped[Optional[str]] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    signature_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    processing_status: Mapped[str] = mapped_column(String(40), default="received", nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class MetaAuditLog(Base):
    __tablename__ = "meta_audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("locations.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    details: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class PaymentCustomer(Base):
    __tablename__ = "payment_customers"
    __table_args__ = (Index("ix_payment_customer_location", "location_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    phanta_customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"))
    paystack_customer_code: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(320))
    authorization_secret_ref: Mapped[Optional[str]] = mapped_column(Text)


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        Index("ix_payments_location_status", "location_id", "status"),
        Index("ix_payments_reference", "reference"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    reference: Mapped[str] = mapped_column(String(150), unique=True, nullable=False)
    paystack_transaction_id: Mapped[Optional[str]] = mapped_column(String(100))
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="ZAR", nullable=False)
    status: Mapped[str] = mapped_column(String(40), default="initialized", nullable=False)
    gateway_response: Mapped[Optional[str]] = mapped_column(Text)
    channel: Mapped[Optional[str]] = mapped_column(String(50))
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    metadata_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class Plan(Base):
    __tablename__ = "plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    paystack_plan_code: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    interval: Mapped[str] = mapped_column(String(30), nullable=False)
    invoice_limit: Mapped[Optional[int]] = mapped_column(Integer)


class Subscription(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (Index("ix_subscriptions_location_status", "location_id", "status"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    paystack_subscription_code: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    paystack_email_token: Mapped[Optional[str]] = mapped_column(String(255))
    plan_code: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    current_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id", ondelete="SET NULL"))


class Invoice(Base):
    __tablename__ = "invoices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    subscription_id: Mapped[int] = mapped_column(ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False)
    paystack_invoice_id: Mapped[Optional[str]] = mapped_column(String(100))
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    period_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    failure_reason: Mapped[Optional[str]] = mapped_column(Text)


class Refund(Base):
    __tablename__ = "refunds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    payment_id: Mapped[int] = mapped_column(ForeignKey("payments.id", ondelete="CASCADE"), nullable=False)
    paystack_refund_id: Mapped[Optional[str]] = mapped_column(String(100))
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class PaystackWebhookEvent(Base):
    __tablename__ = "paystack_webhook_events"
    __table_args__ = (
        Index("ix_paystack_webhook_event_key", "event_key"),
        Index("ix_paystack_webhook_received", "received_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("locations.id", ondelete="SET NULL"))
    event_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    signature_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    processing_status: Mapped[str] = mapped_column(String(40), default="received", nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class AIUsageLog(Base):
    __tablename__ = "ai_usage_log"
    __table_args__ = (
        Index("ix_ai_usage_location_created", "location_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("locations.id", ondelete="SET NULL"))
    conversation_id: Mapped[Optional[int]] = mapped_column(ForeignKey("conversations.id", ondelete="SET NULL"))
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    model: Mapped[str] = mapped_column(String(150), nullable=False)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    request_id: Mapped[Optional[str]] = mapped_column(String(255))
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    estimated_cost: Mapped[Optional[float]] = mapped_column(Numeric(12, 6))
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class PromptVersion(Base):
    __tablename__ = "prompt_versions"
    __table_args__ = (Index("ix_prompt_versions_key_active", "prompt_key", "active"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prompt_key: Mapped[str] = mapped_column(String(150), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaMessageTemplate(Base):
    """PHANTA's durable view of a WhatsApp message template.

    Meta remains the source of truth for template review. This table lets PHANTA
    select only known/safe templates and mirror review state from webhooks.
    """
    __tablename__ = "meta_message_templates"
    __table_args__ = (
        UniqueConstraint("location_id", "meta_template_id", name="uq_meta_template_location_id"),
        UniqueConstraint("location_id", "name", "language", name="uq_meta_template_location_name_language"),
        Index("ix_meta_templates_location_status", "location_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    waba_id: Mapped[Optional[str]] = mapped_column(String(100))
    meta_template_id: Mapped[Optional[str]] = mapped_column(String(100))
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    language: Mapped[str] = mapped_column(String(50), nullable=False)
    category: Mapped[str] = mapped_column(String(40), default="UTILITY", nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="PENDING", nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(Text)
    components_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaMessageAttempt(Base):
    """Outbound attempt audit trail, separate from the customer-visible message."""
    __tablename__ = "meta_message_attempts"
    __table_args__ = (
        Index("ix_meta_message_attempts_message", "message_id", "created_at"),
        Index("ix_meta_message_attempts_location_status", "location_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.id", ondelete="CASCADE"), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    http_status: Mapped[Optional[int]] = mapped_column(Integer)
    meta_error_code: Mapped[Optional[str]] = mapped_column(String(80))
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    response_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    retryable: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaSocialOAuthSession(Base):
    """Short-lived server-side state for Flyer Lady Meta Page connection."""
    __tablename__ = "meta_social_oauth_sessions"
    __table_args__ = (UniqueConstraint("state_nonce", name="uq_meta_social_oauth_state_nonce"), Index("ix_meta_social_oauth_location_status", "location_id", "status"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    state_nonce: Mapped[str] = mapped_column(String(128), nullable=False)
    encrypted_user_access_token: Mapped[str] = mapped_column(Text, nullable=False)
    meta_user_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    redirect_uri: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="started", nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class MetaSocialConnection(Base):
    """Location-scoped Facebook Page / Instagram connection for Flyer Lady."""
    __tablename__ = "meta_social_connections"
    __table_args__ = (UniqueConstraint("location_id", name="uq_meta_social_connection_location"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    # Meta app-scoped user ID from the Flyer Lady OAuth /me response. This is
    # the stable identity used to resolve Meta User Data Deletion callbacks.
    meta_user_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    page_id: Mapped[str] = mapped_column(String(100), nullable=False)
    page_name: Mapped[Optional[str]] = mapped_column(String(255))
    instagram_business_account_id: Mapped[Optional[str]] = mapped_column(String(100))
    instagram_username: Mapped[Optional[str]] = mapped_column(String(255))
    encrypted_page_access_token: Mapped[str] = mapped_column(Text, nullable=False)
    token_key_version: Mapped[str] = mapped_column(String(20), default="v1", nullable=False)
    token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    permissions_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    connection_status: Mapped[str] = mapped_column(String(40), default="connected", nullable=False)
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    last_health_check_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class GoogleBusinessOAuthSession(Base):
    """Short-lived server-side state for the Google Business Profile
    connect flow, holding the refresh token between the OAuth callback
    and the account/location picker being submitted.

    Mirrors MetaSocialOAuthSession exactly, for the identical reason: the
    refresh token must never sit in the browser-held Flask session
    (signed, not encrypted -- readable by anyone with the cookie) for
    however long the user takes to pick a listing. Only this row's
    opaque integer id travels through the browser, as a hidden form
    field on the picker page, the same way Flyer Lady's oauth_session_id
    already does."""
    __tablename__ = "google_business_oauth_sessions"
    __table_args__ = (UniqueConstraint("state_nonce", name="uq_google_business_oauth_state_nonce"), Index("ix_google_business_oauth_location_status", "location_id", "status"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    state_nonce: Mapped[str] = mapped_column(String(128), nullable=False)
    encrypted_refresh_token: Mapped[str] = mapped_column(Text, nullable=False)
    # The exact account/location options Google returned to THIS OAuth
    # grant during the callback -- not a permission list, an allow-list.
    # /connect/complete must validate the submitted account_id +
    # google_location_id against this before trusting them; without it,
    # those two fields arrive from the browser with no proof they were
    # ever actually returned by Google for this authenticated flow.
    options_json: Mapped[Optional[list]] = mapped_column(JSON)
    redirect_uri: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="started", nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class GoogleBusinessConnection(Base):
    """Location-scoped Google Business Profile connection, for posting
    Local Posts (see flyer_lady/platforms/google_business_publisher.py).
    Mirrors MetaSocialConnection's shape -- same one-connection-per-
    location pattern, same encrypted-token-at-rest approach."""
    __tablename__ = "google_business_connections"
    __table_args__ = (UniqueConstraint("location_id", name="uq_google_business_connection_location"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    google_account_id: Mapped[str] = mapped_column(String(100), nullable=False)
    google_location_id: Mapped[str] = mapped_column(String(100), nullable=False)
    business_name: Mapped[Optional[str]] = mapped_column(String(255))
    encrypted_refresh_token: Mapped[str] = mapped_column(Text, nullable=False)
    token_key_version: Mapped[str] = mapped_column(String(20), default="v1", nullable=False)
    connection_status: Mapped[str] = mapped_column(String(40), default="connected", nullable=False)
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    last_health_check_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class XOAuthSession(Base):
    """Short-lived server-side state for the X (Twitter) connect flow.

    Mirrors GoogleBusinessOAuthSession exactly, with one addition X's
    flow specifically requires: the PKCE code_verifier. X's OAuth 2.0
    Authorization Code flow mandates PKCE (unlike Meta/Google's plain
    authorization-code exchange) -- the verifier generated when building
    authorize_url() must be presented again at token exchange, so it has
    to survive the browser round-trip the same way the refresh token
    does: server-side here, never in the browser-held Flask session.
    """
    __tablename__ = "x_oauth_sessions"
    __table_args__ = (UniqueConstraint("state_nonce", name="uq_x_oauth_state_nonce"), Index("ix_x_oauth_location_status", "location_id", "status"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    state_nonce: Mapped[str] = mapped_column(String(128), nullable=False)
    code_verifier: Mapped[str] = mapped_column(String(128), nullable=False)
    encrypted_refresh_token: Mapped[Optional[str]] = mapped_column(Text)
    redirect_uri: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="started", nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class XConnection(Base):
    """Location-scoped X (Twitter) connection, for posting to
    flyer_lady/platforms/x_publisher.py. Mirrors MetaSocialConnection
    and GoogleBusinessConnection's shape -- same one-connection-per-
    location pattern, same encrypted-token-at-rest approach."""
    __tablename__ = "x_connections"
    __table_args__ = (UniqueConstraint("location_id", name="uq_x_connection_location"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    x_user_id: Mapped[str] = mapped_column(String(100), nullable=False)
    x_username: Mapped[Optional[str]] = mapped_column(String(100))
    encrypted_access_token: Mapped[str] = mapped_column(Text, nullable=False)
    encrypted_refresh_token: Mapped[Optional[str]] = mapped_column(Text)
    token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    token_key_version: Mapped[str] = mapped_column(String(20), default="v1", nullable=False)
    connection_status: Mapped[str] = mapped_column(String(40), default="connected", nullable=False)
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    last_health_check_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class XUsageCounter(Base):
    """Atomic monthly post counters enforcing the billing safety
    requirement: X API usage is paid by VANTA, so a per-tenant monthly
    post limit and a global monthly limit across every tenant both have
    to hold even under a retry bug hammering the publish path.

    One row per (scope, scope_key, month_key). scope is "tenant" (one
    row per location per month) or "global" (one row per month,
    scope_key is always the literal string "global"). The count is
    incremented only via a single atomic
    `UPDATE ... SET count = count + 1 WHERE count < :limit`
    (flyer_lady/billing/x_spend_guard.py) -- never read-then-write in
    two steps, which would leave a real race window under concurrent
    workers. This table has no data of its own value beyond that
    counter; it is not a per-post audit ledger."""
    __tablename__ = "x_usage_counters"
    __table_args__ = (UniqueConstraint("scope", "scope_key", "month_key", name="uq_x_usage_counter_scope"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)
    scope_key: Mapped[str] = mapped_column(String(40), nullable=False)
    month_key: Mapped[str] = mapped_column(String(7), nullable=False)
    count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class ThreadsOAuthSession(Base):
    """Short-lived server-side state for the Threads connect flow.

    Mirrors GoogleBusinessOAuthSession's shape (no PKCE needed here --
    unlike X, Threads' OAuth 2.0 flow is a plain authorization-code
    exchange with a client secret, confirmed directly against
    developers.facebook.com/documentation/threads/get-started/
    get-access-tokens-and-permissions). Holds the long-lived token
    only transiently between the callback and account confirmation,
    same reasoning as every other *OAuthSession model in this file: it
    must never sit in the browser-held Flask session."""
    __tablename__ = "threads_oauth_sessions"
    __table_args__ = (UniqueConstraint("state_nonce", name="uq_threads_oauth_state_nonce"), Index("ix_threads_oauth_location_status", "location_id", "status"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    state_nonce: Mapped[str] = mapped_column(String(128), nullable=False)
    encrypted_long_lived_token: Mapped[Optional[str]] = mapped_column(Text)
    redirect_uri: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="started", nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class ThreadsConnection(Base):
    """Location-scoped Threads connection, for posting to
    flyer_lady/platforms/threads_publisher.py. Mirrors XConnection's
    shape -- same one-connection-per-location pattern, same
    encrypted-token-at-rest approach. Only one token is stored (Threads
    long-lived user access tokens are self-refreshing in place, unlike
    X's separate access/refresh token pair -- see
    integrations/threads/auth/token_store.py)."""
    __tablename__ = "threads_connections"
    __table_args__ = (UniqueConstraint("location_id", name="uq_threads_connection_location"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    threads_user_id: Mapped[str] = mapped_column(String(100), nullable=False)
    threads_username: Mapped[Optional[str]] = mapped_column(String(100))
    encrypted_long_lived_token: Mapped[str] = mapped_column(Text, nullable=False)
    token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    token_key_version: Mapped[str] = mapped_column(String(20), default="v1", nullable=False)
    connection_status: Mapped[str] = mapped_column(String(40), default="connected", nullable=False)
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    last_health_check_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class TikTokOAuthSession(Base):
    """Short-lived server-side state for the TikTok connect flow.

    No PKCE column, unlike XOAuthSession: confirmed directly against
    TikTok's own Login Kit documentation that PKCE applies to desktop/
    iOS/Android public clients only -- the web/server flow (a
    confidential client holding the client_secret server-side, which is
    what VANTA's Flask backend is) uses the state parameter for
    CSRF protection instead, the same as Threads."""
    __tablename__ = "tiktok_oauth_sessions"
    __table_args__ = (UniqueConstraint("state_nonce", name="uq_tiktok_oauth_state_nonce"), Index("ix_tiktok_oauth_location_status", "location_id", "status"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    state_nonce: Mapped[str] = mapped_column(String(128), nullable=False)
    encrypted_access_token: Mapped[Optional[str]] = mapped_column(Text)
    encrypted_refresh_token: Mapped[Optional[str]] = mapped_column(Text)
    redirect_uri: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="started", nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)


class TikTokConnection(Base):
    """Location-scoped TikTok connection, for posting to
    flyer_lady/platforms/tiktok_publisher.py.

    selected_privacy_level is deliberately its own column, nullable,
    with no default: the required flow's own ordering -- creator_info,
    show nickname, THEN the user selects a privacy level, THEN
    publishing becomes possible -- and the explicit "do not silently
    choose a privacy level" requirement both mean a connection can
    exist in a real, valid state (OAuth complete, account discovered)
    while still being unable to publish, because this is not set yet.
    flyer_lady/publish_service.py's tiktok_post branch checks for
    exactly that and refuses to guess."""
    __tablename__ = "tiktok_connections"
    __table_args__ = (UniqueConstraint("location_id", name="uq_tiktok_connection_location"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    tiktok_open_id: Mapped[str] = mapped_column(String(100), nullable=False)
    tiktok_username: Mapped[Optional[str]] = mapped_column(String(100))
    creator_nickname: Mapped[Optional[str]] = mapped_column(String(255))
    allowed_privacy_levels: Mapped[Optional[list]] = mapped_column(JSON)
    selected_privacy_level: Mapped[Optional[str]] = mapped_column(String(40))
    encrypted_access_token: Mapped[str] = mapped_column(Text, nullable=False)
    encrypted_refresh_token: Mapped[str] = mapped_column(Text, nullable=False)
    token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    token_key_version: Mapped[str] = mapped_column(String(20), default="v1", nullable=False)
    connection_status: Mapped[str] = mapped_column(String(40), default="connected", nullable=False)
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    last_health_check_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

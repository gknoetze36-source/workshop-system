"""Persistence models for PHANTA's external integrations.

Google Calendar models are intentionally absent: PHANTA v1 does not use
Google Calendar. Customer review URLs are stored on Location in models/core.py;
they require no Google/HelloPeter API integration.
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

"""PHANTA core relational database models.

Phase 2 implementation:
- Multi-location source of truth
- Customer/vehicle/booking/service history
- Conversations/messages
- Recommendations/quotes/approvals
- Follow-ups/tasks
- Audit logs/conversation summaries/tool executions

The models deliberately use portable SQLAlchemy types so the same schema can
be exercised with SQLite in tests and PostgreSQL in production.
PostgreSQL-specific protections such as Row Level Security and the booking
EXCLUDE constraint are documented in migrations/README.md and are applied
at the production database layer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    JSON,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class Owner(Base, TimestampMixin):
    """Canonical business owner. One owner has exactly one location."""
    __tablename__ = "owners"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(Integer, unique=True)
    name: Mapped[Optional[str]] = mapped_column(String(200))
    email: Mapped[Optional[str]] = mapped_column(String(320))
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    location: Mapped[Optional["Location"]] = relationship(back_populates="owner", uselist=False)


class Location(Base, TimestampMixin):
    """Canonical operational location and universal business scope.

    One owner has exactly one location. All customer, booking, messaging,
    automation, payment and integration records are scoped directly to this
    location.
    """
    __tablename__ = "locations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    owner_id: Mapped[int] = mapped_column(ForeignKey("owners.id", ondelete="CASCADE"), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    slug: Mapped[Optional[str]] = mapped_column(String(200))
    industry: Mapped[str] = mapped_column(String(80), default="workshop", nullable=False)
    legal_name: Mapped[Optional[str]] = mapped_column(String(250))
    support_email: Mapped[Optional[str]] = mapped_column(String(320))
    review_platform: Mapped[Optional[str]] = mapped_column(String(30))
    review_url: Mapped[Optional[str]] = mapped_column(String(1000))
    review_request_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    review_message_template: Mapped[Optional[str]] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    owner: Mapped["Owner"] = relationship(back_populates="location", uselist=False)
    customers: Mapped[list["Customer"]] = relationship(back_populates="location", cascade="all, delete-orphan")
    bookings: Mapped[list["Booking"]] = relationship(back_populates="location", cascade="all, delete-orphan")
    conversations: Mapped[list["Conversation"]] = relationship(back_populates="location", cascade="all, delete-orphan")
    tasks: Mapped[list["Task"]] = relationship(back_populates="location", cascade="all, delete-orphan")
    audit_logs: Mapped[list["AuditLog"]] = relationship(back_populates="location", cascade="all, delete-orphan")


class Customer(Base, TimestampMixin):
    __tablename__ = "customers"
    __table_args__ = (
        UniqueConstraint("location_id", "whatsapp_number", name="uq_customer_location_whatsapp"),
        Index("ix_customers_location_name", "location_id", "last_name", "first_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    whatsapp_number: Mapped[str] = mapped_column(String(32), nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(320))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    privacy_status: Mapped[Optional[str]] = mapped_column(String(50))
    deleted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    location: Mapped["Location"] = relationship(back_populates="customers")
    vehicles: Mapped[list["Vehicle"]] = relationship(back_populates="customer", cascade="all, delete-orphan")
    bookings: Mapped[list["Booking"]] = relationship(back_populates="customer")
    conversations: Mapped[list["Conversation"]] = relationship(back_populates="customer")
    follow_ups: Mapped[list["FollowUp"]] = relationship(back_populates="customer")
    quotes: Mapped[list["Quote"]] = relationship(back_populates="customer")


class Vehicle(Base, TimestampMixin):
    __tablename__ = "vehicles"
    __table_args__ = (
        Index("ix_vehicles_location_customer", "location_id", "customer_id"),
        Index("ix_vehicles_customer", "customer_id"),
        Index("ix_vehicles_registration", "registration"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    make: Mapped[str] = mapped_column(String(100), nullable=False)
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    mileage: Mapped[Optional[int]] = mapped_column(Integer)
    engine: Mapped[Optional[str]] = mapped_column(String(100))
    transmission: Mapped[Optional[str]] = mapped_column(String(100))
    vin: Mapped[Optional[str]] = mapped_column(String(64))
    registration: Mapped[Optional[str]] = mapped_column(String(32))

    customer: Mapped["Customer"] = relationship(back_populates="vehicles")
    bookings: Mapped[list["Booking"]] = relationship(back_populates="vehicle")
    services: Mapped[list["Service"]] = relationship(back_populates="vehicle", cascade="all, delete-orphan")
    recommendations: Mapped[list["Recommendation"]] = relationship(back_populates="vehicle", cascade="all, delete-orphan")


class Booking(Base, TimestampMixin):
    __tablename__ = "bookings"
    __table_args__ = (
        Index("ix_bookings_location_start", "location_id", "start_time"),
        Index("ix_bookings_vehicle_start", "vehicle_id", "start_time"),
        CheckConstraint("end_time > start_time", name="ck_booking_time_order"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id", ondelete="RESTRICT"), nullable=False)
    vehicle_id: Mapped[int] = mapped_column(ForeignKey("vehicles.id", ondelete="RESTRICT"), nullable=False)
    bay_id: Mapped[Optional[int]] = mapped_column(Integer)
    technician_id: Mapped[Optional[int]] = mapped_column(Integer)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(40), default="pending", nullable=False)
    service_type: Mapped[str] = mapped_column(String(100), nullable=False)
    source: Mapped[str] = mapped_column(String(30), default="whatsapp", nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)

    location: Mapped["Location"] = relationship(back_populates="bookings")
    customer: Mapped["Customer"] = relationship(back_populates="bookings")
    vehicle: Mapped["Vehicle"] = relationship(back_populates="bookings")
    services: Mapped[list["Service"]] = relationship(back_populates="booking", cascade="all, delete-orphan")
    quotes: Mapped[list["Quote"]] = relationship(back_populates="booking", cascade="all, delete-orphan")


class BookingConfirmation(Base):
    __tablename__ = "booking_confirmations"
    __table_args__ = (
        UniqueConstraint("location_id", "booking_id", name="uq_booking_confirmation_location_booking"),
        Index("ix_booking_confirmations_location_customer_decided", "location_id", "customer_id", "decided_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    booking_id: Mapped[int] = mapped_column(ForeignKey("bookings.id", ondelete="RESTRICT"), nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id", ondelete="RESTRICT"), nullable=False)
    decision: Mapped[str] = mapped_column(String(20), nullable=False)
    raw_message: Mapped[str] = mapped_column(Text, nullable=False)
    channel: Mapped[str] = mapped_column(String(30), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    booking: Mapped["Booking"] = relationship()
    customer: Mapped["Customer"] = relationship()


class Service(Base, TimestampMixin):
    __tablename__ = "service_records"
    __table_args__ = (
        Index("ix_services_location_vehicle_performed", "location_id", "vehicle_id", "performed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    vehicle_id: Mapped[int] = mapped_column(ForeignKey("vehicles.id", ondelete="CASCADE"), nullable=False)
    booking_id: Mapped[Optional[int]] = mapped_column(ForeignKey("bookings.id", ondelete="SET NULL"))
    service_type: Mapped[str] = mapped_column(String(100), nullable=False)
    performed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    mileage_at_service: Mapped[Optional[int]] = mapped_column(Integer)
    notes: Mapped[Optional[str]] = mapped_column(Text)

    vehicle: Mapped["Vehicle"] = relationship(back_populates="services")
    booking: Mapped[Optional["Booking"]] = relationship(back_populates="services")


class Conversation(Base, TimestampMixin):
    __tablename__ = "conversations"
    __table_args__ = (
        Index("ix_conversations_location_customer", "location_id", "customer_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    channel: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    location: Mapped["Location"] = relationship(back_populates="conversations")
    customer: Mapped["Customer"] = relationship(back_populates="conversations")
    messages: Mapped[list["Message"]] = relationship(back_populates="conversation", cascade="all, delete-orphan")
    summaries: Mapped[list["ConversationSummary"]] = relationship(back_populates="conversation", cascade="all, delete-orphan")
    tool_executions: Mapped[list["ToolExecution"]] = relationship(back_populates="conversation", cascade="all, delete-orphan")


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("channel", "whatsapp_message_id", name="uq_message_channel_external_id"),
        Index("ix_messages_location_conversation_created", "location_id", "conversation_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)
    channel: Mapped[str] = mapped_column(String(30), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    whatsapp_message_id: Mapped[Optional[str]] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(30), default="received", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    conversation: Mapped["Conversation"] = relationship(back_populates="messages")


class ServiceRule(Base, TimestampMixin):
    """Deterministic maintenance rule used by the Service Advisor."""
    __tablename__ = "service_rules"
    __table_args__ = (
        Index("ix_service_rules_scope", "location_id", "make", "model", "engine"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"))
    service_type: Mapped[str] = mapped_column(String(100), nullable=False)
    interval_km: Mapped[Optional[int]] = mapped_column(Integer)
    interval_months: Mapped[Optional[int]] = mapped_column(Integer)
    make: Mapped[Optional[str]] = mapped_column(String(100))
    model: Mapped[Optional[str]] = mapped_column(String(100))
    engine: Mapped[Optional[str]] = mapped_column(String(100))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Recommendation(Base, TimestampMixin):
    __tablename__ = "recommendations"
    __table_args__ = (
        Index("ix_recommendations_location_vehicle_status", "location_id", "vehicle_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    vehicle_id: Mapped[int] = mapped_column(ForeignKey("vehicles.id", ondelete="CASCADE"), nullable=False)
    service_type: Mapped[str] = mapped_column(String(100), nullable=False)
    due_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    due_mileage: Mapped[Optional[int]] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String(50), default="rule_engine", nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="open", nullable=False)

    vehicle: Mapped["Vehicle"] = relationship(back_populates="recommendations")


class Quote(Base, TimestampMixin):
    __tablename__ = "quotes"
    __table_args__ = (
        Index("ix_quotes_location_status", "location_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id", ondelete="RESTRICT"), nullable=False)
    booking_id: Mapped[Optional[int]] = mapped_column(ForeignKey("bookings.id", ondelete="SET NULL"))
    status: Mapped[str] = mapped_column(String(30), default="draft", nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="ZAR", nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=0, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)

    booking: Mapped[Optional["Booking"]] = relationship(back_populates="quotes")
    customer: Mapped["Customer"] = relationship(back_populates="quotes")
    line_items: Mapped[list["QuoteLineItem"]] = relationship(back_populates="quote", cascade="all, delete-orphan")


class QuoteLineItem(Base, TimestampMixin):
    __tablename__ = "quote_line_items"
    __table_args__ = (
        Index("ix_quote_items_location_quote", "location_id", "quote_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    quote_id: Mapped[int] = mapped_column(ForeignKey("quotes.id", ondelete="CASCADE"), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    labour_category: Mapped[Optional[str]] = mapped_column(String(100))
    parts: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    ai_confidence: Mapped[Optional[float]] = mapped_column(Numeric(5, 4))
    status: Mapped[str] = mapped_column(String(30), default="ai_suggested", nullable=False)

    quote: Mapped["Quote"] = relationship(back_populates="line_items")
    approvals: Mapped[list["Approval"]] = relationship(back_populates="quote_line_item", cascade="all, delete-orphan")


class Approval(Base):
    __tablename__ = "approvals"
    __table_args__ = (
        Index("ix_approvals_location_line_item_decided", "location_id", "quote_line_item_id", "decided_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    quote_line_item_id: Mapped[int] = mapped_column(ForeignKey("quote_line_items.id", ondelete="RESTRICT"), nullable=False)
    decision: Mapped[str] = mapped_column(String(20), nullable=False)
    decided_by: Mapped[str] = mapped_column(String(100), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    raw_message: Mapped[str] = mapped_column(Text, nullable=False)
    channel: Mapped[str] = mapped_column(String(30), nullable=False)

    quote_line_item: Mapped["QuoteLineItem"] = relationship(back_populates="approvals")


class FollowUp(Base, TimestampMixin):
    __tablename__ = "follow_ups"
    __table_args__ = (
        Index("ix_followups_location_due", "location_id", "scheduled_for", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="scheduled", nullable=False)
    channel: Mapped[str] = mapped_column(String(30), default="whatsapp", nullable=False)
    payload: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)

    customer: Mapped["Customer"] = relationship(back_populates="follow_ups")


class Task(Base, TimestampMixin):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("ix_tasks_location_status_priority", "location_id", "status", "priority"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    related_entity: Mapped[Optional[str]] = mapped_column(String(255))
    assigned_to: Mapped[Optional[int]] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(30), default="open", nullable=False)
    priority: Mapped[str] = mapped_column(String(20), default="normal", nullable=False)
    details: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)

    location: Mapped["Location"] = relationship(back_populates="tasks")


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (
        Index("ix_audit_location_created", "location_id", "created_at"),
        Index("ix_audit_entity", "entity_type", "entity_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    actor: Mapped[str] = mapped_column(String(50), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(100), nullable=False)
    before: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    after: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    location: Mapped["Location"] = relationship(back_populates="audit_logs")


class ConversationSummary(Base):
    __tablename__ = "conversation_summaries"
    __table_args__ = (Index("ix_summaries_location_customer_created", "location_id", "customer_id", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    customer_id: Mapped[int] = mapped_column(ForeignKey("customers.id", ondelete="CASCADE"), nullable=False)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False)
    summary_text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    conversation: Mapped["Conversation"] = relationship(back_populates="summaries")


class ToolExecution(Base):
    __tablename__ = "tool_executions"
    __table_args__ = (
        Index("ix_tool_exec_location_conversation_created", "location_id", "conversation_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id", ondelete="CASCADE"), nullable=False)
    conversation_id: Mapped[int] = mapped_column(ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False)
    tool_name: Mapped[str] = mapped_column(String(150), nullable=False)
    arguments: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    result: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    conversation: Mapped["Conversation"] = relationship(back_populates="tool_executions")


# Phase 15: booking confirmations are append-only evidence.
from sqlalchemy import event as _sa_event

@_sa_event.listens_for(BookingConfirmation, "before_update")
def _prevent_booking_confirmation_update(mapper, connection, target):
    raise ValueError("booking confirmations are immutable")

@_sa_event.listens_for(BookingConfirmation, "before_delete")
def _prevent_booking_confirmation_delete(mapper, connection, target):
    raise ValueError("booking confirmations are immutable")

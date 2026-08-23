"""Phase 19 dashboard queries.

Two deliberately separate read models:
- WorkshopDashboardQueries: operational information for a workshop/reception user.
- PlatformAdminDashboardQueries: PHANTA operator information across locations.

No pricing, repair authorisation, or CRM-style customer analytics are exposed here.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from sqlalchemy import func, select, and_, exists
from sqlalchemy.orm import Session

from models.core import Booking, BookingConfirmation, Conversation, Message, Location, Customer, Vehicle, AuditLog
from sqlalchemy import text
from models.integration_models import (
    AIUsageLog, Invoice, MetaAuditLog, MetaBusinessConnection,
    MetaWebhookEvent, Payment, PaystackWebhookEvent, Subscription, MetaBusinessVerificationStatus, MetaPermissionGrant, MetaSocialConnection, MetaSocialOAuthSession,
)


ACTIVE_BOOKING_STATUSES = {"pending", "confirmed", "checked_in", "in_progress", "ready_for_collection"}
WAITING_STATUSES = {"checked_in", "in_progress", "ready_for_collection"}


class WorkshopDashboardQueries:
    def __init__(self, session: Session, location_id: int, now: datetime | None = None):
        self.session = session
        self.location_id = location_id
        self.now = now or datetime.now(timezone.utc)

    def todays_bookings(self) -> list[Booking]:
        today = self.now.date()
        return list(self.session.scalars(
            select(Booking)
            .where(Booking.location_id == self.location_id,
                   func.date(Booking.start_time) == today)
            .order_by(Booking.start_time.asc())
        ).all())

    def vehicles_waiting(self) -> list[Booking]:
        return list(self.session.scalars(
            select(Booking)
            .where(Booking.location_id == self.location_id,
                   Booking.status.in_(WAITING_STATUSES))
            .order_by(Booking.start_time.asc())
        ).all())

    def overdue_vehicles(self) -> list[Booking]:
        # Overdue means an active booking whose scheduled end has passed.
        return list(self.session.scalars(
            select(Booking)
            .where(Booking.location_id == self.location_id,
                   Booking.status.in_(ACTIVE_BOOKING_STATUSES),
                   Booking.end_time < self.now)
            .order_by(Booking.end_time.asc())
        ).all())

    def booking_requests_needing_confirmation(self) -> list[Booking]:
        """Customer booking decisions that have not yet been recorded.

        This replaces the old repair-authorisation/approval queue. PHANTA only
        confirms bookings; it never authorises repairs or spending.
        """
        confirmation_exists = exists(
            select(BookingConfirmation.id).where(
                BookingConfirmation.location_id == self.location_id,
                BookingConfirmation.booking_id == Booking.id,
            )
        )
        return list(self.session.scalars(
            select(Booking)
            .where(Booking.location_id == self.location_id,
                   Booking.status == "pending",
                   ~confirmation_exists)
            .order_by(Booking.created_at.asc())
        ).all())

    def unanswered_messages(self) -> list[Message]:
        """Return conversations whose latest customer message is unanswered."""
        latest_inbound = select(func.max(Message.id)).where(
            Message.location_id == self.location_id,
            Message.conversation_id == Conversation.id,
            Message.direction == "inbound",
        ).correlate(Conversation).scalar_subquery()
        latest_message = select(Message).where(Message.id == latest_inbound).scalar_subquery()
        # A conversation is unanswered when its latest message is inbound.
        return list(self.session.scalars(
            select(Message)
            .join(Conversation, Conversation.id == Message.conversation_id)
            .where(Message.location_id == self.location_id,
                   Message.direction == "inbound",
                   Message.id == latest_inbound)
            .order_by(Message.created_at.desc())
        ).all())

    def booking_notes(self, vehicle_ids: list[int]) -> dict[int, list[dict]]:
        if not vehicle_ids:
            return {}
        rows = self.session.execute(text("""
            SELECT id, subject_id, content, created_by_user_id, created_at, updated_at
            FROM notes
            WHERE location_id = :location_id
              AND subject_type = 'vehicle'
              AND subject_id = ANY(:vehicle_ids)
            ORDER BY created_at DESC
        """), {"location_id": self.location_id, "vehicle_ids": vehicle_ids}).mappings().all()
        result = {vehicle_id: [] for vehicle_id in vehicle_ids}
        for row in rows:
            result[int(row["subject_id"])].append(dict(row))
        return result

    def connection_health(self) -> dict:
        connection = self.session.scalar(select(MetaBusinessConnection).where(
            MetaBusinessConnection.location_id == self.location_id))
        if not connection:
            return {
                "status": "not_connected",
                "quality_rating": None,
                "last_health_check_at": None,
                "phone_number": None,
                "reconnect_required": True,
            }
        return {
            "status": connection.connection_status,
            "quality_rating": connection.quality_rating,
            "last_health_check_at": connection.last_health_check_at,
            "phone_number": connection.display_phone_number,
            "reconnect_required": connection.connection_status == "reconnect_required",
        }

    def billing_state(self) -> dict:
        subscription = self.session.scalar(select(Subscription).where(
            Subscription.location_id == self.location_id).order_by(Subscription.id.desc()))
        invoice = self.session.scalar(select(Invoice).join(Subscription, Invoice.subscription_id == Subscription.id)
            .where(Invoice.location_id == self.location_id)
            .order_by(Invoice.id.desc()))
        return {
            "subscription_status": subscription.status if subscription else "not_configured",
            "current_period_end": subscription.current_period_end if subscription else None,
            "latest_invoice_status": invoice.status if invoice else None,
        }


class PlatformAdminDashboardQueries:
    def __init__(self, session: Session, now: datetime | None = None):
        self.session = session
        self.now = now or datetime.now(timezone.utc)

    def connection_health(self) -> dict:
        rows = self.session.execute(select(
            MetaBusinessConnection.connection_status,
            func.count(MetaBusinessConnection.id)
        ).group_by(MetaBusinessConnection.connection_status)).all()
        return {status: count for status, count in rows}

    def billing_state(self) -> dict:
        rows = self.session.execute(select(
            Subscription.status, func.count(Subscription.id)
        ).group_by(Subscription.status)).all()
        return {status: count for status, count in rows}

    def ai_usage_cost(self, since: datetime | None = None) -> dict:
        since = since or (self.now - timedelta(days=30))
        row = self.session.execute(select(
            func.coalesce(func.sum(AIUsageLog.input_tokens), 0),
            func.coalesce(func.sum(AIUsageLog.output_tokens), 0),
            func.coalesce(func.sum(AIUsageLog.estimated_cost), 0),
            func.count(AIUsageLog.id),
        ).where(AIUsageLog.created_at >= since)).one()
        return {
            "period_start": since,
            "input_tokens": int(row[0] or 0),
            "output_tokens": int(row[1] or 0),
            "estimated_cost": float(row[2] or 0),
            "requests": int(row[3] or 0),
        }

    def integration_errors(self, limit: int = 25) -> list[dict]:
        meta = list(self.session.execute(select(MetaWebhookEvent).where(
            MetaWebhookEvent.processing_status.in_(["failed", "error"])
        ).order_by(MetaWebhookEvent.received_at.desc()).limit(limit)).scalars())
        paystack = list(self.session.execute(select(PaystackWebhookEvent).where(
            PaystackWebhookEvent.processing_status.in_(["failed", "error"])
        ).order_by(PaystackWebhookEvent.received_at.desc()).limit(limit)).scalars())
        errors = [
            {"provider": "meta", "type": e.event_type, "location_id": e.location_id, "at": e.received_at}
            for e in meta
        ] + [
            {"provider": "paystack", "type": e.event_type, "location_id": e.location_id, "at": e.received_at}
            for e in paystack
        ]
        return sorted(errors, key=lambda x: x["at"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)[:limit]


class ClientAuditQueries:
    """Read-only, evidence-based client audit centre queries.

    All cross-location reads require the platform-admin database session. No
    secrets or raw integration tokens are returned. Missing evidence remains
    ``None``/an empty list rather than being replaced with a guessed status.
    """
    def __init__(self, session: Session):
        self.session = session

    def clients(self, limit: int = 500) -> list[dict]:
        locations = list(self.session.scalars(select(Location).order_by(Location.name.asc()).limit(limit)).all())
        if not locations:
            return []
        ids = [t.id for t in locations]
        connections = {r.location_id: r for r in self.session.scalars(select(MetaBusinessConnection).where(MetaBusinessConnection.location_id.in_(ids))).all()}
        social = {r.location_id: r for r in self.session.scalars(select(MetaSocialConnection).where(MetaSocialConnection.location_id.in_(ids))).all()}
        subs = list(self.session.scalars(select(Subscription).where(Subscription.location_id.in_(ids)).order_by(Subscription.id.desc())).all())
        latest_sub = {}
        for row in subs:
            latest_sub.setdefault(row.location_id, row)
        return [{
            "location_id": t.id, "name": t.name, "legal_name": t.legal_name, "support_email": t.support_email,
            "active": t.active,
            "whatsapp": {"status": c.connection_status, "waba_id": c.waba_id, "phone_number_id": c.phone_number_id} if (c := connections.get(t.id)) else None,
            "flyer_lady": {"status": s.connection_status, "page_id": s.page_id, "page_name": s.page_name} if (s := social.get(t.id)) else None,
            "billing": {"status": latest_sub[t.id].status} if t.id in latest_sub else None,
        } for t in locations]

    def client(self, location_id: int) -> dict | None:
        location = self.session.get(Location, location_id)
        if not location:
            return None
        whatsapp = self.session.scalar(select(MetaBusinessConnection).where(MetaBusinessConnection.location_id == location_id))
        verification = self.session.scalar(select(MetaBusinessVerificationStatus).where(MetaBusinessVerificationStatus.location_id == location_id).order_by(MetaBusinessVerificationStatus.id.desc()))
        permissions = list(self.session.scalars(select(MetaPermissionGrant).where(MetaPermissionGrant.location_id == location_id).order_by(MetaPermissionGrant.permission.asc())).all())
        social = self.session.scalar(select(MetaSocialConnection).where(MetaSocialConnection.location_id == location_id))
        subscription = self.session.scalar(select(Subscription).where(Subscription.location_id == location_id).order_by(Subscription.id.desc()))
        invoice = self.session.scalar(select(Invoice).where(Invoice.location_id == location_id).order_by(Invoice.id.desc()))
        meta_failed = self.session.scalar(select(func.count(MetaWebhookEvent.id)).where(MetaWebhookEvent.location_id == location_id, MetaWebhookEvent.processing_status.in_(["failed", "error"]))) or 0
        pay_failed = self.session.scalar(select(func.count(PaystackWebhookEvent.id)).where(PaystackWebhookEvent.location_id == location_id, PaystackWebhookEvent.processing_status.in_(["failed", "error"]))) or 0
        audit_rows = list(self.session.scalars(select(AuditLog).where(AuditLog.location_id == location_id).order_by(AuditLog.created_at.desc()).limit(50)).all())
        return {
            "location": {"id": location.id, "name": location.name, "legal_name": location.legal_name, "support_email": location.support_email, "active": location.active, "created_at": location.created_at},
            "whatsapp": ({"connection_status": whatsapp.connection_status, "business_id": whatsapp.business_id, "waba_id": whatsapp.waba_id, "phone_number_id": whatsapp.phone_number_id, "display_phone_number": whatsapp.display_phone_number, "verified_name": whatsapp.verified_name, "quality_rating": whatsapp.quality_rating, "messaging_tier": whatsapp.messaging_tier, "token_stored": bool(whatsapp.encrypted_access_token), "token_expires_at": whatsapp.token_expires_at, "last_health_check_at": whatsapp.last_health_check_at} if whatsapp else None),
            "verification": ({"business_verification_status": verification.business_verification_status, "display_name_status": verification.display_name_status, "phone_verification_status": verification.phone_verification_status, "last_checked_at": verification.last_checked_at} if verification else None),
            "permissions": [{"permission": p.permission, "access_level": p.access_level, "granted": p.granted, "granted_at": p.granted_at} for p in permissions],
            "flyer_lady": ({"connection_status": social.connection_status, "page_id": social.page_id, "page_name": social.page_name, "instagram_business_account_id": social.instagram_business_account_id, "token_stored": bool(social.encrypted_page_access_token), "token_expires_at": social.token_expires_at, "last_health_check_at": social.last_health_check_at} if social else None),
            "billing": ({"subscription_status": subscription.status, "current_period_end": subscription.current_period_end, "latest_invoice_status": invoice.status if invoice else None} if subscription else None),
            "webhooks": {"meta_failed": int(meta_failed), "paystack_failed": int(pay_failed)},
            "audit_log": [{"action": a.action, "entity_type": a.entity_type, "entity_id": a.entity_id, "created_at": a.created_at} for a in audit_rows],
        }

"""Phase 18 post-service review requests.

Review providers are intentionally not integrated. PHANTA stores one workshop
configured review URL and, when enabled, sends that plain URL through the
existing WhatsApp messaging layer after a booking/service is completed.
"""
from __future__ import annotations

from urllib.parse import urlparse

from sqlalchemy import select

from models.core import Booking, Conversation, Customer, FollowUp, Location
from repositories.audit_repo import AuditLogRepository
from integrations.meta.messaging.messaging_service import MetaMessagingService


class ReviewConfigurationError(ValueError):
    pass


class PostServiceReviewService:
    """Small deterministic service for workshop-configured review links."""

    ALLOWED_PLATFORMS = {"google", "hellopeter"}
    PLATFORM_HOSTS = {
        "google": ("google.com", "g.page", "google.co.za", "maps.google.com"),
        "hellopeter": ("hellopeter.com", "www.hellopeter.com"),
    }

    DEFAULT_MESSAGE = (
        "Thank you for choosing {workshop}. We hope you were happy with the service. "
        "If you have a moment, we would really appreciate your feedback:\n{url}"
    )

    def __init__(self, session, messaging: MetaMessagingService | None = None):
        self.session = session
        self.messaging = messaging
        self.audit = AuditLogRepository(session)

    @classmethod
    def validate_url(cls, platform: str, url: str) -> str:
        platform = str(platform or "").strip().lower()
        url = str(url or "").strip()
        if platform not in cls.ALLOWED_PLATFORMS:
            raise ReviewConfigurationError("review platform must be google or hellopeter")
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ReviewConfigurationError("review URL must be a valid HTTPS URL")
        host = parsed.hostname.lower().rstrip(".") if parsed.hostname else ""
        allowed = cls.PLATFORM_HOSTS[platform]
        if not any(host == item or host.endswith("." + item) for item in allowed):
            raise ReviewConfigurationError(f"review URL is not a valid {platform} URL")
        return url

    def configure(self, location_id: int, *, platform: str | None, url: str | None, enabled: bool) -> Location:
        location = self.session.scalar(select(Location).where(Location.id == location_id))
        if location is None:
            raise ReviewConfigurationError("workshop not found")

        if enabled:
            if not platform or not url:
                raise ReviewConfigurationError("platform and URL are required when review requests are enabled")
            url = self.validate_url(platform, url)
            location.review_platform = str(platform).strip().lower()
            location.review_url = url
            location.review_request_enabled = True
        else:
            location.review_request_enabled = False
            if platform is not None:
                platform = str(platform).strip().lower()
                if platform:
                    if platform not in self.ALLOWED_PLATFORMS:
                        raise ReviewConfigurationError("review platform must be google or hellopeter")
                    location.review_platform = platform
            if url is not None:
                location.review_url = self.validate_url(location.review_platform or "google", url)

        self.session.flush()
        self.audit.record(
            location_id,
            "staff",
            "review.configuration_updated",
            "location",
            location_id,
            after={
                "review_platform": location.review_platform,
                "review_url": location.review_url,
                "review_request_enabled": location.review_request_enabled,
            },
        )
        return location

    def _conversation(self, location_id: int, customer_id: int) -> Conversation:
        conversation = self.session.scalar(
            select(Conversation)
            .where(
                Conversation.location_id == location_id,
                Conversation.customer_id == customer_id,
                Conversation.channel == "whatsapp",
            )
            .order_by(Conversation.started_at.desc())
        )
        if conversation:
            return conversation
        conversation = Conversation(location_id=location_id, customer_id=customer_id, channel="whatsapp")
        self.session.add(conversation)
        self.session.flush()
        return conversation

    def send_for_booking(self, location_id: int, booking_id: int):
        booking = self.session.scalar(
            select(Booking).where(Booking.id == booking_id, Booking.location_id == location_id)
        )
        if booking is None:
            raise ReviewConfigurationError("booking not found")
        if booking.status != "completed":
            raise ReviewConfigurationError("review request requires a completed booking")

        location = self.session.scalar(select(Location).where(Location.id == location_id))
        if location is None:
            raise ReviewConfigurationError("workshop not found")
        if not location.review_request_enabled:
            return None
        if not location.review_platform or not location.review_url:
            return None
        url = self.validate_url(location.review_platform, location.review_url)

        # A completed booking must produce at most one automatic review request.
        existing = self.session.scalar(select(FollowUp).where(
            FollowUp.location_id == location_id,
            FollowUp.customer_id == booking.customer_id,
            FollowUp.type == "post_service_review",
            FollowUp.payload["booking_id"].as_integer() == booking.id,
        ))
        if existing:
            return None

        customer = self.session.scalar(select(Customer).where(
            Customer.id == booking.customer_id,
            Customer.location_id == location_id,
        ))
        if customer is None:
            raise ReviewConfigurationError("customer not found")
        if self.messaging is None:
            raise RuntimeError("MetaMessagingService is required for outbound review requests")

        conversation = self._conversation(location_id, customer.id)
        body = self.DEFAULT_MESSAGE.format(workshop=location.name, url=url)
        message = self.messaging.send_auto(
            location_id=location_id,
            conversation_id=conversation.id,
            to=customer.whatsapp_number,
            body=body,
        )

        # Use FollowUp as the durable idempotency record, not as a scheduled
        # message. The message is sent immediately on completion.
        record = FollowUp(
            location_id=location_id,
            customer_id=customer.id,
            type="post_service_review",
            scheduled_for=booking.updated_at,
            status="sent",
            channel="whatsapp",
            payload={
                "booking_id": booking.id,
                "vehicle_id": booking.vehicle_id,
                "review_platform": location.review_platform,
                "review_url": url,
                "message_id": message.id,
            },
        )
        self.session.add(record)
        self.session.flush()
        self.audit.record(
            location_id,
            "system",
            "review.post_service_request_sent",
            "booking",
            booking.id,
            after={"message_id": message.id, "review_platform": location.review_platform},
        )
        return message

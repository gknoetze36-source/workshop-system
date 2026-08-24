"""Phase 12 runtime assembly.

Keeps provider construction and WhatsApp delivery outside conversation/business logic.
"""
from __future__ import annotations

from integrations.ai.providers.openai_provider import OpenAIProvider
from integrations.ai.services.ai_dispatcher import AIDispatcher
from integrations.ai.conversations.conversation_service import AIConversationService
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingService
from integrations.meta.services.graph_api_client import GraphApiClient
from ai.booking.availability import BookingAvailabilityService, OperatingWindow, WorkshopSchedule
from ai.booking.service import BookingService
from datetime import time


def build_service_advisor(session):
    provider = OpenAIProvider()
    dispatcher = AIDispatcher({"openai": provider})
    return AIConversationService(dispatcher)


def build_booking_service(session, location_id: int):
    """Build the Phase 11 booking service used by Service Advisor tools.

    Reads the location's actual configured hours via
    services/operating_hours_service.py rather than a hardcoded Mon-Fri 8-5
    window. See that module's docstring for why this was extracted.
    """
    from services.operating_hours_service import build_workshop_schedule

    schedule = build_workshop_schedule(location_id)
    return BookingService(session, BookingAvailabilityService(session, schedule))


def deliver_whatsapp(session, *, location_id: int, conversation_id: int, customer_id: int, text: str):
    service = MetaMessagingService(
        session,
        graph=GraphApiClient(MetaAuthConfig.from_env()),
        token_store=MetaTokenStore(),
    )
    from models.core import Customer
    customer = session.get(Customer, customer_id)
    if not customer or customer.location_id != location_id:
        raise ValueError("customer not found")
    return service.send_auto(
        location_id=location_id,
        conversation_id=conversation_id,
        to=customer.whatsapp_number,
        body=text,
    )

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

    Production location configuration can replace the default weekday windows
    without changing the Phase 12 conversation contract.
    """
    hours = {d: [OperatingWindow(time(8, 0), time(17, 0))] for d in range(5)}
    return BookingService(session, BookingAvailabilityService(session, WorkshopSchedule(hours)))


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

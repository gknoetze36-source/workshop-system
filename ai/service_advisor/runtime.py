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
    """Build the AI conversation service used to reply on WhatsApp.

    Previously never passed usage_repo to AIDispatcher, so
    AIDispatcher._log_usage()/_log_error() were both silent no-ops for
    every real Service Advisor conversation -- ai_usage_log exists (with
    its own RLS policy, migration 0021) specifically to record this, and
    integrations/ai/repositories/ai_usage_repo.py's AIUsageRepository was
    already built and ready; it just never got the session it needed.
    Every OpenAI call's tokens, cost, latency, and any retry/failure were
    silently lost instead of tracked.
    """
    from integrations.ai.repositories.ai_usage_repo import AIUsageRepository

    provider = OpenAIProvider()
    dispatcher = AIDispatcher({"openai": provider}, usage_repo=AIUsageRepository(session))
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

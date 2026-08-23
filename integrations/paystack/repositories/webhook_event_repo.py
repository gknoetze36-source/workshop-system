from __future__ import annotations
from models.integration_models import PaystackWebhookEvent
class PaystackWebhookEventRepository:
    def get(self, session, event_key): return session.query(PaystackWebhookEvent).filter_by(event_key=event_key).one_or_none()
    def create(self, session, *, location_id, event_key, event_type, payload, signature_valid=True):
        event = PaystackWebhookEvent(location_id=location_id, event_key=event_key, event_type=event_type, payload=payload, signature_valid=signature_valid)
        session.add(event); session.flush(); return event

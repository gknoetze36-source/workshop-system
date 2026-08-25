from sqlalchemy import select
from sqlalchemy.orm import Session

from models.core import (
    Customer, Vehicle, Booking, Conversation, Quote, QuoteLineItem
)

class TenantIntegrityError(ValueError):
    """Raised when related records do not belong to the requested tenant."""

class TenantGuard:
    def __init__(self, session: Session):
        self.session = session

    def customer(self, tenant_id, customer_id):
        obj = self.session.scalar(select(Customer).where(Customer.id == customer_id, Customer.tenant_id == tenant_id))
        if obj is None:
            raise TenantIntegrityError("customer does not belong to tenant")
        return obj

    def vehicle(self, tenant_id, vehicle_id):
        obj = self.session.scalar(select(Vehicle).where(Vehicle.id == vehicle_id, Vehicle.tenant_id == tenant_id))
        if obj is None:
            raise TenantIntegrityError("vehicle does not belong to tenant")
        return obj

    def booking(self, tenant_id, booking_id):
        obj = self.session.scalar(select(Booking).where(Booking.id == booking_id, Booking.tenant_id == tenant_id))
        if obj is None:
            raise TenantIntegrityError("booking does not belong to tenant")
        return obj

    def conversation(self, tenant_id, conversation_id):
        obj = self.session.scalar(select(Conversation).where(Conversation.id == conversation_id, Conversation.tenant_id == tenant_id))
        if obj is None:
            raise TenantIntegrityError("conversation does not belong to tenant")
        return obj

    def quote(self, tenant_id, quote_id):
        obj = self.session.scalar(select(Quote).where(Quote.id == quote_id, Quote.tenant_id == tenant_id))
        if obj is None:
            raise TenantIntegrityError("quote does not belong to tenant")
        return obj

    def quote_line_item(self, tenant_id, line_item_id):
        obj = self.session.scalar(select(QuoteLineItem).where(QuoteLineItem.id == line_item_id, QuoteLineItem.tenant_id == tenant_id))
        if obj is None:
            raise TenantIntegrityError("quote line item does not belong to tenant")
        return obj

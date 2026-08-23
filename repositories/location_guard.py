from sqlalchemy import select
from sqlalchemy.orm import Session

from models.core import (
    Customer, Vehicle, Booking, Conversation, Quote, QuoteLineItem
)

class LocationIntegrityError(ValueError):
    """Raised when related records do not belong to the requested location."""

class LocationGuard:
    def __init__(self, session: Session):
        self.session = session

    def customer(self, location_id, customer_id):
        obj = self.session.scalar(select(Customer).where(Customer.id == customer_id, Customer.location_id == location_id))
        if obj is None:
            raise LocationIntegrityError("customer does not belong to location")
        return obj

    def vehicle(self, location_id, vehicle_id):
        obj = self.session.scalar(select(Vehicle).where(Vehicle.id == vehicle_id, Vehicle.location_id == location_id))
        if obj is None:
            raise LocationIntegrityError("vehicle does not belong to location")
        return obj

    def booking(self, location_id, booking_id):
        obj = self.session.scalar(select(Booking).where(Booking.id == booking_id, Booking.location_id == location_id))
        if obj is None:
            raise LocationIntegrityError("booking does not belong to location")
        return obj

    def conversation(self, location_id, conversation_id):
        obj = self.session.scalar(select(Conversation).where(Conversation.id == conversation_id, Conversation.location_id == location_id))
        if obj is None:
            raise LocationIntegrityError("conversation does not belong to location")
        return obj

    def quote(self, location_id, quote_id):
        obj = self.session.scalar(select(Quote).where(Quote.id == quote_id, Quote.location_id == location_id))
        if obj is None:
            raise LocationIntegrityError("quote does not belong to location")
        return obj

    def quote_line_item(self, location_id, line_item_id):
        obj = self.session.scalar(select(QuoteLineItem).where(QuoteLineItem.id == line_item_id, QuoteLineItem.location_id == location_id))
        if obj is None:
            raise LocationIntegrityError("quote line item does not belong to location")
        return obj

from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Quote

class QuoteRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_id(self, location_id: int, quote_id: int):
        return self.session.scalar(select(Quote).where(Quote.id == quote_id, Quote.location_id == location_id))

    def create(self, location_id, customer_id, **kwargs):
        from .location_guard import LocationGuard
        LocationGuard(self.session).customer(location_id, customer_id)
        obj = Quote(location_id=location_id, customer_id=customer_id, **kwargs)
        self.session.add(obj); self.session.flush(); return obj

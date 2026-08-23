from sqlalchemy import select
from sqlalchemy.orm import Session
from models.integration_models import MetaSocialConnection

class MetaSocialConnectionRepository:
    def get_for_location(self, session: Session, location_id: int):
        return session.scalar(select(MetaSocialConnection).where(MetaSocialConnection.location_id == location_id))
    def upsert(self, session: Session, location_id: int, **values):
        connection = self.get_for_location(session, location_id)
        if connection is None:
            connection = MetaSocialConnection(location_id=location_id, page_id="", encrypted_page_access_token="")
            session.add(connection)
        allowed = {c.key for c in MetaSocialConnection.__table__.columns}
        for key, value in values.items():
            if key not in allowed: raise ValueError(f"Unknown Meta social connection field: {key}")
            setattr(connection, key, value)
        session.flush()
        return connection

from __future__ import annotations
from sqlalchemy import select
from sqlalchemy.orm import Session
from models.integration_models import MetaBusinessConnection

class MetaConnectionRepository:
    def get_for_location(self, session: Session, location_id: int) -> MetaBusinessConnection | None:
        return session.scalar(select(MetaBusinessConnection).where(MetaBusinessConnection.location_id == location_id))

    def upsert_connection(self, session: Session, location_id: int, **values) -> MetaBusinessConnection:
        connection = self.get_for_location(session, location_id)
        if connection is None:
            connection = MetaBusinessConnection(location_id=location_id)
            session.add(connection)
        allowed = {c.key for c in MetaBusinessConnection.__table__.columns}
        for key, value in values.items():
            if key not in allowed:
                raise ValueError(f"Unknown Meta connection field: {key}")
            setattr(connection, key, value)
        session.flush()
        return connection

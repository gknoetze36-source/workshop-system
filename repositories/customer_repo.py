from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Customer

class CustomerRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_id(self, location_id: int, customer_id: int):
        return self.session.scalar(select(Customer).where(Customer.id == customer_id, Customer.location_id == location_id))

    def get_by_whatsapp(self, location_id: int, whatsapp_number: str):
        return self.session.scalar(select(Customer).where(Customer.location_id == location_id, Customer.whatsapp_number == whatsapp_number))

    def create(self, location_id: int, first_name: str, last_name: str, whatsapp_number: str, **kwargs):
        customer = Customer(location_id=location_id, first_name=first_name, last_name=last_name, whatsapp_number=whatsapp_number, **kwargs)
        self.session.add(customer)
        self.session.flush()
        return customer

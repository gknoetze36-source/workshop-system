"""The master account's client list shows each workshop owner's email."""
from ai.dashboard.queries import ClientAuditQueries
from models.core import Location, Owner
from tests.unit.test_meta_webhook_phase8 import setup_session


def test_client_list_and_detail_include_owner_email():
    session = setup_session()
    loc = Location(owner=Owner(email="owner@smithsauto.co.za"), name="Smith's Auto", support_email="help@smithsauto.co.za")
    session.add(loc); session.commit()
    q = ClientAuditQueries(session)
    row = q.clients()[0]
    assert (row["owner_email"], row["support_email"]) == ("owner@smithsauto.co.za", "help@smithsauto.co.za")
    assert q.client(loc.id)["location"]["owner_email"] == "owner@smithsauto.co.za"

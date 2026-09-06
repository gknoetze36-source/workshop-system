import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.core import Base, Location, Owner
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.services.embedded_signup_service import EmbeddedSignupService

def cfg(): return MetaAuthConfig(app_id="1234567890123456",app_secret="a"*32,graph_api_version="v26.0",system_user_token="token",app_domains=("https://phanta.example",),embedded_signup_config_id="123456")
class FakeClient:
    def exchange_embedded_signup_code(self,code): assert code=="one-time-code"; return {"access_token":"secret-token","token_type":"bearer","expires_in":5184000}
def db():
    e=create_engine("sqlite:///:memory:"); Base.metadata.create_all(e); return sessionmaker(bind=e)()
def test_launch():
    s=db();t=Location(owner=Owner(), name="Workshop");s.add(t);s.commit();x=EmbeddedSignupService(cfg(),FakeClient()).begin(s,t.id);assert x.config_id=="123456"
def test_callback_persists_without_raw_token():
    s=db();t=Location(owner=Owner(), name="Workshop");s.add(t);s.commit();svc=EmbeddedSignupService(cfg(),FakeClient());x=svc.begin(s,t.id);s.commit();r=svc.complete(s,location_id=t.id,state_nonce=x.state_nonce,code="one-time-code",business_id="b",waba_id="w",phone_number_id="p");s.commit();from models.integration_models import MetaBusinessConnection,MetaSignupSession;c=s.query(MetaBusinessConnection).filter_by(location_id=t.id).one();assert c.waba_id=="w" and c.token_secret_ref!="secret-token";assert s.query(MetaSignupSession).one().status=="completed"
def test_replay_rejected():
    s=db(); t=Location(owner=Owner(), name="Workshop"); s.add(t); s.commit()
    svc=EmbeddedSignupService(cfg(), FakeClient()); x=svc.begin(s,t.id); s.commit()
    svc.complete(s, location_id=t.id, state_nonce=x.state_nonce, code="one-time-code", business_id="b", waba_id="w", phone_number_id="p"); s.commit()
    with pytest.raises(ValueError, match="already been consumed"):
        svc.complete(s, location_id=t.id, state_nonce=x.state_nonce, code="one-time-code", business_id="b", waba_id="w", phone_number_id="p")

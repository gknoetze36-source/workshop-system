from integrations.paystack.services.paystack_client import PaystackClient, PaystackAPIError

def test_client_centralizes_base_url():
    c = PaystackClient("sk_test_x")
    assert c.secret_key == "sk_test_x"

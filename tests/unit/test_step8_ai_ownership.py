import inspect

def test_openai_provider_requires_credential(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    from integrations.ai.providers.openai_provider import OpenAIProvider
    try:
        OpenAIProvider(api_key="")
    except ValueError as exc:
        assert "OPENAI_API_KEY" in str(exc)
    else:
        raise AssertionError("provider must reject missing API key")

def test_service_advisor_context_is_owner_location_scoped():
    from integrations.ai.conversations.conversation_service import AIConversationService
    source = inspect.getsource(AIConversationService._context)
    assert "Location.id == location_id" in source
    assert "Owner.id == location.owner_id" in source
    assert '"industry": location.industry' in source

def test_service_advisor_tools_are_location_scoped():
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry
    source = inspect.getsource(ServiceAdvisorToolRegistry._customer)
    assert "Customer.location_id == self.ctx.location_id" in source

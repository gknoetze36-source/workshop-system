from integrations.ai.providers.base_provider import AIRequest, AIResponse
from integrations.ai.services.ai_dispatcher import AIDispatcher
from integrations.ai.models.model_config import ModelRoute


class Fake:
    name = "openai"

    def complete(self, request):
        return AIResponse(
            text="ok",
            provider=self.name,
            model=request.model,
            input_tokens=1,
            output_tokens=2,
        )


def test_ai_dispatcher_provider_abstraction():
    d = AIDispatcher({"openai": Fake()})
    r = d.complete(
        AIRequest(messages=[{"role": "user", "content": "hi"}], model=""),
        task_type="conversation",
        route=ModelRoute("openai", "test"),
    )
    assert r.provider == "openai"
    assert r.model == "test"

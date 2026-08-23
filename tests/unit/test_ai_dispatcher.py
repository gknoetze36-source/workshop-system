from integrations.ai.models.model_config import ModelRoute
from integrations.ai.providers.base_provider import AIProvider, AIRequest, AIResponse, AIProviderError
from integrations.ai.services.ai_dispatcher import AIDispatcher, TokenBucket


class FakeProvider(AIProvider):
    name = "openai"

    def __init__(self, fail=False):
        self.fail = fail

    def complete(self, request):
        if self.fail:
            raise AIProviderError("down", retryable=True)
        return AIResponse(
            text="hello",
            provider="openai",
            model=request.model,
            input_tokens=10,
            output_tokens=5,
        )


class Repo:
    def __init__(self):
        self.rows = []

    def record(self, **kwargs):
        self.rows.append(kwargs)


def test_dispatcher_routes_and_logs():
    repo = Repo()
    d = AIDispatcher(
        {"openai": FakeProvider()},
        usage_repo=repo,
        bucket=TokenBucket(1, 100),
    )
    response = d.complete(
        AIRequest(messages=[{"role": "user", "content": "hi"}], model=""),
        task_type="conversation",
        route=ModelRoute("openai", "model-x"),
        location_id=3,
    )
    assert response.text == "hello"
    assert repo.rows[0]["provider"] == "openai"
    assert repo.rows[0]["success"] is True


def test_dispatcher_fails_without_fallback():
    d = AIDispatcher(
        {"openai": FakeProvider(True)},
        bucket=TokenBucket(10, 100),
        max_retries=0,
    )
    try:
        d.complete(
            AIRequest(messages=[{"role": "user", "content": "hi"}], model=""),
            task_type="conversation",
            route=ModelRoute("openai", "bad"),
        )
    except RuntimeError as exc:
        assert "OpenAI AI provider failed after retries" in str(exc)
    else:
        raise AssertionError("dispatcher unexpectedly succeeded")

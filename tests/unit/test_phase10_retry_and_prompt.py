from integrations.ai.models.model_config import ModelRoute
from integrations.ai.providers.base_provider import AIProvider, AIProviderError, AIRequest, AIResponse
from integrations.ai.services.ai_dispatcher import AIDispatcher, TokenBucket
from integrations.ai.prompts.prompt_registry import PromptRegistry


class FlakyProvider(AIProvider):
    name = "openai"

    def __init__(self, failures):
        self.failures = failures

    def complete(self, request):
        if self.failures:
            self.failures -= 1
            raise AIProviderError("temporary", retryable=True)
        return AIResponse(
            text="ok",
            provider="openai",
            model=request.model,
            input_tokens=100,
            output_tokens=50,
        )


class Repo:
    def __init__(self):
        self.rows = []

    def record(self, **kwargs):
        self.rows.append(kwargs)


def test_dispatcher_retries_before_returning_success():
    p = FlakyProvider(2)
    repo = Repo()
    sleeps = []
    d = AIDispatcher(
        {"openai": p},
        usage_repo=repo,
        bucket=TokenBucket(3, 100),
        max_retries=2,
        sleep=sleeps.append,
    )
    r = d.complete(
        AIRequest(messages=[{"role": "user", "content": "hi"}], model=""),
        task_type="conversation",
        route=ModelRoute("openai", "gpt-test", input_cost_per_million=1, output_cost_per_million=2),
    )
    assert r.text == "ok"
    assert sleeps == [0.25, 0.5]
    assert repo.rows[-1]["estimated_cost"] == 0.0002


def test_dispatcher_does_not_fallback_after_retries():
    primary = FlakyProvider(99)
    d = AIDispatcher(
        {"openai": primary},
        bucket=TokenBucket(10, 100),
        max_retries=1,
        sleep=lambda _: None,
    )
    try:
        d.complete(
            AIRequest(messages=[{"role": "user", "content": "hi"}], model=""),
            task_type="conversation",
            route=ModelRoute("openai", "gpt-test"),
        )
    except RuntimeError as exc:
        assert "OpenAI AI provider failed after retries" in str(exc)
    else:
        raise AssertionError("dispatcher unexpectedly succeeded")


def test_dispatcher_rejects_non_openai_route():
    d = AIDispatcher({"openai": FlakyProvider(0)}, sleep=lambda _: None)
    try:
        d.complete(
            AIRequest(messages=[{"role": "user", "content": "hi"}], model=""),
            task_type="conversation",
            route=ModelRoute("anthropic", "claude-test"),
        )
    except RuntimeError as exc:
        assert "OpenAI only" in str(exc)
    else:
        raise AssertionError("non-OpenAI route was accepted")


def test_prompt_registry_uses_active_version():
    class Repo:
        def get_active(self, key):
            return type("Row", (), {"content": "v2"})()

    assert PromptRegistry(Repo()).get("service_advisor_system") == "v2"

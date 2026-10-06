from integrations.ai.providers import AIRequest, AIResponse, ToolDefinition, OpenAIProvider


class FakeHTTP:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status_code = status
        self.text = ""

    def __call__(self, *args, **kwargs):
        class Response:
            status_code = self.status_code
            text = self.text

            def json(inner):
                return self.payload

        return Response()


def req():
    return AIRequest(
        messages=[{"role": "user", "content": "hello"}],
        model="test-model",
        tools=[ToolDefinition("lookup", "lookup data", {"type": "object", "properties": {}})],
    )


def test_openai_adapter_normalizes_function_call():
    http = FakeHTTP({
        "id": "resp_1",
        "model": "test-model",
        "output_text": "",
        "output": [
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "lookup",
                "arguments": "{}",
            }
        ],
        "usage": {"input_tokens": 3, "output_tokens": 4},
    })
    p = OpenAIProvider(api_key="x", http_request=http)
    r = p.complete(req())
    assert r.provider == "openai"
    assert r.tool_calls[0].name == "lookup"
    assert r.input_tokens == 3


def test_openai_only_marks_strict_compatible_tools_strict():
    """OpenAI rejects the whole request if a strict tool has an optional or
    free-form field -- that broke every Service Advisor reply in production."""
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry
    sent = {}

    def http(method, url, json=None, **_):
        sent.update(json)
        return FakeHTTP({"id": "r", "model": "m", "output_text": "ok", "output": [], "usage": {}})()

    tools = ServiceAdvisorToolRegistry(context=None).definitions()
    OpenAIProvider(api_key="x", http_request=http).complete(
        AIRequest(messages=[{"role": "user", "content": "hi"}], model="m", tools=tools))
    by_name = {t["name"]: t for t in sent["tools"]}
    for tool in sent["tools"]:
        if tool["strict"]:
            props = tool["parameters"].get("properties") or {}
            assert set(tool["parameters"].get("required") or []) == set(props), tool["name"]
            assert all(p.get("type") != "object" for p in props.values()), tool["name"]
    assert by_name["capture_customer_context"]["strict"] is False
    assert by_name["get_vehicle"]["strict"] is True


def test_openai_reads_reply_text_from_rest_output_items():
    """The REST API has no top-level output_text; reading only that made every
    Service Advisor reply look empty, so each one became a staff handoff."""
    http = FakeHTTP({
        "id": "resp_2", "model": "gpt-5",
        "output": [
            {"type": "reasoning", "summary": []},
            {"type": "message", "role": "assistant",
             "content": [{"type": "output_text", "text": "Thursday morning is open."}]},
        ],
        "usage": {"input_tokens": 5, "output_tokens": 6},
    })
    r = OpenAIProvider(api_key="x", http_request=http).complete(req())
    assert r.text == "Thursday morning is open."


def test_reasoning_models_get_low_effort_and_token_headroom():
    sent = {}

    def http(method, url, json=None, **_):
        sent.update(json)
        return FakeHTTP({"id": "r", "model": "m", "output": [], "usage": {}})()

    p = OpenAIProvider(api_key="x", http_request=http)
    p.complete(AIRequest(messages=[{"role": "user", "content": "hi"}], model="gpt-5", max_tokens=600))
    assert sent["reasoning"] == {"effort": "low"} and sent["max_output_tokens"] > 600
    sent.clear()
    p.complete(AIRequest(messages=[{"role": "user", "content": "hi"}], model="gpt-4.1", max_tokens=600))
    assert "reasoning" not in sent and sent["max_output_tokens"] == 600

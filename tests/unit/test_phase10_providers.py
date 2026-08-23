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

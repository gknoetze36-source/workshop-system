from integrations.ai.providers.base_provider import AIRequest, AIResponse, ToolCall, ToolDefinition


def test_normalized_ai_request_and_response_contract():
    req = AIRequest(messages=[{"role": "user", "content": "hello"}], model="test", tools=[ToolDefinition("x", "do x", {"type": "object"})])
    assert req.messages[0]["role"] == "user"
    response = AIResponse(text="ok", provider="fake", model="test", tool_calls=[ToolCall("1", "x", {})])
    assert response.tool_calls[0].name == "x"

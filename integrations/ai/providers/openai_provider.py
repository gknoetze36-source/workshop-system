from __future__ import annotations

import os
from typing import Any

import requests

from .base_provider import AIProvider, AIProviderError, AIRequest, AIResponse, ToolCall


class OpenAIProvider(AIProvider):
    name = "openai"
    default_base_url = "https://api.openai.com/v1"

    def __init__(self, api_key: str | None = None, *, base_url: str | None = None, timeout: float = 30.0, http_request=None):
        self.api_key = (api_key or os.getenv("OPENAI_API_KEY", "")).strip()
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is not configured")
        self.base_url = (base_url or os.getenv("OPENAI_BASE_URL", self.default_base_url)).rstrip("/")
        self.timeout = timeout
        self._http_request = http_request or requests.request

    def complete(self, request: AIRequest) -> AIResponse:
        body: dict[str, Any] = {"model": request.model, "input": self._input_messages(request.messages), "store": False}
        if request.system:
            body["instructions"] = request.system
        if request.temperature is not None:
            body["temperature"] = request.temperature
        if request.max_tokens is not None:
            body["max_output_tokens"] = request.max_tokens
        if request.tools:
            body["tools"] = [{"type": "function", "name": t.name, "description": t.description, "parameters": t.parameters, "strict": True} for t in request.tools]
        if request.response_schema:
            body["text"] = {"format": {"type": "json_schema", "name": "phanta_output", "strict": True, "schema": request.response_schema}}
        try:
            response = self._http_request("POST", f"{self.base_url}/responses", headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}, json=body, timeout=self.timeout)
        except requests.Timeout as exc:
            raise AIProviderError("OpenAI request timed out", retryable=True) from exc
        except requests.ConnectionError as exc:
            raise AIProviderError("OpenAI connection failed", retryable=True) from exc
        except requests.RequestException as exc:
            raise AIProviderError("OpenAI request failed", retryable=True) from exc
        return self._parse(response)


    @staticmethod
    def _input_messages(messages):
        out = []
        for m in messages:
            if m.get("role") == "tool":
                out.append({"type":"function_call_output","call_id":m.get("tool_call_id",""),"output":m.get("content","")})
            elif m.get("role") == "assistant" and m.get("tool_calls"):
                if m.get("content"):
                    out.append({"role":"assistant","content":m["content"]})
                for call in m["tool_calls"]:
                    import json
                    out.append({"type":"function_call","call_id":call.get("id",""),"name":call.get("name",""),"arguments":json.dumps(call.get("arguments") or {})})
            else:
                out.append({"role":"assistant" if m.get("role") in {"assistant","model"} else "user","content":m.get("content","")})
        return out

    def _parse(self, response) -> AIResponse:
        try:
            payload = response.json()
        except ValueError:
            payload = {"error": {"message": response.text}}
        if response.status_code >= 400:
            retryable = response.status_code == 429 or response.status_code >= 500
            raise AIProviderError(payload.get("error", {}).get("message", "OpenAI API error"), status_code=response.status_code, retryable=retryable)
        text = payload.get("output_text") or ""
        calls: list[ToolCall] = []
        for item in payload.get("output", []) or []:
            if item.get("type") == "function_call":
                args = item.get("arguments", {})
                if isinstance(args, str):
                    import json
                    args = json.loads(args or "{}")
                calls.append(ToolCall(id=str(item.get("call_id") or item.get("id") or ""), name=str(item.get("name", "")), arguments=args or {}))
        usage = payload.get("usage") or {}
        return AIResponse(text=text, provider=self.name, model=payload.get("model", ""), request_id=payload.get("id"), input_tokens=usage.get("input_tokens"), output_tokens=usage.get("output_tokens"), tool_calls=calls, raw=payload)

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator


@dataclass(slots=True)
class ToolDefinition:
    name: str
    description: str
    parameters: dict[str, Any]


@dataclass(slots=True)
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass(slots=True)
class AIRequest:
    messages: list[dict[str, Any]]
    model: str
    system: str | None = None
    tools: list[ToolDefinition] = field(default_factory=list)
    response_schema: dict[str, Any] | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AIResponse:
    text: str
    provider: str
    model: str
    request_id: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


class AIProviderError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, retryable: bool = False):
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


class AIProvider(ABC):
    name: str

    @abstractmethod
    def complete(self, request: AIRequest) -> AIResponse:
        raise NotImplementedError

    def stream(self, request: AIRequest) -> Iterator[str]:
        raise NotImplementedError(f"{self.name} does not implement streaming")

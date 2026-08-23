from __future__ import annotations

import time
import uuid
from dataclasses import replace
from threading import Lock
from typing import Callable

from integrations.ai.models.model_config import ModelRoute, get_route
from integrations.ai.providers import AIProvider, AIProviderError, AIRequest, AIResponse


class TokenBucket:
    """Small in-process limiter. One dispatcher should normally be used per worker process."""

    def __init__(self, capacity: float = 10.0, refill_per_second: float = 2.0):
        if capacity <= 0 or refill_per_second <= 0:
            raise ValueError("token bucket capacity/refill must be positive")
        self.capacity = capacity
        self.tokens = capacity
        self.refill_per_second = refill_per_second
        self.last = time.monotonic()
        self.lock = Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.refill_per_second)
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = max(0.01, (1 - self.tokens) / self.refill_per_second)
            time.sleep(wait)


class AIDispatcher:
    """Single AI choke point for OpenAI routing, retries, rate limiting and cost logging.

    Phase 10 deliberately has no provider fallback. A second provider can be
    introduced later behind the same AIProvider contract without changing
    Service Advisor business logic.
    """

    def __init__(
        self,
        providers: dict[str, AIProvider],
        *,
        usage_repo=None,
        cost_estimator: Callable[[AIResponse, ModelRoute], float] | None = None,
        bucket: TokenBucket | None = None,
        max_retries: int = 2,
        backoff_base_seconds: float = 0.25,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.providers = providers
        self.usage_repo = usage_repo
        self.cost_estimator = cost_estimator or self._default_cost
        self.bucket = bucket or TokenBucket()
        self.max_retries = max(0, int(max_retries))
        self.backoff_base_seconds = max(0.0, float(backoff_base_seconds))
        self.sleep = sleep

    @staticmethod
    def _default_cost(response: AIResponse, route: ModelRoute) -> float:
        if response.input_tokens is None or response.output_tokens is None:
            return 0.0
        return (
            (response.input_tokens / 1_000_000) * route.input_cost_per_million
            + (response.output_tokens / 1_000_000) * route.output_cost_per_million
        )

    def complete(
        self,
        request: AIRequest,
        *,
        task_type: str,
        location_id: int | None = None,
        conversation_id: int | None = None,
        route: ModelRoute | None = None,
    ) -> AIResponse:
        selected = route or get_route(task_type)

        # Phase 10 guardrail: only OpenAI is a valid configured provider.
        if selected.provider != "openai":
            raise RuntimeError(
                f"Phase 10 is configured for OpenAI only; received provider={selected.provider!r}"
            )

        provider = self.providers.get("openai")
        if provider is None:
            raise RuntimeError("OpenAI provider is not configured")

        errors: list[str] = []

        for retry_number in range(self.max_retries + 1):
            request_id = str(uuid.uuid4())
            started = time.perf_counter()
            try:
                self.bucket.acquire()
                response = provider.complete(replace(request, model=selected.model))
                latency_ms = int((time.perf_counter() - started) * 1000)
                if not response.request_id:
                    response.request_id = request_id

                self._log_usage(
                    location_id,
                    conversation_id,
                    task_type,
                    response,
                    selected,
                    latency_ms,
                    True,
                    None,
                )
                return response

            except AIProviderError as exc:
                latency_ms = int((time.perf_counter() - started) * 1000)
                errors.append(f"openai: {exc}")
                self._log_error(
                    location_id,
                    conversation_id,
                    task_type,
                    "openai",
                    selected.model,
                    request_id,
                    latency_ms,
                    str(exc),
                    retry_number,
                )
                if not exc.retryable or retry_number >= self.max_retries:
                    break
                self.sleep(self.backoff_base_seconds * (2 ** retry_number))

            except Exception as exc:
                latency_ms = int((time.perf_counter() - started) * 1000)
                errors.append("openai: unexpected provider failure")
                self._log_error(
                    location_id,
                    conversation_id,
                    task_type,
                    "openai",
                    selected.model,
                    request_id,
                    latency_ms,
                    str(exc),
                    retry_number,
                )
                if retry_number >= self.max_retries:
                    break
                self.sleep(self.backoff_base_seconds * (2 ** retry_number))

        raise RuntimeError("OpenAI AI provider failed after retries: " + " | ".join(errors))

    def _log_usage(self, location_id, conversation_id, task_type, response, route, latency_ms, success, error):
        if not self.usage_repo:
            return
        self.usage_repo.record(
            location_id=location_id,
            conversation_id=conversation_id,
            provider=response.provider,
            model=response.model,
            task_type=task_type,
            request_id=response.request_id,
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
            estimated_cost=self.cost_estimator(response, route),
            latency_ms=latency_ms,
            success=success,
            error=error,
        )

    def _log_error(
        self,
        location_id,
        conversation_id,
        task_type,
        provider,
        model,
        request_id,
        latency_ms,
        error,
        retry_number,
    ):
        if self.usage_repo:
            self.usage_repo.record(
                location_id=location_id,
                conversation_id=conversation_id,
                provider=provider,
                model=model,
                task_type=task_type,
                request_id=request_id,
                input_tokens=None,
                output_tokens=None,
                estimated_cost=None,
                latency_ms=latency_ms,
                success=False,
                error=f"retry={retry_number}: {error}",
            )

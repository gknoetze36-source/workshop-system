from __future__ import annotations

from integrations.ai.repositories.prompt_version_repo import PromptVersionRepository


class PromptRegistry:
    """Versioned prompt storage with deterministic active-version lookup."""

    def __init__(self, repo: PromptVersionRepository):
        self.repo = repo

    def get(self, prompt_key: str, default: str | None = None) -> str:
        row = self.repo.get_active(prompt_key)
        if row:
            return row.content
        if default is not None:
            return default
        raise KeyError(f"No active prompt registered for {prompt_key!r}")

    def publish(self, prompt_key: str, content: str, version: int):
        if not prompt_key.strip():
            raise ValueError("prompt_key is required")
        if not content.strip():
            raise ValueError("prompt content is required")
        if version <= 0:
            raise ValueError("prompt version must be positive")
        return self.repo.create(prompt_key, version, content, active=True)

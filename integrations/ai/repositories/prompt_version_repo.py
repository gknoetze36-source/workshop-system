from __future__ import annotations

from models.integration_models import PromptVersion


class PromptVersionRepository:
    def __init__(self, session):
        self.session = session

    def get_active(self, prompt_key: str):
        return (self.session.query(PromptVersion).filter(PromptVersion.prompt_key == prompt_key, PromptVersion.active.is_(True)).order_by(PromptVersion.version.desc()).first())

    def create(self, prompt_key: str, version: int, content: str, active: bool = False):
        if active:
            self.session.query(PromptVersion).filter(PromptVersion.prompt_key == prompt_key).update({PromptVersion.active: False})
        row = PromptVersion(prompt_key=prompt_key, version=version, content=content, active=active)
        self.session.add(row)
        self.session.flush()
        return row

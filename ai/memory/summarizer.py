from __future__ import annotations

class ConversationSummarizer:
    def __init__(self, conversation_service):
        self.conversation_service = conversation_service

    def close(self, **kwargs):
        return self.conversation_service.close_and_summarize(**kwargs)

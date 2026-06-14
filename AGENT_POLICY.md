# Odysseus Agent Policy (Workshop System)

## Core Rule
All AI-generated changes must be safe, incremental, and reversible.

## Allowed Actions
- Create new files
- Modify existing files
- Refactor code in small increments
- Add logging, validation, and tests
- Improve documentation

## Forbidden Actions
- Deleting core modules without explicit instruction
- Large-scale rewrites of multiple systems at once
- Removing security or auth logic
- Modifying production deployment configs without review

## Change Process
1. Plan change before editing files
2. Apply minimal safe edits
3. Validate logic (lint/test if available)
4. Summarize changes
5. Wait for approval before commit (optional mode)

## Safety Mode
If uncertainty is high:
- STOP
- Ask for clarification
- Do not modify code

## Memory Rule
Always align changes with:
- VANTA_OWNER_PLAYBOOK.md
- SYSTEM_ARCHITECTURE.md
- SECURITY_AUDIT.md
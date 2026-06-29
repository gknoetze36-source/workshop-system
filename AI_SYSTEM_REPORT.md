# AI System Audit Report

**Generated**: 2026-06-21  
**Scope**: AI workflows, memory systems, agent orchestration, MCP integrations, automation orchestration  
**Based on**: Review of codebase, automation engine playbook, system architecture, and MCP server implementation.

---

## 1. AI Workflows

### Findings
- **Primary AI component**: `ai_engine.py` provides intent classification (`booking`, `pricing`, `repair`, `chat`) using:
  - Rule‑based keyword matching (fast, no cost)
  - Fallback to OpenAI GPT‑4o‑mini via `_classify_with_ai` when `ENABLE_AI_INTENT_CLASSIFICATION=true`
  - LRU cache (`maxsize=512`) to reduce duplicate API calls
- Classification is used by the MCP server (`classify_message` tool) and potentially elsewhere (imported in `workshop_mcp_server.py`).
- No evidence of model fine‑tuning, prompt versioning, or A/B testing.
- No alternative local/self‑hosted models are integrated.

### Risks
- **Cost**: Each uncached classification incurs an OpenAI API call (GPT‑4o‑mini pricing applies). Under high volume, costs can grow.
- **Reliability**: Dependence on external OpenAI API introduces latency and failure points; no circuit breaker or graceful degradation beyond keyword fallback.
- **Scalability**: LRU cache is in‑process; multiple worker instances do not share cache, leading to redundant calls.
- **Observability**: No logging/metrics on classification outcomes, latency, or cache hit rate.

### Recommendations
- **Improve cache efficiency**: 
  - Promote cache to a shared layer (e.g., Redis) so all workers/web processes benefit.
  - Increase `maxsize` or implement a TTL‑based cache.
- **Introduce a local fallback model**: 
  - Train a lightweight classifier (e.g., logistic regression on TF‑IDF or a small transformer) using existing labelled data; use it as primary, OpenAI as fallback for edge cases.
  - This reduces API dependency and latency.
- **Add metrics and alerting**:
  - Emit counters for cache hits/misses, API call latency, and error rates to monitoring (e.g., via `communication_logs` or a dedicated `ai_metrics` table).
  - Set alerts on error rate or latency spikes.
- **Implement prompt/version management**:
  - Store prompts in version‑controlled files or database; allow rolling back if a new prompt degrades accuracy.
- **Consider using Anthropic’s models** (if cost/latency preferable) via a unified LLM abstraction layer.

---

## 2. Memory Systems

### Findings
- The codebase does **not** contain a persistent memory or context store for AI agents or conversations.
- Short‑term caching exists only in the AI engine (LRU cache) and standard DB‑based usage tables (`usage_daily`, `chatbot_usage_monthly`).
- No mechanisms for storing customer‑specific preferences, conversation history, or long‑term context that could improve personalization or agent behavior.

### Risks
- **Missed personalization opportunities**: Without memory, each interaction is stateless; cannot recall past issues, preferences, or service history.
- **Agent effectiveness limited**: Any future AI agents (e.g., a booking concierge) would lack contextual memory, reducing their utility.
- **Data silos**: Usage metrics are stored but not linked to individual customer profiles for behavioral insights.

### Recommendations
- **Add a customer interaction memory table**:
  - Columns: `customer_id`, `franchise_id`, `last_interaction_at`, `interaction_history` (JSONB), `preferences` (JSONB), `summary_text`.
  - Update on each inbound/outbound message via a lightweight process or trigger.
- **Expose memory via MCP**:
  - Add MCP tools to retrieve/update customer memory, enabling external agents to leverage context.
- **Leverage memory for AI workflows**:
  - Use recent interaction history as additional context for intent classification (e.g., if a customer repeatedly asks about pricing, bias toward `pricing`).
  - Store classification outcomes to improve self‑supervised learning over time.
- **Implement data retention policy**:
  - Archive or anonymize old interaction data to comply with privacy regulations.

---

## 3. Agent Orchestration

### Findings
- No dedicated agent orchestration framework is present.
- `agent_loop.py` is a trivial example and not integrated.
- Orchestration of work is currently handled by:
  - **Automation Engine** (event → job → worker)
  - **Scheduler** (time‑based cron jobs)
  - **MCP Server** (on‑demand tool access for external clients)
- Agents (if defined) would need to be built ad‑hoc, lacking standard lifecycle management (registration, discovery, supervision).

### Risks
- **Ad‑hoc agent development** leads to duplicated boilerplate (message loops, error handling, state persistence).
- **Hard to observe or debug** agent behavior without a common framework.
- **Scaling challenges**: custom agents may improperly share resources (e.g., DB connections) or conflict with existing workers.

### Recommendations
- **Adopt a lightweight agent framework** (e.g., Python‑based with async message queues) or formalize agents as:
  - **Event‑driven workers** subscribing to specific automation event types (similar to existing `automation_rules`).
  - **Long‑running processes** that pull tasks from a dedicated `agent_tasks` table.
- **Provide common building blocks**:
  - Base class for agents with lifecycle methods (`initialize`, `process_task`, `shutdown`).
  - Standardized logging, metrics, and error reporting.
  - Optional integration with the customer memory table for context.
- **Register agents in the MCP server**:
  - Expose agent‑control tools (start/stop/status) and allow agents to register capabilities via MCP.
- **Use the existing automation infrastructure** where possible:
  - Reuse `scheduled_jobs` and `automation_worker` for agent task execution, adding an `agent_type` column to differentiate.

---

## 4. MCP Integrations

### Findings
- A fully featured MCP server (`workshop_mcp_server.py`) is implemented, exposing five tools:
  1. `booking_availability`
  2. `customer_search`
  3. `get_billing_info`
  4. `classify_message` (AI workflow)
  5. `get_dashboard_stats`
- The server follows MCP stdio transport, can be invoked by any MCP‑compatible client (e.g., Claude Desktop, custom agents).
- It is **not** currently used internally; external clients must launch it as a subprocess.
- No authentication/authorization is built into the MCP server (relies on underlying DB credentials via the workshop system’s `initialize_database`).

### Risks
- **Surface area**: Direct DB access via MCP tools could expose sensitive data if invoked by untrusted clients.
- **Operational overhead**: Running an extra subprocess for each external client may consume resources; no connection pooling.
- **Missing tool coverage**: Several domain actions (e.g., create booking, update franchise settings) are not exposed, limiting MCP utility.
- **No versioning**: Tool interfaces are not versioned; changes could break existing clients.

### Recommendations
- **Add simple authentication** to the MCP server:
  - Require a bearer token (e.g., `FRONTEND_API_TOKEN` or a dedicated MCP token) validated at the start of each session.
  - Alternatively, rely on the OS environment (stdio) and restrict execution to trusted users; document this limitation.
- **Expand tool set** to cover core mutative operations safely:
  - `create_booking` (with validation)
  - `update_customer_preferences`
  - `trigger_automation_rule` (manual execution)
  - Ensure each tool performs appropriate authorization checks (franchise/scoping).
- **Implement tool versioning** via MCP’s `protover` or by namespacing tools (e.g., `booking_availability_v2`).
- **Consider exposing the MCP server as a long‑running service** (e.g., via HTTP/SSE) rather than stdio per‑invocation, to reduce startup overhead and enable connection pooling.
- **Log MCP invocations** (tool name, args, duration, errors) to an `mcp_logs` table for audit and debugging.

---

## 5. Automation Orchestration

*(Note: This area was already audited in the automation engine playbook; highlights are included for completeness.)*

### Findings
- Event‑driven automation via `automation_engine.emit_event()` → `scheduled_jobs` → `automation_worker`.
- Supports retry with exponential backoff, dead‑letter (`failed_jobs`), and admin retry endpoints.
- Scheduler (`scheduler.py`) runs time‑based jobs every 5 minutes (inquiry followups) and at fixed times for reminders.
- Duplicate prevention is partial: `scheduled_jobs` lacks a unique constraint on `(event_type, payload_key)`; duplicate rules can create duplicate jobs.
- Observability relies on polling tables (`scheduled_jobs`, `automation_logs`, `failed_jobs`) and service logs.

### Risks
- **Polling inefficiency**: Workers sleep between intervals, adding latency; database polling under load can affect performance.
- **Duplicate jobs**: Missing uniqueness can cause redundant message sends and increased cost.
- **Limited visibility**: No real‑time dashboards for job throughput, latency, or failure patterns.
- **Worker crash recovery**: Jobs marked `running` may be stranded if the worker crashes before updating status.

### Recommendations
- **Replace polling with a push‑based queue**:
  - Use PostgreSQL `LISTEN/NOTIFY` or a lightweight broker (Redis) to signal new jobs, reducing latency and DB load.
- **Add unique constraint** on `scheduled_jobs` for `(event_type, customer_id, franchise_id)` (or a hash of the payload) to prevent duplicates at insert time.
- **Enhance observability**:
  - Emit metrics to a monitoring system (e.g., Prometheus) via an `automation_metrics` table or direct exposition.
  - Add a `/automation/health` endpoint that reports worker lag, queue depth, and failure rates.
- **Implement worker heartbeat**:
  - Workers periodically update a `worker_heartbeat` record; on startup, detect stale `running` jobs and reset them to `pending`.
- **Consider dead‑letter queues** for repeatedly failing jobs beyond max attempts, with segregated handling.

---

## 6. Cross‑Cutting Recommendations for Reliability, Cost, and Simplicity

### Reliability
- **Circuit breaker** for external API calls (OpenAI, Meta, Paystack) to prevent cascading failures.
- **Idempotency keys** for all outbound actions (message sending, payment creation) to safely retry.
- **Database connection pooling** tuned for worker/web concurrency (currently max 5; consider increasing based on load).
- **Automated failover** for services (web, worker, scheduler) via Railway’s restart policies; add health checks (`/health/db`, `/health/worker`).

### Cost Reduction
- **Cache AI classifications** aggressively (shared cache, local model) to cut OpenAI API spend.
- **Batch metadata updates**: Instead of writing a row per message for `communication_logs`, consider batching or asynchronous writing for high‑volume traffic.
- **Optimize SQL queries**:
  - Ensure indexes on frequently queried columns (`scheduled_jobs.status`, `scheduled_jobs.scheduled_for`, `communication_logs.recipient`).
  - Review `platform_helpers.ensure_service` for N+1 query patterns; adopt UPSERT.
- **Right‑size worker concurrency**: Adjust `AUTOMATION_WORKER_BATCH_SIZE` and interval based on actual job volume to avoid over‑provisioning.

### Simplification
- **Unify AI routing**: Move all AI classification logic into a single service module with clear interfaces (rule, cache, model) to ease testing and replacement.
- **Deprecate unused Blueprints**: Remove empty `routes/` package or replace with actual Blueprint‑based routing to reduce confusion.
- **Centralize configuration**: Group feature flags (`ENABLE_AI_INTENT_CLASSIFICATION`, OpenAI model choice, cache TTL) in a single config object or `.env` section with documentation.
- **Standardize error handling**: Define a custom exception hierarchy and middleware to return consistent JSON error responses across HTTP and MCP.

---

## Conclusion
The workshop system possesses a functional foundation for AI‑enabled workflows (classification, MCP exposure) and a robust automation engine. The primary opportunities lie in:
1. **Making AI cheaper and more reliable** via caching, local fallbacks, and shared infra.
2. **Adding memory/context** to enable personalized, stateful agents.
3. **Formalizing agent orchestration** to leverage existing automation primitives while providing a clean development model.
4. **Hardening and expanding the MCP server** to become a secure, versatile integration point for both internal and external consumers.
5. **Refining automation orchestration** with push‑based scheduling, deduplication, and improved observability.

Addressing these areas will increase system reliability, reduce operating costs, and simplify future extension—positioning the workshop system to take full advantage of AI‑driven automation.

---
*Prepared for handoff to business‑analyst.*
# DeerFlow Full Architecture Guide

A comprehensive architecture reference for the DeerFlow system, covering every layer from network routing to agent internals.

---

## Table of Contents

- [1. System Overview](#1-system-overview)
- [2. Code Architecture (Harness / App Split)](#2-code-architecture-harness--app-split)
- [3. Configuration System](#3-configuration-system)
- [4. Agent Internals](#4-agent-internals)
- [5. Middleware Chain (15 Stages)](#5-middleware-chain-15-stages)
- [6. Data Flow: Complete Message Lifecycle](#6-data-flow-complete-message-lifecycle)
- [7. The Six Core Subsystems](#7-the-six-core-subsystems)
  - [7a. Sandbox System](#7a-sandbox-system)
  - [7b. Subagent System](#7b-subagent-system)
  - [7c. Memory System](#7c-memory-system)
  - [7d. Skills System](#7d-skills-system)
  - [7e. MCP Integration](#7e-mcp-integration)
  - [7f. IM Channels](#7f-im-channels)
- [8. Checkpointer (State Persistence)](#8-checkpointer-state-persistence)
- [9. Frontend Modes](#9-frontend-modes)
- [10. Service Startup](#10-service-startup)
- [11. Security Model](#11-security-model)
- [12. Key Design Patterns](#12-key-design-patterns)

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER                                           │
│                    Browser / Feishu / Slack / Telegram                       │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         NGINX  (port 2026)                                  │
│  Entry point for everything. CORS centralized here.                         │
│                                                                             │
│  /api/langgraph/*  ──→  LangGraph Server :2024  (SSE, buffering OFF)       │
│  /api/*            ──→  Gateway API :8001                                   │
│  /api/sandboxes    ──→  Provisioner :8002  (optional, K8s only)             │
│  /*                ──→  Frontend :3000                                       │
└───────┬──────────────────┬──────────────────┬───────────────────────────────┘
        │                  │                  │
        ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────────┐  ┌─────────────────────────────────────┐
│  Frontend    │  │  Gateway API     │  │  LangGraph Server                   │
│  Next.js     │  │  FastAPI         │  │  Agent Runtime                      │
│  port 3000   │  │  port 8001       │  │  port 2024                          │
│              │  │                  │  │                                     │
│  React 19    │  │  /api/models     │  │  make_lead_agent(config)            │
│  TypeScript  │  │  /api/mcp        │  │  ├─ Model (LLM)                    │
│  Tailwind 4  │  │  /api/skills     │  │  ├─ Tools (sandbox/MCP/built-in)   │
│  LangGraph   │  │  /api/memory     │  │  ├─ 15 Middlewares                  │
│  SDK         │  │  /api/agents     │  │  ├─ System Prompt                   │
│              │  │  /api/uploads    │  │  └─ Checkpointer                    │
│  useStream() │  │  /api/artifacts  │  │                                     │
│  ──SSE──→    │  │  /api/threads    │  │  Streams ThreadState via SSE        │
│              │  │  /api/channels   │  │                                     │
└──────────────┘  │  /api/suggestions│  └────────────────┬────────────────────┘
                  │  /health         │                   │
                  └────────┬─────────┘                   │
                           │                             │
                  ┌────────▼─────────┐          ┌────────▼────────┐
                  │  IM Channels     │          │  Sandbox        │
                  │  (optional)      │          │                 │
                  │  Feishu          │          │  LocalSandbox   │
                  │  Slack           │          │  (subprocess)   │
                  │  Telegram        │          │       OR        │
                  │                  │          │  AioSandbox     │
                  │  MessageBus      │          │  (Docker/K8s)   │
                  │  ChannelManager  │          │                 │
                  │  ChannelStore    │          │  Virtual paths: │
                  └──────────────────┘          │  /mnt/user-data │
                                                │  /mnt/skills    │
                                                └─────────────────┘
```

---

## 2. Code Architecture (Harness / App Split)

```
backend/
├── packages/harness/deerflow/    ←── HARNESS (publishable package: deerflow-harness)
│   │                                  Import as: deerflow.*
│   │                                  NEVER imports from app.*
│   │
│   ├── agents/
│   │   ├── lead_agent/
│   │   │   ├── agent.py          ←── make_lead_agent() — the factory
│   │   │   └── prompt.py         ←── System prompt assembly
│   │   ├── middlewares/          ←── 15 middleware components
│   │   ├── memory/               ←── Memory extraction + storage
│   │   ├── thread_state.py       ←── ThreadState schema
│   │   └── checkpointer/         ←── State persistence (memory/sqlite/postgres)
│   │
│   ├── sandbox/
│   │   ├── tools.py              ←── bash, ls, read_file, write_file, str_replace
│   │   ├── sandbox.py            ←── Abstract interface
│   │   ├── local/                ←── LocalSandbox (subprocess execution)
│   │   └── middleware.py         ←── Sandbox lifecycle management
│   │
│   ├── subagents/
│   │   ├── executor.py           ←── Dual thread pool execution engine
│   │   ├── builtins/             ←── general-purpose + bash agents
│   │   └── registry.py           ←── Agent lookup
│   │
│   ├── tools/builtins/           ←── present_files, ask_clarification, view_image,
│   │                                  task, setup_agent
│   ├── models/factory.py         ←── create_chat_model() with reflection
│   ├── skills/                   ←── Skill discovery + parsing
│   ├── mcp/                      ←── MCP tool loading (stdio/SSE/HTTP + OAuth)
│   ├── config/                   ←── Config system (app, model, sandbox, skills,
│   │                                  memory, extensions)
│   ├── community/                ←── Community tools (tavily, jina, firecrawl,
│   │                                  aio_sandbox)
│   └── reflection/               ←── Dynamic module loading (resolve_class,
│                                      resolve_variable)
│
├── app/                          ←── APP (unpublished, internal only)
│   │                                  Import as: app.*
│   │                                  CAN import from deerflow.*
│   │
│   ├── gateway/
│   │   ├── app.py                ←── FastAPI setup + lifespan
│   │   └── routers/              ←── 10 REST API routers
│   │
│   └── channels/
│       ├── manager.py            ←── ChannelManager (dispatch loop)
│       ├── message_bus.py        ←── Pub/sub hub
│       ├── store.py              ←── channel:chat_id → thread_id mapping
│       ├── feishu.py             ←── Feishu/Lark (streaming card updates)
│       ├── slack.py              ←── Slack Socket Mode
│       └── telegram.py           ←── Telegram Bot API
│
├── tests/                        ←── Test suite
└── langgraph.json                ←── Entry point: "lead_agent" →
                                       deerflow.agents:make_lead_agent

frontend/
├── src/
│   ├── app/workspace/chats/[thread_id]/  ←── Chat page
│   ├── core/
│   │   ├── threads/hooks.ts      ←── useThreadStream, useSubmitThread
│   │   ├── api/api-client.ts     ←── LangGraph SDK singleton
│   │   ├── messages/utils.ts     ←── groupMessages, extractContent
│   │   ├── skills/               ←── Skills CRUD hooks
│   │   ├── memory/               ←── Memory hooks
│   │   └── settings/             ←── User preferences (localStorage)
│   └── components/
│       ├── workspace/            ←── Chat components
│       └── ui/                   ←── Shadcn primitives (auto-generated)

skills/
├── public/                       ←── 17 official skills (committed)
│   ├── bootstrap/                ←── Agent creation onboarding
│   ├── data-analysis/            ←── CSV/Excel analysis
│   └── ...
└── custom/                       ←── User-installed skills (gitignored)
```

**Boundary rule**: `deerflow.*` never imports `app.*`. Enforced by `tests/test_harness_boundary.py` in CI.

---

## 3. Configuration System

```
deer-flow/                         ←── Project root (recommended config location)
├── config.yaml                    ←── Main app config
├── extensions_config.json         ←── MCP servers + skills enabled state
├── .env                           ←── API keys + secrets
│
└── backend/.deer-flow/            ←── Runtime data (auto-created)
    ├── memory.json                ←── Global memory
    ├── USER.md                    ←── Optional user profile
    ├── agents/
    │   └── {agent_name}/
    │       ├── config.yaml        ←── Custom agent config
    │       ├── SOUL.md            ←── Agent personality
    │       └── memory.json        ←── Per-agent memory
    └── threads/
        └── {thread_id}/
            └── user-data/
                ├── workspace/     ←── Agent working directory
                ├── uploads/       ←── User uploaded files
                └── outputs/       ←── Agent output files
```

### config.yaml structure

```yaml
config_version: 3
log_level: info

models:
  - name: gpt-4
    use: langchain_openai:ChatOpenAI        # Reflection-based class loading
    model: gpt-4
    api_key: $OPENAI_API_KEY                # Env var resolution
    supports_thinking: true
    supports_vision: true

tools:
  - name: web_search
    group: web
    use: deerflow.community.tavily.tools:web_search_tool

sandbox:
  use: deerflow.sandbox.local:LocalSandboxProvider    # Dev
  # use: deerflow.community.aio_sandbox:AioSandboxProvider  # Production

skills:
  container_path: /mnt/skills

memory:
  enabled: true
  debounce_seconds: 30
  max_facts: 100
  max_injection_tokens: 2000

checkpointer:
  type: sqlite                              # memory | sqlite | postgres
  connection_string: checkpoints.db

channels:
  langgraph_url: http://localhost:2024
  feishu: { enabled: true, app_id: $FEISHU_APP_ID, ... }
  slack: { enabled: false, ... }
  telegram: { enabled: false, ... }
```

### Config resolution priority

| Config File | Priority |
|-------------|----------|
| `$DEER_FLOW_CONFIG_PATH` env var | 1st |
| `config.yaml` in cwd (backend/) | 2nd |
| `config.yaml` in parent (project root) | 3rd |

**Auto-reload**: `get_app_config()` caches config but detects mtime changes — no restart needed.

### Extensions configuration (extensions_config.json)

```json
{
  "mcpServers": {
    "example-server": {
      "enabled": true,
      "type": "stdio",
      "command": "python",
      "args": ["-m", "example_module"]
    }
  },
  "skills": {
    "data-analysis": { "enabled": true },
    "bootstrap": { "enabled": false }
  }
}
```

Resolution: `$DEER_FLOW_EXTENSIONS_CONFIG_PATH` → cwd → parent dir. Always read fresh (not cached) for cross-process consistency between Gateway and LangGraph.

---

## 4. Agent Internals

### Agent creation (per-request)

```
make_lead_agent(config)
    │
    ├─ Extract runtime context from frontend:
    │   thinking_enabled, is_plan_mode, subagent_enabled,
    │   model_name, reasoning_effort, agent_name, is_bootstrap
    │
    ├─ Load custom agent (if agent_name):
    │   load_agent_config() → config.yaml (model, tool_groups)
    │   load_agent_soul()   → SOUL.md (personality)
    │
    ├─ Create LLM:
    │   create_chat_model(name, thinking_enabled, reasoning_effort)
    │   └─ resolve_class("langchain_openai:ChatOpenAI") → instantiate
    │
    ├─ Load tools:
    │   get_available_tools(model_name, groups, subagent_enabled)
    │   ├─ Config tools (web_search, etc.) — filtered by tool_groups
    │   ├─ Built-in (present_files, ask_clarification)
    │   ├─ Sandbox (bash, ls, read_file, write_file, str_replace)
    │   ├─ MCP tools (lazy-loaded from extensions_config.json)
    │   ├─ view_image (if model supports vision)
    │   └─ task (if subagent_enabled)
    │
    ├─ Build middleware chain (strict order):
    │   [see §5 below]
    │
    ├─ Assemble system prompt:
    │   apply_prompt_template()
    │   ├─ <role> section
    │   ├─ <soul> SOUL.md content (if custom agent)
    │   ├─ <memory> injected facts + context
    │   ├─ <thinking_style> reasoning guidelines
    │   ├─ <skill_system> available skills with /mnt/skills paths
    │   ├─ <subagents> delegation instructions (if enabled)
    │   └─ <response_style> formatting rules
    │
    └─ return create_agent(model, tools, middlewares, prompt, ThreadState)
```

### ThreadState schema

```python
class ThreadState(AgentState):
    messages: Annotated[list[Message], add_messages]           # Conversation history
    sandbox: NotRequired[SandboxState | None]                  # Sandbox ID
    thread_data: NotRequired[ThreadDataState | None]           # Directory paths
    title: NotRequired[str | None]                             # Auto-generated title
    artifacts: Annotated[list[str], merge_artifacts]           # Output files (deduplicated)
    todos: NotRequired[list | None]                            # Task list (plan mode)
    uploaded_files: NotRequired[list[dict] | None]             # File metadata
    viewed_images: Annotated[dict[str, ViewedImageData], merge_viewed_images]
```

### Model resolution priority

```
1. Frontend request (config.configurable.model_name)
2. Custom agent override (agent_config.model)
3. Global default (first model in config.yaml)
```

---

## 5. Middleware Chain (15 Stages)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  MIDDLEWARE CHAIN — executes in strict order per agent invocation            │
│                                                                              │
│  Hook types:                                                                 │
│  • before_agent  — runs before LLM call                                     │
│  • after_model   — runs after LLM responds, before tools execute            │
│  • wrap_tool_call — wraps around each tool execution                        │
│  • after_agent   — runs after agent turn completes                          │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ Runtime Middlewares (always present) ─────────────────────────────────┐  │
│  │  1. ThreadDataMiddleware      before_agent: create thread directories  │  │
│  │  2. UploadsMiddleware         before_agent: inject <uploaded_files>     │  │
│  │  3. SandboxMiddleware         before_agent: acquire sandbox            │  │
│  │  4. DanglingToolCallMiddleware before_agent: patch missing tool results│  │
│  │  5. GuardrailMiddleware       wrap_tool_call: authorize (if configured)│  │
│  │  6. ToolErrorHandlingMiddleware wrap_tool_call: catch exceptions       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌─ Conditional Middlewares ──────────────────────────────────────────────┐  │
│  │  7. SummarizationMiddleware   before_agent: reduce context (optional)  │  │
│  │  8. TodoListMiddleware        (if is_plan_mode): add write_todos tool  │  │
│  │  9. TokenUsageMiddleware      (if enabled): track consumption          │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌─ Always-present Middlewares ───────────────────────────────────────────┐  │
│  │ 10. TitleMiddleware           after_agent: auto-generate thread title  │  │
│  │ 11. MemoryMiddleware          after_agent: queue for async extraction  │  │
│  │ 12. ViewImageMiddleware       before_agent: inject base64 (if vision)  │  │
│  │ 13. DeferredToolFilterMiddleware (if tool_search): hide deferred tools │  │
│  │ 14. SubagentLimitMiddleware   after_model: cap task calls (if ultra)   │  │
│  │ 15. LoopDetectionMiddleware   after_model: detect infinite loops       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌─ MUST BE LAST ────────────────────────────────────────────────────────┐  │
│  │ 16. ClarificationMiddleware   wrap_tool_call: Command(goto=END)       │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Data Flow: Complete Message Lifecycle

```
USER types "Analyze my CSV" + attaches file
  │
  ▼ ① UPLOAD
  POST /api/threads/{id}/uploads → nginx → Gateway:8001
  → Saves to ~/.deer-flow/threads/{id}/user-data/uploads/data.csv
  → Returns virtual path: /mnt/user-data/uploads/data.csv
  │
  ▼ ② OPTIMISTIC UI
  Frontend immediately displays user message (no server wait)
  │
  ▼ ③ SUBMIT
  thread.submit({messages: [{type: "human", content: "Analyze..."}]}, {
    context: { thinking_enabled, is_plan_mode, subagent_enabled, thread_id }
  })
  → POST /api/langgraph/threads/{id}/runs (SSE connection opened)
  │
  ▼ ④ NGINX
  Rewrite /api/langgraph/* → /*
  proxy_pass → langgraph:2024
  proxy_buffering OFF (critical for SSE streaming)
  │
  ▼ ⑤ LANGGRAPH SERVER
  Invoke make_lead_agent(config)
  → Create model + tools + 15 middlewares + system prompt
  │
  ▼ ⑥ MIDDLEWARE: before_agent (top to bottom)
  ThreadData → create dirs
  Uploads → inject file list into message
  Sandbox → acquire (id: "local")
  │
  ▼ ⑦ AGENT LOOP
  ┌─────────────────────────────────────────────────┐
  │  LLM receives: system prompt + memory + messages │
  │  LLM responds: "Let me read data.csv"            │
  │    tool_call: read_file("/mnt/user-data/...")     │
  │          ↓                                        │
  │  Tool executes: validate → translate path → read  │
  │    returns: file contents                         │
  │          ↓                                        │
  │  LLM responds: "I see 3 columns..."              │
  │    tool_call: bash("python3 -c '...'")            │
  │          ↓                                        │
  │  Tool executes: validate → translate → subprocess │
  │    returns: analysis results                      │
  │          ↓                                        │
  │  LLM responds: final markdown with insights       │
  │  (no more tool calls → loop ends)                 │
  └─────────────────────────────────────────────────┘
  │
  ▼ ⑧ MIDDLEWARE: after_agent (bottom to top)
  Title → LLM generates "CSV Revenue Analysis"
  Memory → queue conversation (fires 30s later)
  Sandbox → release to warm pool
  │
  ▼ ⑨ SSE STREAMING (throughout steps ⑥-⑧)
  ┌──────────────────────────────────────────────────┐
  │ data: {"event":"values","data":{messages:[...]}} │ ← State snapshots
  │ data: {"event":"messages-tuple","data":[...]}    │ ← Incremental text
  │ data: {"event":"custom","data":{type:"task_*"}}  │ ← Subagent progress
  │ data: {"event":"end"}                            │ ← Done
  └──────────────────────────────────────────────────┘
  │
  ▼ ⑩ FRONTEND RECEIVES
  useStream callbacks:
    onUpdateEvent → update title in React Query cache
    onCustomEvent → update subagent progress UI
    onFinish → invalidate thread list query
  │
  ▼ ⑪ OPTIMISTIC → REAL
  thread.messages.length increased → clear optimistic messages
  │
  ▼ ⑫ RENDER
  groupMessages() → [human] [processing: thinking+tools] [assistant: response]
  extractContent() → markdown with charts/tables displayed
```

---

## 7. The Six Core Subsystems

### 7a. Sandbox System

```
┌──────────────────────────────────────────────────────────────┐
│  Agent sees:                  Host filesystem:               │
│  /mnt/user-data/workspace/ →  ~/.deer-flow/threads/{id}/...  │
│  /mnt/user-data/uploads/   →  ~/.deer-flow/threads/{id}/...  │
│  /mnt/user-data/outputs/   →  ~/.deer-flow/threads/{id}/...  │
│  /mnt/skills/              →  deer-flow/skills/ (read-only)  │
└──────────────────────────────────────────────────────────────┘

LocalSandbox (dev):     subprocess.run(), singleton, 10-min timeout
AioSandbox (prod):      Docker container per thread, HTTP API
                        Warm pool, deterministic IDs, idle timeout
                        Cross-process discovery via file locks
```

**5 sandbox tools**: `bash`, `ls`, `read_file`, `write_file`, `str_replace`

**Path translation**: bidirectional — virtual paths in commands translated to host paths for execution, host paths in output masked back to virtual paths.

**Security**: path traversal rejection, post-resolution symlink check, skills read-only enforcement.

### 7b. Subagent System

```
Lead agent calls task("Research AWS pricing", subagent_type="general-purpose")
    │
    ├─ Scheduler pool (3 workers) → submits to execution pool
    ├─ Execution pool (3 workers) → runs subagent with own agent loop
    ├─ Same sandbox + filesystem, isolated conversation context
    ├─ disallowed_tools=["task"] → no recursion
    ├─ SSE events: task_started → task_running → task_completed
    └─ 15-min timeout, SubagentLimitMiddleware caps at 3 concurrent
```

**Built-in agents**: `general-purpose` (all tools except task/clarification/present_files, max 50 turns) and `bash` (sandbox tools only, max 30 turns).

**Subagents always run with `thinking_enabled=False`** — faster and cheaper for focused work.

### 7c. Memory System

```
after_agent → filter messages → queue → debounce 30s → LLM extraction
    │
    ▼
memory.json: { user: {workContext, topOfMind}, history: {...}, facts: [...] }
    │
    ▼ (next conversation)
format_memory_for_injection(max_tokens=2000) → <memory> tag in system prompt
```

**Fact extraction**: LLM outputs structured JSON with categories (preference/knowledge/context/behavior/goal) and confidence scores (0.7 threshold).

**Safety**: 4-layer upload stripping, atomic file writes (temp → rename), mtime-based cache invalidation.

### 7d. Skills System

```
skills/public/data-analysis/SKILL.md
    │
    ├─ Layer 1 (always in prompt): name + description (~50 tokens)
    ├─ Layer 2 (on-demand): read_file("/mnt/skills/...") → full instructions
    └─ Layer 3 (when executing): scripts, templates, references
```

**Discovery**: `load_skills()` recursively scans `skills/{public,custom}/` for SKILL.md files, parses YAML frontmatter, reads enabled state from `extensions_config.json`.

**Installation**: agent creates `.skill` ZIP archive → user downloads → `POST /api/skills/install` → validates → copies to `skills/custom/` → available next message, no restart needed.

### 7e. MCP Integration

```
extensions_config.json → build_servers_config() → MultiServerMCPClient
    │
    ├─ stdio transport: spawns process (command + args)
    ├─ SSE transport: connects to URL with optional OAuth
    ├─ HTTP transport: connects to URL with optional OAuth
    │
    └─ get_cached_mcp_tools() → lazy load, mtime-based cache invalidation
```

**Tool search**: when `tool_search.enabled`, MCP tools are registered in a deferred registry and loaded on-demand via the `tool_search` tool, instead of being directly bound to the model.

### 7f. IM Channels

```
Feishu/Slack/Telegram → Channel.start() → MessageBus.publish_inbound()
    │
    ▼
ChannelManager._dispatch_loop()
    ├─ Commands (/help, /new, /status, /bootstrap) → handle locally
    └─ Chat messages → look up thread → client.runs.stream() or .wait()
        │
        ▼
    Response + artifacts → OutboundMessage → Channel.send()
```

**Session mapping**: `ChannelStore` persists `{channel:chat_id:topic_id} → thread_id` in JSON file.

**Streaming**: Feishu patches a running card in-place for incremental updates; Slack/Telegram wait for final response.

**Artifact delivery**: virtual paths validated to resolve inside `sandbox_outputs_dir` before sending files to platforms.

---

## 8. Checkpointer (State Persistence)

```
┌──────────────────────────────────────────────────────────────┐
│  Checkpointer — persists ThreadState between turns           │
│                                                              │
│  memory    → InMemorySaver (testing, single process)         │
│  sqlite    → SqliteSaver (file-based, single process)        │
│  postgres  → PostgresSaver (multi-process, cloud-ready)      │
│                                                              │
│  Config: checkpointer.type + connection_string               │
│  Lazy init: created on first use                             │
│  Enables: conversation resumption, streamResumable           │
└──────────────────────────────────────────────────────────────┘
```

---

## 9. Frontend Modes

```
┌─────────────────────────────────────────────────────────────────┐
│  Mode      │ thinking │ plan_mode │ subagents │ Use Case        │
├────────────┼──────────┼───────────┼───────────┼─────────────────┤
│  Flash     │    ✗     │     ✗     │     ✗     │ Quick answers   │
│  Pro       │    ✓     │     ✓     │     ✗     │ Complex tasks   │
│  Ultra     │    ✓     │     ✓     │     ✓     │ Multi-step work │
└─────────────────────────────────────────────────────────────────┘
```

Frontend sends these as `context` object in `thread.submit()` → backend reads via `config.get("configurable", {})` to configure the agent per-request.

---

## 10. Service Startup

### Local development (no Docker)

```
make dev
  └─ serve.sh --dev
      ├─ LangGraph server on :2024  (langgraph dev --allow-blocking)
      ├─ Gateway API on :8001       (uvicorn app.gateway.app:app --reload)
      ├─ Frontend on :3000          (pnpm dev, Turbopack)
      └─ Nginx on :2026             (reverse proxy)
```

### Docker development

```
make docker-start
  └─ docker-compose-dev.yaml
      ├─ nginx :2026 (depends_on all)
      ├─ frontend :3000 (hot-reload, source mounted)
      ├─ gateway :8001 (--reload, source mounted)
      ├─ langgraph :2024 (source mounted)
      └─ provisioner :8002 (optional, for AioSandbox K8s)
```

### Production Docker

```
make up
  └─ docker-compose.yaml
      ├─ nginx :2026
      ├─ frontend :3000 (multi-stage build, production optimized)
      ├─ gateway :8001 (no reload, workers enabled)
      ├─ langgraph :2024 (production mode)
      └─ provisioner :8002 (optional)
```

### Gateway startup sequence

1. Load config via `get_app_config()` (validates all sections)
2. Load gateway config via `get_gateway_config()`
3. Start channel service (if any channels enabled in config)
4. Ready to accept HTTP requests

### LangGraph startup sequence

1. Load config via `get_app_config()`
2. Initialize checkpointer (if configured)
3. Register `make_lead_agent` graph from `langgraph.json`
4. Ready for stream requests on port 2024

---

## 11. Security Model

| Layer | Protection |
|-------|-----------|
| **Virtual paths** | Agent sees `/mnt/user-data`, never real host paths |
| **Path validation** | Reject `..` traversal, verify resolved paths stay in sandbox |
| **Output masking** | Real paths in stderr/stdout replaced with virtual paths |
| **Skills read-only** | `/mnt/skills` write attempts → PermissionError |
| **Artifact security** | IM channels validate paths resolve inside `sandbox_outputs_dir` |
| **Config secrets** | `$ENV_VAR` resolution, `.env` files never committed |
| **Harness boundary** | `deerflow.*` never imports `app.*`, enforced by CI test |
| **Skill installation** | ZIP validation: no traversal, no symlinks, 512MB limit, frontmatter whitelist |
| **Docker isolation** | AioSandbox: per-thread containers with volume mounts |
| **CORS** | Centralized in nginx, not per-service |

---

## 12. Key Design Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| **Factory per-request** | `make_lead_agent()` | Fresh agent each time, no stale state |
| **Reflection-based loading** | `resolve_class("module:Class")` | Config-driven LLM/tool selection |
| **Lazy initialization** | Sandbox, MCP tools, checkpointer | Resources created only when needed |
| **mtime cache invalidation** | Config, extensions, memory | Auto-detect file changes without restart |
| **Debounce queue** | Memory system | Batch expensive LLM calls (30s timer) |
| **Atomic file writes** | Memory, config updates | temp → rename, no corruption on crash |
| **Deterministic IDs** | AioSandbox | `sha256(thread_id)[:8]` → cross-process container sharing |
| **Warm pool** | AioSandbox containers | Released containers kept running, fast re-acquire |
| **Progressive disclosure** | Skills | ~50 tokens in prompt, full content loaded on-demand |
| **Optimistic UI** | Frontend | Messages shown instantly, replaced when server responds |
| **SSE streaming** | LangGraph → nginx → frontend | Real-time updates, no polling |
| **Pub/sub decoupling** | IM channels | MessageBus decouples platforms from agent dispatch |
| **One-way dependency** | Harness / App split | Harness publishable, app internal-only |

---

## Key Files Reference

### Backend — Agent System
- `backend/langgraph.json` — Entry point config
- `backend/packages/harness/deerflow/agents/lead_agent/agent.py` — Agent factory
- `backend/packages/harness/deerflow/agents/lead_agent/prompt.py` — System prompt
- `backend/packages/harness/deerflow/agents/thread_state.py` — State schema
- `backend/packages/harness/deerflow/agents/middlewares/` — All middleware files
- `backend/packages/harness/deerflow/agents/checkpointer/` — State persistence

### Backend — Sandbox
- `backend/packages/harness/deerflow/sandbox/tools.py` — 5 sandbox tools
- `backend/packages/harness/deerflow/sandbox/local/` — LocalSandbox
- `backend/packages/harness/deerflow/community/aio_sandbox/` — AioSandbox
- `backend/packages/harness/deerflow/config/paths.py` — Path singleton

### Backend — Subagents
- `backend/packages/harness/deerflow/subagents/executor.py` — Execution engine
- `backend/packages/harness/deerflow/subagents/builtins/` — Built-in configs
- `backend/packages/harness/deerflow/tools/builtins/task_tool.py` — task() tool

### Backend — Memory
- `backend/packages/harness/deerflow/agents/memory/updater.py` — LLM extraction
- `backend/packages/harness/deerflow/agents/memory/queue.py` — Debounce queue
- `backend/packages/harness/deerflow/agents/memory/prompt.py` — LLM prompts

### Backend — Skills
- `backend/packages/harness/deerflow/skills/` — Discovery + parsing
- `backend/app/gateway/routers/skills.py` — Gateway REST endpoints
- `skills/` — Actual SKILL.md definitions

### Backend — MCP
- `backend/packages/harness/deerflow/mcp/tools.py` — Tool loading
- `backend/packages/harness/deerflow/mcp/client.py` — Server config builder
- `backend/packages/harness/deerflow/mcp/oauth.py` — OAuth token management

### Backend — Channels
- `backend/app/channels/manager.py` — ChannelManager dispatch
- `backend/app/channels/message_bus.py` — Pub/sub hub
- `backend/app/channels/store.py` — Session mapping
- `backend/app/channels/feishu.py` — Feishu integration
- `backend/app/channels/slack.py` — Slack integration
- `backend/app/channels/telegram.py` — Telegram integration

### Backend — Configuration
- `config.yaml` — Main app config
- `extensions_config.json` — MCP servers and skills state
- `backend/packages/harness/deerflow/config/app_config.py` — Config system
- `backend/packages/harness/deerflow/config/extensions_config.py` — Extensions config

### Backend — Gateway
- `backend/app/gateway/app.py` — FastAPI setup + lifespan
- `backend/app/gateway/routers/` — 10 REST API routers

### Frontend
- `frontend/src/core/threads/hooks.ts` — Thread hooks (primary API)
- `frontend/src/core/api/api-client.ts` — LangGraph SDK singleton
- `frontend/src/core/messages/utils.ts` — Message grouping + extraction
- `frontend/src/app/workspace/chats/[thread_id]/page.tsx` — Chat page
- `frontend/src/env.js` — Environment validation

### Infrastructure
- `docker/nginx/nginx.conf` — Nginx routing + CORS + SSE config
- `docker-compose.yaml` — Production orchestration
- `docker-compose-dev.yaml` — Development orchestration
- `Makefile` — Root commands (dev, install, check, up, stop)

# DeerFlow Learning Guide

A step-by-step deep dive into the DeerFlow codebase, covering architecture, agent system, subagents, memory, sandbox, and skills.

---

## Table of Contents

- [Step 1: What DeerFlow Is](#step-1-what-deerflow-is)
- [Step 2: Architecture Deep Dive](#step-2-architecture-deep-dive)
  - [The 10,000-Foot View](#the-10000-foot-view)
  - [Why Two Backend Services?](#why-two-backend-services)
  - [The Life of a Message](#the-life-of-a-message)
  - [The Harness / App Boundary](#the-harness--app-boundary)
  - [The Sandbox: Agent's Computer](#the-sandbox-agents-computer)
  - [Frontend: A Thin Streaming Client](#frontend-a-thin-streaming-client)
  - [Configuration: Three Files That Matter](#configuration-three-files-that-matter)
- [Step 3: The Agent Code](#step-3-the-agent-code)
  - [How the Pieces Connect](#how-the-pieces-connect)
  - [langgraph.json — The Entry Point](#langgraphjson--the-entry-point)
  - [make_lead_agent() — The Factory Function](#make_lead_agent--the-factory-function)
  - [create_chat_model() — The LLM Factory](#create_chat_model--the-llm-factory)
  - [get_available_tools() — The Toolbox](#get_available_tools--the-toolbox)
  - [ThreadState — What the Agent Remembers](#threadstate--what-the-agent-remembers)
  - [_build_middlewares() — The Pipeline](#_build_middlewares--the-pipeline)
  - [apply_prompt_template() — The System Prompt](#apply_prompt_template--the-system-prompt)
  - [How a Middleware Actually Works](#how-a-middleware-actually-works)
- [Step 4: The Subagent System](#step-4-the-subagent-system)
  - [What Problem It Solves](#what-problem-subagents-solve)
  - [The Components](#subagent-components)
  - [SubagentConfig — What Defines a Subagent](#subagentconfig--what-defines-a-subagent)
  - [The Two Built-in Subagents](#the-two-built-in-subagents)
  - [task_tool() — The Bridge](#task_tool--the-bridge)
  - [SubagentExecutor — The Engine](#subagentexecutor--the-engine)
  - [SubagentLimitMiddleware — The Safety Net](#subagentlimitmiddleware--the-safety-net)
  - [Complete Timeline](#subagent-complete-timeline)
  - [Key Design Insights](#subagent-key-design-insights)
- [Step 5: The Memory System](#step-5-the-memory-system)
  - [What Problem It Solves](#what-problem-memory-solves)
  - [The Four Components](#memory-components)
  - [The Complete Lifecycle](#memory-lifecycle)
  - [MemoryMiddleware — Capturing Conversations](#memorymiddleware--capturing-conversations)
  - [MemoryUpdateQueue — Debouncing](#memoryupdatequeue--debouncing)
  - [MemoryUpdater — The LLM-Powered Extraction Engine](#memoryupdater--the-llm-powered-extraction-engine)
  - [memory.json — The Data Structure](#memoryjson--the-data-structure)
  - [Injection — How Memory Gets Into the Prompt](#injection--how-memory-gets-into-the-prompt)
  - [Safety Mechanisms](#memory-safety-mechanisms)
  - [Configuration](#memory-configuration)
- [Step 6: The Sandbox System](#step-6-the-sandbox-system)
  - [The Big Picture](#sandbox-big-picture)
  - [Virtual Path Translation](#virtual-path-translation)
  - [Security Model](#sandbox-security-model)
  - [The Two Sandbox Implementations](#the-two-sandbox-implementations)
  - [Container Lifecycle (AioSandbox)](#container-lifecycle-aiosandbox)
  - [The 5 Sandbox Tools](#the-5-sandbox-tools)
  - [Lazy Initialization Flow](#lazy-initialization-flow)
  - [Key Design Insights](#sandbox-key-design-insights)
- [Step 7: The Skills System](#step-7-the-skills-system)
  - [Progressive Disclosure](#progressive-disclosure)
  - [Skill Discovery Pipeline](#skill-discovery-pipeline)
  - [SKILL.md Anatomy](#skillmd-anatomy)
  - [How Skills Enter the Prompt](#how-skills-enter-the-prompt)
  - [Runtime Flow — Agent Using a Skill](#runtime-flow--agent-using-a-skill)
  - [Configuration & State](#skills-configuration--state)
  - [Skill Installation Workflow](#skill-installation-workflow)
  - [Gateway API Endpoints](#skills-gateway-api-endpoints)
  - [Frontend Integration](#skills-frontend-integration)
  - [Key Design Insights](#skills-key-design-insights)
- [Suggested Learning Schedule](#suggested-learning-schedule)
- [Key Files Reference](#key-files-reference)

---

## Step 1: What DeerFlow Is

DeerFlow (**D**eep **E**xploration and **E**fficient **R**esearch **Flow**) is an open-source super agent harness — think of it as an "operating system" for AI agents. It gives AI agents:

- A **sandbox** (their own computer to run code)
- **Memory** (remembers you across sessions)
- **Sub-agents** (can spawn helpers for parallel work)
- **Skills** (extensible capabilities via Markdown files)
- **Tools** (web search, file ops, bash, MCP servers)

**Stack**: Python 3.12+ (backend), Next.js 16 / React 19 / TypeScript 5.8 (frontend), Nginx reverse proxy, Docker/K8s sandbox.

Built by ByteDance, it hit #1 on GitHub Trending in Feb 2026. DeerFlow 2.0 is a ground-up rewrite sharing no code with v1.

---

## Step 2: Architecture Deep Dive

### The 10,000-Foot View

DeerFlow has **4 processes** running simultaneously, stitched together by Nginx:

```
┌─────────────────────────────────────────────────────────────────┐
│                     YOUR BROWSER                                │
│        http://localhost:2026  (everything goes here)            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    NGINX (port 2026)                             │
│  The "traffic cop" — looks at the URL path and routes:          │
│                                                                  │
│  /api/langgraph/*  ──→  LangGraph Server (2024) [Agent brain]   │
│  /api/models       ──→  Gateway API (8001)      [REST services] │
│  /api/memory       ──→  Gateway API (8001)                      │
│  /api/skills       ──→  Gateway API (8001)                      │
│  /api/threads/*/uploads → Gateway API (8001)                    │
│  /* (everything else) → Frontend (3000)         [UI]            │
└──────────────────────────────────────────────────────────────────┘
```

| Process | Port | Technology | Job |
|---------|------|------------|-----|
| **Nginx** | 2026 | C/nginx | Routes traffic, handles CORS, enables SSE streaming |
| **LangGraph Server** | 2024 | Python/LangGraph | Runs AI agents, manages conversations, executes tools |
| **Gateway API** | 8001 | Python/FastAPI | REST API for config, file uploads, memory, skills |
| **Frontend** | 3000 | Node/Next.js | Chat UI, renders messages, handles user input |

### Why Two Backend Services?

The backend is split because **agent execution** and **management APIs** have very different needs:

**LangGraph Server** (port 2024):
- Long-running (a conversation can take minutes)
- Stateful (tracks thread state across turns)
- Streaming (sends Server-Sent Events as the agent thinks)
- Runs the agent + middleware chain + tools

**Gateway API** (port 8001):
- Quick request/response (milliseconds)
- Stateless REST endpoints
- Manages configuration, file uploads, memory, skills
- Also starts IM channels (Telegram, Slack, Feishu)

Both share the same `deerflow` harness package — they import the same config, models, and tools code.

### The Life of a Message

Here's exactly what happens when you type "Research quantum computing" and press Enter:

#### Phase 1: Frontend → Nginx → LangGraph

```
1. You type message in browser
2. React hook sendMessage() fires
3. If you attached files:
   POST /api/threads/{id}/uploads → Nginx → Gateway (8001)
   Files stored in backend/.deer-flow/threads/{id}/user-data/uploads/
4. LangGraph SDK sends:
   POST /api/langgraph/threads/{id}/runs/stream
   Body: { messages: [...], config: { thinking_enabled, model_name, ... } }
5. Nginx rewrites /api/langgraph/* → /* and proxies to port 2024
   (with SSE streaming enabled, 10-minute timeout)
```

#### Phase 2: Agent Initialization

```
6. LangGraph Server calls make_lead_agent(config)
7. Agent factory resolves:
   - Which LLM model to use (from config.configurable.model_name)
   - Which tools to load (sandbox + MCP + built-in + community + subagent)
   - System prompt (with skills, memory, and instructions)
8. Builds 12 middlewares in strict order
9. Returns a fully configured LangGraph StateGraph
```

#### Phase 3: Middleware Chain (Before LLM Call)

Each middleware gets a chance to modify state before the LLM sees it:

```
10. ThreadDataMiddleware      → Creates per-thread directories
11. UploadsMiddleware         → Injects uploaded file info into messages
12. SandboxMiddleware         → Acquires sandbox for code execution
13. DanglingToolCallMiddleware → Patches interrupted tool calls
14. GuardrailMiddleware       → Checks tool call authorization (optional)
15. SummarizationMiddleware   → Summarizes older messages if context too long (optional)
16. TodoListMiddleware        → Adds task tracking tool (plan mode only)
17. TitleMiddleware           → Auto-generates thread title
18. MemoryMiddleware          → Queues conversation for memory extraction
19. ViewImageMiddleware       → Converts images to base64 (vision models only)
20. SubagentLimitMiddleware   → Enforces max 3 concurrent sub-agents (if enabled)
21. ClarificationMiddleware   → Intercepts "ask user" requests (must be last)
```

#### Phase 4: LLM Call + Tool Execution

```
22. System prompt assembled (skills + memory + instructions)
23. LLM generates response (streaming tokens back)
24. If LLM calls a tool (e.g., bash, web_search):
    → Tool executes in sandbox → Result feeds back to LLM
    → LLM continues thinking... (loop until done)
25. If LLM spawns sub-agents:
    → task() tool creates background threads (max 3)
    → Results flow back as custom SSE events
```

#### Phase 5: Streaming Back to Browser

```
26. LangGraph streams SSE events:
    - "messages-tuple" → each token/message as generated
    - "values" → full state snapshot (title, artifacts, todos)
    - "end" → stream complete
27. Nginx passes through (buffering disabled for SSE)
28. Frontend useStream() hook receives events
29. React re-renders with each update
30. User sees response appearing word by word
```

### The Harness / App Boundary

The backend has a **strict import firewall**:

```
┌─────────────────────────────────────────────────────┐
│  HARNESS (packages/harness/deerflow/)               │
│  The publishable, reusable core.                    │
│  Import: deerflow.*                                 │
│                                                      │
│  • agents/     (lead agent, middleware, memory)      │
│  • sandbox/    (execution environments)              │
│  • tools/      (built-in tools)                      │
│  • models/     (LLM factory)                         │
│  • mcp/        (MCP integration)                     │
│  • skills/     (skill loader)                        │
│  • config/     (configuration system)                │
│  • client.py   (embedded Python client)              │
│                                                      │
│  ❌ CANNOT import from app.*                         │
│     (enforced by test_harness_boundary.py in CI)     │
└────────────────────────┬────────────────────────────┘
                         │ imports ↓ (one-way only)
┌────────────────────────▼────────────────────────────┐
│  APP (app/)                                          │
│  The application layer. Not published.               │
│  Import: app.*                                       │
│                                                      │
│  • gateway/    (FastAPI REST API)                    │
│  • channels/   (Telegram, Slack, Feishu)             │
│                                                      │
│  ✅ CAN import from deerflow.*                       │
└─────────────────────────────────────────────────────┘
```

**Why?** So `deerflow` can be published as a standalone Python package (`deerflow-harness` on PyPI). You could build your own app layer on top of it.

### The Sandbox: Agent's Computer

The agent doesn't just generate text — it has a **virtual filesystem**:

```
What the agent sees:              What's actually on disk:
─────────────────────             ──────────────────────
/mnt/user-data/workspace/    →    backend/.deer-flow/threads/{id}/user-data/workspace/
/mnt/user-data/uploads/      →    backend/.deer-flow/threads/{id}/user-data/uploads/
/mnt/user-data/outputs/      →    backend/.deer-flow/threads/{id}/user-data/outputs/
/mnt/skills/public/          →    skills/public/
/mnt/skills/custom/          →    skills/custom/
```

The agent can run `bash` commands, read/write files, and execute code — all within this isolated space. In Docker mode, this runs in an actual container. In local mode, path translation handles the mapping.

### Frontend: A Thin Streaming Client

The frontend's core is a wrapper around the LangGraph SDK:

```
User types message
    ↓
useThreadStream() hook
    ↓
thread.submit(message, config)    ← sends to backend
    ↓
LangGraph SDK handles:
  - SSE connection management
  - Event multiplexing (messages, state, errors)
  - Auto-reconnection
    ↓
thread.messages  ← real-time array of messages
thread.values    ← current state (title, todos, artifacts)
thread.isLoading ← is agent still thinking?
    ↓
React components subscribe and re-render
```

The frontend sends **context** with every message that controls agent behavior:

| Frontend Mode | `thinking_enabled` | `is_plan_mode` | `subagent_enabled` |
|---|---|---|---|
| Flash ⚡ | `False` | `False` | `False` |
| Standard | `True` | `False` | `False` |
| Pro 🧠 | `True` | `True` | `False` |
| Ultra 🚀 | `True` | `True` | `True` |

### Configuration: Three Files That Matter

```
config.yaml                    ← The brain's settings
├── models[] ─────────────────── Which LLMs to use (GPT, Claude, Gemini, etc.)
├── tools[] ──────────────────── Available tools + class paths
├── sandbox.use ──────────────── Local vs Docker vs K8s sandbox
├── memory ───────────────────── Memory system (enabled, debounce, limits)
├── summarization ────────────── Context compression settings
├── channels ─────────────────── IM integrations (Telegram, Slack, Feishu)
└── subagents.enabled ────────── Allow sub-agent delegation

extensions_config.json         ← Extensions state
├── mcpServers{} ─────────────── MCP server configs (enabled, URL, auth)
└── skills{} ─────────────────── Skill enabled/disabled state

.env                           ← Secrets
├── OPENAI_API_KEY
├── TAVILY_API_KEY
└── (other provider keys)
```

Config values starting with `$` are resolved as environment variables at runtime (e.g., `api_key: $OPENAI_API_KEY`).

---

## Step 3: The Agent Code

### How the Pieces Connect

```
langgraph.json                    ← "When someone talks to me, call this function"
    │
    ▼
make_lead_agent(config)           ← The factory that BUILDS the agent
    │
    ├── create_chat_model()       ← Pick and configure the LLM
    ├── get_available_tools()     ← Assemble the toolbox
    ├── _build_middlewares()      ← Build the processing pipeline
    ├── apply_prompt_template()   ← Generate the system prompt
    └── ThreadState              ← Define what the agent remembers
    │
    ▼
create_agent(model, tools, middleware, system_prompt, state_schema)
    │
    ▼
A fully configured LangGraph Agent  ← Ready to process messages
```

### langgraph.json — The Entry Point

```json
{
  "graphs": {
    "lead_agent": "deerflow.agents:make_lead_agent"
  },
  "checkpointer": {
    "path": "./packages/harness/deerflow/agents/checkpointer/async_provider.py:make_checkpointer"
  }
}
```

- **`lead_agent`** → The agent ID the frontend sends messages to (`assistantId: "lead_agent"`)
- **`deerflow.agents:make_lead_agent`** → When a message arrives, LangGraph calls this Python function
- **`checkpointer`** → Saves conversation state between turns

**Key insight**: `make_lead_agent` is called **on every new run** (every message). The agent is rebuilt fresh each time. This is why runtime configuration works — it's read anew per request.

### make_lead_agent() — The Factory Function

**File**: `backend/packages/harness/deerflow/agents/lead_agent/agent.py`

#### Step 1: Extract Runtime Settings

```python
cfg = config.get("configurable", {})

thinking_enabled = cfg.get("thinking_enabled", True)
reasoning_effort = cfg.get("reasoning_effort", None)
requested_model_name = cfg.get("model_name") or cfg.get("model")
is_plan_mode = cfg.get("is_plan_mode", False)
subagent_enabled = cfg.get("subagent_enabled", False)
```

#### Step 2: Resolve the Model

Three-level fallback:
1. What the user requested in the UI → `requested_model_name`
2. What's configured for this agent → `agent_model_name`
3. First model in `config.yaml` → default

If the user asks for a model that doesn't exist, it logs a warning and falls back silently.

#### Step 3: Build the Agent

```python
return create_agent(
    model=create_chat_model(name=model_name, thinking_enabled=thinking_enabled),
    tools=get_available_tools(model_name=model_name, subagent_enabled=subagent_enabled),
    middleware=_build_middlewares(config, model_name=model_name),
    system_prompt=apply_prompt_template(subagent_enabled=subagent_enabled, ...),
    state_schema=ThreadState,
)
```

### create_chat_model() — The LLM Factory

**File**: `backend/packages/harness/deerflow/models/factory.py`

```python
def create_chat_model(name=None, thinking_enabled=False, **kwargs):
    config = get_app_config()
    model_config = config.get_model_config(name)

    # Use reflection to instantiate the right class
    model_class = resolve_class(model_config.use, BaseChatModel)
    # e.g., "langchain_openai:ChatOpenAI" → imports ChatOpenAI from langchain_openai
```

**What `resolve_class` does**: Takes a string like `"langchain_openai:ChatOpenAI"` from your `config.yaml` and dynamically imports that Python class. This is how DeerFlow supports any LLM provider.

**Thinking mode handling**:
- If thinking IS enabled and model supports it → Apply `when_thinking_enabled` overrides
- If thinking is NOT enabled but model has thinking settings → Explicitly disable thinking

### get_available_tools() — The Toolbox

**File**: `backend/packages/harness/deerflow/tools/tools.py`

Assembles tools from **four sources**:

```
1. CONFIG TOOLS (from config.yaml)
   resolved via reflection: "deerflow.community.tavily:..."
   → web_search, web_fetch, etc.

2. BUILT-IN TOOLS (always present)
   → present_file   (show output files to user)
   → ask_clarification (ask user a question)
   + view_image     (if model supports vision)
   + task           (if subagent_enabled)

3. MCP TOOLS (from extensions_config.json)
   → Any tools from connected MCP servers
   → Lazy loaded, cached with mtime invalidation

4. SANDBOX TOOLS (loaded by SandboxMiddleware)
   → bash, ls, read_file, write_file, str_replace
   (added by the middleware, not here)
```

Conditional logic:
- `view_image` only added if model has `supports_vision: true`
- `task` (subagent tool) only added if `subagent_enabled`
- MCP tools read fresh from disk each time

### ThreadState — What the Agent Remembers

**File**: `backend/packages/harness/deerflow/agents/thread_state.py`

```python
class ThreadState(AgentState):
    sandbox: SandboxState | None           # {sandbox_id: "local" or container ID}
    thread_data: ThreadDataState | None    # {workspace_path, uploads_path, outputs_path}
    title: str | None                      # Auto-generated conversation title
    artifacts: Annotated[list[str], merge_artifacts]     # Output files (deduped)
    todos: list | None                     # Task list (plan mode)
    uploaded_files: list[dict] | None      # Files the user uploaded
    viewed_images: Annotated[dict, merge_viewed_images]  # Base64 image cache
```

The `Annotated` trick uses custom **reducers** to merge state updates:
- `merge_artifacts`: Deduplicates — same artifact path won't appear twice
- `merge_viewed_images`: Empty dict `{}` = CLEAR all images (used after LLM has seen them)

### _build_middlewares() — The Pipeline

**File**: `backend/packages/harness/deerflow/agents/lead_agent/agent.py`

```python
def _build_middlewares(config, model_name, agent_name=None):
    # Phase 1: Base runtime middlewares (always present)
    middlewares = build_lead_runtime_middlewares(lazy_init=True)
    # → [ThreadData, Uploads, Sandbox, DanglingToolCall, Guardrail, ToolErrorHandling]

    # Phase 2: Conditional middlewares
    if summarization_enabled:  middlewares.append(SummarizationMiddleware(...))
    if is_plan_mode:           middlewares.append(TodoMiddleware(...))
    if token_usage_enabled:    middlewares.append(TokenUsageMiddleware())

    # Phase 3: Always-on middlewares
    middlewares.append(TitleMiddleware())
    middlewares.append(MemoryMiddleware())
    if model_supports_vision:  middlewares.append(ViewImageMiddleware())
    if subagent_enabled:       middlewares.append(SubagentLimitMiddleware(max_concurrent=3))
    middlewares.append(LoopDetectionMiddleware())

    # MUST BE LAST
    middlewares.append(ClarificationMiddleware())
    return middlewares
```

### apply_prompt_template() — The System Prompt

**File**: `backend/packages/harness/deerflow/agents/lead_agent/prompt.py`

The system prompt is assembled from multiple sections:

```
<role>You are DeerFlow 2.0, an open-source super agent.</role>
{soul}              ← Custom agent personality (from SOUL.md)
{memory_context}    ← "You know the user prefers Python, works at..."
<thinking_style>    ← How to approach problems
<clarification_system>  ← When and how to ask questions
{skills_section}    ← "You have skills: research, slides, web-page..."
{subagent_section}  ← "You can spawn sub-agents (max 3 per turn)..."
<working_directory> ← "Your files are at /mnt/user-data/..."
<response_style>    ← "Be clear and concise..."
<citations>         ← "Always cite sources..."
<critical_reminders>← "Clarify first, load skills, output to /outputs..."
<current_date>2026-03-26, Wednesday</current_date>
```

Skills are injected as XML with paths — the agent uses `read_file` to load SKILL.md **only when needed** (progressive loading).

### How a Middleware Actually Works

#### Example A: `ThreadDataMiddleware` (Simple — `before_agent`)

```python
class ThreadDataMiddleware(AgentMiddleware):
    def before_agent(self, state, runtime):
        thread_id = runtime.context.get("thread_id")
        paths = {
            "workspace_path": f".deer-flow/threads/{thread_id}/user-data/workspace",
            "uploads_path":   f".deer-flow/threads/{thread_id}/user-data/uploads",
            "outputs_path":   f".deer-flow/threads/{thread_id}/user-data/outputs",
        }
        return {"thread_data": paths}  # Merged into ThreadState
```

Pattern: `before_agent` runs before the LLM call. Returns a dict that merges into state.

#### Example B: `ClarificationMiddleware` (Advanced — `wrap_tool_call`)

```python
class ClarificationMiddleware(AgentMiddleware):
    def wrap_tool_call(self, request, handler):
        if request.tool_call.get("name") != "ask_clarification":
            return handler(request)  # Let other tools run normally

        args = request.tool_call.get("args", {})
        formatted = f"❓ {args.get('question', '')}"

        return Command(
            update={"messages": [ToolMessage(content=formatted, ...)]},
            goto=END,  # STOPS execution!
        )
```

Pattern: `wrap_tool_call` wraps every tool execution. It can pass through, replace the result, or **stop execution entirely** with `Command(goto=END)`.

#### Example C: `ToolErrorHandlingMiddleware` (Resilience)

```python
class ToolErrorHandlingMiddleware(AgentMiddleware):
    def wrap_tool_call(self, request, handler):
        try:
            return handler(request)
        except GraphBubbleUp:
            raise  # Let LangGraph control signals through
        except Exception as exc:
            return ToolMessage(
                content=f"Error: Tool '{name}' failed: {detail}. Continue with available context.",
                status="error",
            )
```

Without this, a tool crash kills the conversation. With it, the LLM gets an error message and can try a different approach.

---

## Step 4: The Subagent System

### What Problem Subagents Solve

Without subagents, asking "Compare AWS, Azure, and GCP" means:
1. Research AWS → stuff results into context
2. Research Azure → context getting huge
3. Research GCP → context overflowing, early details lost

With subagents, the lead agent becomes an **orchestrator**:
1. Spawn 3 subagents **in parallel**, each researching one provider
2. Each subagent has its **own clean context** — no interference
3. Results flow back → lead agent synthesizes everything

```
┌──────────────────────────────────────────────────────────┐
│                    LEAD AGENT                             │
│  "Compare AWS, Azure, and GCP"                           │
│  Calls: task(), task(), task()  ← 3 parallel tool calls │
└────────┬──────────────┬──────────────┬───────────────────┘
         │              │              │
         ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ Subagent    │ │ Subagent    │ │ Subagent    │  Independent agents
│ (AWS)       │ │ (Azure)     │ │ (GCP)       │  in background threads
│ Own context │ │ Own context │ │ Own context │  shared filesystem
└─────────────┘ └─────────────┘ └─────────────┘
```

### Subagent Components

Six files make this work:

| File | Role |
|------|------|
| `config.py` | Data class: what defines a subagent |
| `builtins/general_purpose.py` | Config for the "general-purpose" subagent |
| `builtins/bash_agent.py` | Config for the "bash" subagent |
| `registry.py` | Lookup: name → config |
| `executor.py` | The engine: creates agents, runs them in thread pools |
| `tools/builtins/task_tool.py` | The tool the lead agent calls to spawn subagents |

### SubagentConfig — What Defines a Subagent

**File**: `backend/packages/harness/deerflow/subagents/config.py`

```python
@dataclass
class SubagentConfig:
    name: str                              # "general-purpose" or "bash"
    description: str                       # When to use this subagent
    system_prompt: str                     # Instructions for the subagent
    tools: list[str] | None = None         # None = inherit ALL tools from parent
    disallowed_tools: list[str] = ["task"] # Can't spawn sub-sub-agents!
    model: str = "inherit"                 # Use parent's model
    max_turns: int = 50                    # Max LLM calls before stopping
    timeout_seconds: int = 900             # 15-minute hard timeout
```

Two critical design decisions:
1. **`disallowed_tools: ["task"]`** — Prevents infinite recursion (no sub-sub-agents)
2. **`model: "inherit"`** — Subagents use the same LLM as the parent

### The Two Built-in Subagents

#### `general-purpose` — The All-Rounder

```python
GENERAL_PURPOSE_CONFIG = SubagentConfig(
    name="general-purpose",
    tools=None,              # Gets ALL tools (web search, file ops, etc.)
    disallowed_tools=["task", "ask_clarification", "present_files"],
    max_turns=50,
)
```

- Gets all tools except `task` (no recursion), `ask_clarification` (can't pause for user input in background), `present_files` (only lead agent shows files)
- System prompt: "Complete the task autonomously, do NOT ask for clarification"

#### `bash` — The Command Specialist

```python
BASH_AGENT_CONFIG = SubagentConfig(
    name="bash",
    tools=["bash", "ls", "read_file", "write_file", "str_replace"],  # Sandbox only!
    disallowed_tools=["task", "ask_clarification", "present_files"],
    max_turns=30,
)
```

Only gets sandbox tools. No web search, no MCP. Focused command runner for git, npm, docker, build tasks.

### task_tool() — The Bridge

**File**: `backend/packages/harness/deerflow/tools/builtins/task_tool.py`

This is what the lead agent actually calls to spawn subagents:

#### Step 1: Look up config
```python
config = get_subagent_config(subagent_type)  # "general-purpose" or "bash"
```

#### Step 2: Inject skills
```python
skills_section = get_skills_prompt_section()
if skills_section:
    config.system_prompt += "\n\n" + skills_section
```

#### Step 3: Inherit parent context
```python
sandbox_state = runtime.state.get("sandbox")     # Same sandbox
thread_data = runtime.state.get("thread_data")     # Same file paths
thread_id = runtime.context.get("thread_id")       # Same thread
parent_model = metadata.get("model_name")          # Same LLM
```

Subagents share the **same sandbox and filesystem** but get **isolated conversation context**.

#### Step 4: Get tools without `task`
```python
tools = get_available_tools(model_name=parent_model, subagent_enabled=False)
#                                                     ^^^^^^^^^^^^^^^^
#                                          Prevents sub-sub-agents
```

#### Step 5: Launch and poll
```python
task_id = executor.execute_async(prompt, task_id=tool_call_id)

writer = get_stream_writer()
writer({"type": "task_started", "task_id": task_id, "description": description})

while True:
    result = get_background_task_result(task_id)

    # Stream intermediate progress
    if new_messages:
        writer({"type": "task_running", "task_id": task_id, "message": message})

    if result.status == COMPLETED:
        writer({"type": "task_completed", "task_id": task_id, "result": result.result})
        cleanup_background_task(task_id)
        return f"Task Succeeded. Result: {result.result}"

    # Also handles FAILED and TIMED_OUT

    time.sleep(5)  # Poll every 5 seconds
```

Three SSE event types flow to the frontend:
1. `task_started` → UI shows "Starting: Research AWS..."
2. `task_running` → UI shows intermediate progress
3. `task_completed` / `task_failed` / `task_timed_out` → Final result

The `task_tool` **blocks** from the lead agent's perspective, but LangGraph runs parallel tool calls simultaneously.

### SubagentExecutor — The Engine

**File**: `backend/packages/harness/deerflow/subagents/executor.py`

#### Dual Thread Pool Architecture

```python
_scheduler_pool = ThreadPoolExecutor(max_workers=3)  # Manages lifecycle
_execution_pool = ThreadPoolExecutor(max_workers=3)  # Runs actual agents
```

Why two pools?

```
_scheduler_pool                    _execution_pool
┌──────────────┐                   ┌──────────────┐
│ Scheduler 1  │ ──submits──────→  │ Executor 1   │  ← Actually runs the agent
│ (manages     │                   │ (agent +      │
│  timeout)    │                   │  LLM calls)   │
├──────────────┤                   ├──────────────┤
│ Scheduler 2  │ ──submits──────→  │ Executor 2   │
├──────────────┤                   ├──────────────┤
│ Scheduler 3  │ ──submits──────→  │ Executor 3   │
└──────────────┘                   └──────────────┘
```

The scheduler wraps the executor to enforce timeouts. If a subagent gets stuck, the scheduler can mark it as TIMED_OUT and cancel the future.

#### Creating a Subagent's Agent

```python
def _create_agent(self):
    model = create_chat_model(name=model_name, thinking_enabled=False)
    #                                          ^^^^^^^^^^^^^^^^^^^^^^^^
    #                     Subagents always run WITHOUT thinking (faster, cheaper)

    middlewares = build_subagent_runtime_middlewares(lazy_init=True)
    # Gets: ThreadData + Sandbox + ToolErrorHandling + Guardrail
    # Does NOT get: Uploads, Todo, Title, Memory, ViewImage, Clarification

    return create_agent(
        model=model, tools=self.tools, middleware=middlewares,
        system_prompt=self.config.system_prompt, state_schema=ThreadState,
    )
```

#### Execution Flow

```python
async def _aexecute(self, task, result_holder):
    agent = self._create_agent()
    state = {
        "messages": [HumanMessage(content=task)],  # Prompt becomes "user message"
        "sandbox": self.sandbox_state,              # Inherited from parent
        "thread_data": self.thread_data,            # Same file paths
    }

    async for chunk in agent.astream(state, stream_mode="values"):
        # Capture AI messages for progress reporting
        if isinstance(last_message, AIMessage):
            result.ai_messages.append(message_dict)

    result.result = last_ai_message.content
    result.status = SubagentStatus.COMPLETED
```

### SubagentLimitMiddleware — The Safety Net

**File**: `backend/packages/harness/deerflow/agents/middlewares/subagent_limit_middleware.py`

Even with prompt instructions, LLMs sometimes generate too many task calls. This middleware is the hard enforcement:

```python
class SubagentLimitMiddleware(AgentMiddleware):
    def __init__(self, max_concurrent=3):
        self.max_concurrent = _clamp_subagent_limit(max_concurrent)  # Clamped to [2, 4]

    def after_model(self, state, runtime):
        # Count task() calls in the AI's response
        task_indices = [i for i, tc in enumerate(tool_calls) if tc["name"] == "task"]

        if len(task_indices) <= self.max_concurrent:
            return None  # Under limit

        # TRUNCATE: Keep first N, drop the rest
        indices_to_drop = set(task_indices[self.max_concurrent:])
        truncated = [tc for i, tc in enumerate(tool_calls) if i not in indices_to_drop]

        updated_msg = last_msg.model_copy(update={"tool_calls": truncated})
        return {"messages": [updated_msg]}
```

Runs `after_model` — after LLM generates response, before tools execute. If GPT-4 outputs 5 `task()` calls, this trims to 3. Limit clamped to [2, 4].

### Subagent Complete Timeline

Tracing "Compare AWS, Azure, and GCP" in Ultra mode:

```
0s      Frontend sends message with subagent_enabled=True
0.5s    LLM thinks: "3 sub-tasks, fits in 1 batch"
        Generates: task("AWS"), task("Azure"), task("GCP")
0.6s    SubagentLimitMiddleware: 3 ≤ limit 3, passes ✓
0.7s    LangGraph executes all 3 task() calls IN PARALLEL
        Each: look up config → inherit context → create executor → launch in background
        Each: sends task_started SSE event
~30s    Subagent 1 (AWS) completes → task_completed SSE event
~45s    Subagent 2 (Azure) completes
~50s    All 3 done → lead agent synthesizes results
51s     Final comparison streams to frontend
```

### Subagent Key Design Insights

1. **Isolation without duplication**: Own context (conversation history) but shared filesystem
2. **No recursion**: `disallowed_tools=["task"]` + `subagent_enabled=False` = 1-level depth limit
3. **Graceful degradation**: If one subagent fails, others' results are still available
4. **Progress streaming**: `task_running` events show real-time intermediate AI messages
5. **Resource bounded**: Max 3 concurrent subagents, enforced by middleware even if LLM tries more
6. **No thinking**: `thinking_enabled=False` saves tokens — subagents are focused workers

---

## Step 5: The Memory System

### What Problem Memory Solves

Without memory, every DeerFlow conversation starts from zero. The agent doesn't know you prefer Python, work on ML projects, or have been debugging a K8s issue. With memory, it **accumulates knowledge about you** across sessions.

### Memory Components

| File | Role |
|------|------|
| `memory_middleware.py` | Captures conversations after each agent turn |
| `queue.py` | Batches + debounces updates (fires after 30s of silence) |
| `updater.py` | Uses LLM to extract facts, saves to disk |
| `prompt.py` | LLM prompts + formatting for injection |
| `memory_config.py` | Configuration (thresholds, limits, toggles) |

Storage: `backend/.deer-flow/memory.json`

### Memory Lifecycle

```
PHASE 1: CAPTURE                 PHASE 2: PROCESS                PHASE 3: INJECT
(every agent turn)               (30s after last message)        (next conversation)

You: "I prefer Python"           Queue fires timer               make_lead_agent()
Agent: "Sure, noted"             → MemoryUpdater calls LLM      → _get_memory_context()
        │                        → LLM extracts facts            → format_memory_for_injection()
        ▼                        → Saves to memory.json          → Injects into system prompt
MemoryMiddleware.after_agent()           │                              │
→ Filter messages                        ▼                              ▼
→ Queue to MemoryUpdateQueue     memory.json:                    <memory>
→ Debounce timer starts (30s)    { "facts": [{                  - [preference|0.90] Prefers Python
                                   "content": "Prefers Python",  </memory>
                                   "confidence": 0.9 }] }
```

### MemoryMiddleware — Capturing Conversations

**File**: `backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py`

Runs `after_agent` — after LLM has responded and all tools finished:

```python
class MemoryMiddleware(AgentMiddleware):
    def after_agent(self, state, runtime):
        messages = state.get("messages", [])
        filtered_messages = _filter_messages_for_memory(messages)
        queue = get_memory_queue()
        queue.add(thread_id=thread_id, messages=filtered_messages)
        return None  # Memory update is async
```

#### The Message Filter

```
KEEP:
  ✅ Human messages (your questions)
  ✅ AI messages WITHOUT tool_calls (final responses)

DISCARD:
  ❌ Tool messages (intermediate search results, etc.)
  ❌ AI messages WITH tool_calls (intermediate steps)
  ❌ <uploaded_files> blocks (session-scoped, won't exist next time)
```

Example conversation filtering:
```
Human: "What's the latest on quantum computing?"          ← KEEP
AI: [tool_call: web_search("quantum computing 2026")]     ← DISCARD
Tool: [search results: 10 articles...]                    ← DISCARD
AI: [tool_call: web_fetch("https://...")]                 ← DISCARD
Tool: [article content...]                                ← DISCARD
AI: "Here are the latest developments in quantum..."      ← KEEP
```

Upload stripping:
```python
# Before: "<uploaded_files>report.pdf</uploaded_files>\nAnalyze this"
# After:  "Analyze this"
# If NOTHING remains after stripping → skip this turn AND its paired AI response
```

### MemoryUpdateQueue — Debouncing

**File**: `backend/packages/harness/deerflow/agents/memory/queue.py`

Why not update immediately? A conversation has many turns, each would trigger an expensive LLM call. The queue uses **debouncing**:

```python
class MemoryUpdateQueue:
    def add(self, thread_id, messages, agent_name=None):
        # Replace any pending update for this thread (newer is better)
        self._queue = [c for c in self._queue if c.thread_id != thread_id]
        self._queue.append(context)
        self._reset_timer()  # Reset countdown

    def _reset_timer(self):
        if self._timer is not None:
            self._timer.cancel()
        self._timer = threading.Timer(
            config.debounce_seconds,  # Default: 30 seconds
            self._process_queue,
        )
        self._timer.daemon = True
        self._timer.start()
```

Visualized:
```
0s      Message 1 → queue.add()         Timer starts: 30s countdown
5s      Message 2 → queue.add()         Timer RESET: 30s from now
10s     Message 3 → queue.add()         Timer RESET: 30s from now
...     (user stops typing)
40s     Timer fires!                    → LLM analyzes all 3 messages at once
```

Per-thread deduplication: only the **latest** messages for each thread stay in queue.

### MemoryUpdater — The LLM-Powered Extraction Engine

**File**: `backend/packages/harness/deerflow/agents/memory/updater.py`

```python
class MemoryUpdater:
    def update_memory(self, messages, thread_id=None, agent_name=None):
        # 1. Load current memory from disk
        current_memory = get_memory_data(agent_name)

        # 2. Format conversation as text
        conversation_text = format_conversation_for_update(messages)

        # 3. Build prompt for LLM
        prompt = MEMORY_UPDATE_PROMPT.format(
            current_memory=json.dumps(current_memory),
            conversation=conversation_text,
        )

        # 4. Call LLM (thinking DISABLED — just extraction)
        model = create_chat_model(name=model_name, thinking_enabled=False)
        response = model.invoke(prompt)

        # 5. Parse JSON response
        update_data = json.loads(response_text)

        # 6. Apply updates
        updated_memory = self._apply_updates(current_memory, update_data, thread_id)

        # 7. Strip upload mentions (safety net)
        updated_memory = _strip_upload_mentions_from_memory(updated_memory)

        # 8. Save atomically (temp file → rename)
        _save_memory_to_file(updated_memory, agent_name)
```

#### How `_apply_updates` works:

1. **Update summaries** — Only if LLM says `shouldUpdate: true`
2. **Remove contradicted facts** — LLM specifies `factsToRemove` list
3. **Add new facts** — With deduplication (whitespace-normalized comparison)
4. **Enforce confidence threshold** — Default 0.7, skip low-confidence facts
5. **Enforce max facts** — Default 100, trim lowest confidence when exceeded

#### LLM Output Format

The LLM returns structured JSON:
```json
{
  "user": {
    "workContext": { "summary": "...", "shouldUpdate": true },
    "topOfMind": { "summary": "...", "shouldUpdate": true }
  },
  "history": {
    "recentMonths": { "summary": "...", "shouldUpdate": false }
  },
  "newFacts": [
    { "content": "Prefers Python", "category": "preference", "confidence": 0.9 }
  ],
  "factsToRemove": ["fact_old_id"]
}
```

Fact categories: `preference`, `knowledge`, `context`, `behavior`, `goal`

Confidence levels:
- 0.9-1.0: Explicitly stated ("I work on X")
- 0.7-0.8: Strongly implied from actions
- 0.5-0.6: Inferred patterns (filtered out by default threshold)

### memory.json — The Data Structure

```json
{
  "version": "1.0",
  "lastUpdated": "2026-03-26T10:30:00Z",
  "user": {
    "workContext": {
      "summary": "Senior backend engineer at Acme Corp, Rust migration with Axum.",
      "updatedAt": "2026-03-26T10:30:00Z"
    },
    "personalContext": {
      "summary": "Bilingual (EN/CN). Prefers concise technical explanations.",
      "updatedAt": "2026-03-25T15:00:00Z"
    },
    "topOfMind": {
      "summary": "Rust migration, K8s memory leak, WebAssembly exploration.",
      "updatedAt": "2026-03-26T10:30:00Z"
    }
  },
  "history": {
    "recentMonths": { "summary": "Deep in Rust migration...", "updatedAt": "..." },
    "earlierContext": { "summary": "Previously used FastAPI...", "updatedAt": "..." },
    "longTermBackground": { "summary": "8+ years backend dev...", "updatedAt": "..." }
  },
  "facts": [
    {
      "id": "fact_a1b2c3d4",
      "content": "Prefers Python over JavaScript for backend",
      "category": "preference",
      "confidence": 0.9,
      "createdAt": "2026-03-10T08:00:00Z",
      "source": "thread-abc-123"
    }
  ]
}
```

Three layers:

| Layer | What | Update Frequency |
|-------|------|-----------------|
| **User Context** | Who you are now | Frequent (topOfMind changes often) |
| **History** | What you've done over time | Moderate (temporal buckets) |
| **Facts** | Discrete knowledge | Per-conversation |

### Injection — How Memory Gets Into the Prompt

On every new conversation, `apply_prompt_template()` calls `_get_memory_context()`:

```python
def format_memory_for_injection(memory_data, max_tokens=2000):
    # 1. Add user context summaries
    # 2. Add history summaries
    # 3. Add facts sorted by confidence (highest first)
    #    Stop adding when token budget exhausted
```

What the agent sees:
```xml
<memory>
User Context:
- Work: Senior backend engineer at Acme Corp, Rust migration
- Current Focus: Rust migration, K8s memory leak, WebAssembly

History:
- Recent: Deep in Rust migration, debugging K8s deployments

Facts:
- [preference | 0.90] Prefers Python over JavaScript
- [context | 0.85] Company uses Kubernetes with 50+ microservices
- [knowledge | 0.82] Experienced with LangChain and FastAPI
</memory>
```

Token budget (default 2000) means highest-confidence facts are injected first, lower ones dropped when budget exhausted.

### Memory Safety Mechanisms

#### Upload Stripping (4 Layers of Defense)

```
Layer 1: _filter_messages_for_memory()
         → Strips <uploaded_files> blocks before queueing

Layer 2: format_conversation_for_update()
         → Strips again from text sent to LLM

Layer 3: MEMORY_UPDATE_PROMPT
         → Tells LLM: "Do NOT record file upload events"

Layer 4: _strip_upload_mentions_from_memory()
         → Regex scrubs any upload mentions that slipped through
```

#### Atomic File Writes

```python
temp_path = file_path.with_suffix(".tmp")
with open(temp_path, "w") as f:
    json.dump(memory_data, f)
temp_path.replace(file_path)  # Atomic rename
```

If process crashes mid-write, you get either old file or new file — never corrupted.

#### Cache with mtime Invalidation

```python
def get_memory_data(agent_name=None):
    current_mtime = file_path.stat().st_mtime
    cached = _memory_cache.get(agent_name)
    if cached is None or cached[1] != current_mtime:
        memory_data = _load_memory_from_file(agent_name)  # Reload
        _memory_cache[agent_name] = (memory_data, current_mtime)
    return cached[0]  # Cache hit
```

### Memory Configuration

```yaml
# config.yaml
memory:
  enabled: true                    # Master switch
  injection_enabled: true          # Inject into prompts?
  storage_path: ""                 # Default: .deer-flow/memory.json
  debounce_seconds: 30             # Wait after last message before processing
  model_name: null                 # LLM for extraction (null = default)
  max_facts: 100                   # Maximum stored facts
  fact_confidence_threshold: 0.7   # Minimum confidence to store
  max_injection_tokens: 2000       # Token budget for prompt injection
```

---

## Step 6: The Sandbox System

The sandbox is how agents **run code**, **read/write files**, and **execute bash commands**. It's a pluggable system with two implementations: run directly on your machine (LocalSandbox) or inside Docker containers (AioSandbox).

### Sandbox Big Picture

```
Agent calls bash("ls /mnt/user-data/uploads")
    │
    ▼
┌─────────────────────────────────────────┐
│           5 Sandbox Tools               │
│  bash, ls, read_file, write_file,       │
│  str_replace                            │
└──────────────┬──────────────────────────┘
               │
          Path Translation
     /mnt/user-data → real host path
               │
    ┌──────────┴──────────┐
    │                     │
┌───▼──────────┐  ┌───────▼──────────┐
│ LocalSandbox │  │   AioSandbox     │
│ (subprocess) │  │ (Docker/K8s HTTP)│
└──────────────┘  └──────────────────┘
```

The agent **never sees real filesystem paths**. It only knows:
- `/mnt/user-data/workspace/` — its working directory
- `/mnt/user-data/uploads/` — files the user uploaded
- `/mnt/user-data/outputs/` — files it produces
- `/mnt/skills/` — read-only skill definitions

### Virtual Path Translation

This is the most important concept. The agent thinks it's working in `/mnt/user-data/`, but really:

```
Virtual (what agent sees)          Physical (what actually exists)
────────────────────────────────────────────────────────────────
/mnt/user-data/workspace/    →    ~/.deer-flow/threads/{id}/user-data/workspace/
/mnt/user-data/uploads/      →    ~/.deer-flow/threads/{id}/user-data/uploads/
/mnt/user-data/outputs/      →    ~/.deer-flow/threads/{id}/user-data/outputs/
/mnt/skills/                 →    deer-flow/skills/  (read-only!)
```

**Translation happens in both directions:**

**Inbound** (agent's command → real path):
```python
# Agent sends: "cat /mnt/user-data/uploads/report.pdf"
# Tool translates to: "cat ~/.deer-flow/threads/abc123/user-data/uploads/report.pdf"
replace_virtual_paths_in_command(command, thread_data)
```

**Outbound** (real output → virtual paths):
```python
# Subprocess returns: "file at /home/user/.deer-flow/threads/abc123/..."
# Tool masks to: "file at /mnt/user-data/..."
mask_local_paths_in_output(output, thread_data)
```

This is **only needed for LocalSandbox**. AioSandbox mounts volumes at the virtual paths directly, so `/mnt/user-data` actually exists inside the container.

### Sandbox Security Model

The sandbox enforces strict path boundaries:

```python
def validate_local_tool_path(path, thread_data, *, read_only=False):
    # ❌ Reject path traversal ("../../etc/passwd")
    _reject_path_traversal(path)

    # ✅ /mnt/user-data/* — always allowed (read + write)
    # ✅ /mnt/skills/* — only when read_only=True
    # ❌ Everything else — rejected with PermissionError
```

For bash commands, additional validation:
```python
def validate_local_bash_command_paths(command, thread_data):
    # Scans for ALL absolute paths in the command
    # Allows: /mnt/user-data/*, /mnt/skills/*, /bin/*, /usr/bin/*, /dev/*
    # Rejects: anything else with an absolute path
```

Even after translation, a **post-resolution check** ensures symlinks can't escape:
```python
def _validate_resolved_user_data_path(resolved, thread_data):
    # resolved = Path("/...").resolve()  (follows symlinks)
    # Must be relative_to(workspace_path | uploads_path | outputs_path)
    # If not → PermissionError
```

### The Two Sandbox Implementations

#### LocalSandbox (Development)

```yaml
# config.yaml
sandbox:
  use: deerflow.sandbox.local:LocalSandboxProvider
```

- **Singleton** — one instance shared by all threads
- Commands run via `subprocess.run(command, shell=True, timeout=600)`
- Detects shell: `/bin/zsh` → `/bin/bash` → `/bin/sh`
- Files read/written directly via Python `open()`
- Path translation + security validation happens in the tools layer

#### AioSandbox (Production)

```yaml
# config.yaml — Docker
sandbox:
  use: deerflow.community.aio_sandbox:AioSandboxProvider
  image: .../all-in-one-sandbox:latest
  idle_timeout: 600
  replicas: 3

# Or Kubernetes
sandbox:
  use: deerflow.community.aio_sandbox:AioSandboxProvider
  provisioner_url: http://provisioner:8002
```

- Each thread gets its own Docker container
- Commands execute via HTTP API (`POST /v1/shell/exec`)
- Files transferred via HTTP (`POST /v1/file/write`)
- Volumes mounted at `/mnt/user-data/*` and `/mnt/skills` directly in container

### Container Lifecycle (AioSandbox)

The AioSandboxProvider manages a **warm pool** to avoid cold starts:

```
acquire("thread-abc")
    │
    ├─ Check 1: In-process cache → instant return
    │
    ├─ Check 2: Warm pool (released but still running) → pop & reuse
    │
    └─ Check 3: Cross-process discovery + create
         │
         ├─ File lock (fcntl.flock) for serialization
         ├─ backend.discover() → docker inspect → found? reuse
         └─ backend.create() → docker run → poll health → return
```

**Deterministic IDs** are key to cross-process sharing:
```python
def _deterministic_sandbox_id(thread_id):
    return hashlib.sha256(thread_id.encode()).hexdigest()[:8]
```

Same thread ID → same sandbox ID → same container name → another process can discover it.

**Release vs Destroy:**
- `release()` → container goes to warm pool (still running, fast re-acquire)
- `destroy()` → container actually stopped and removed
- **Idle timeout** (default 600s) — background thread checks every 60s, destroys idle containers

**Cross-Process Container Discovery:**
```
Process A:                              Process B (same thread):
1. acquire("thread-xyz")               1. acquire("thread-xyz")
2. Generate ID "abc12345"              2. Generate ID "abc12345" (deterministic, same!)
3. Acquire file lock                   3. Acquire file lock (waits for A)
4. discover() → not found              4. discover("abc12345")
5. create() → docker run               5. Container found (started by A)
6. Release file lock                   6. Extract port, verify health
                                       7. Return same container
```

### The 5 Sandbox Tools

| Tool | Parameters | What it does |
|------|-----------|--------------|
| `bash` | `description`, `command` | Execute shell command with 10-min timeout |
| `ls` | `description`, `path` | Tree listing, 2 levels deep, ignores .git/node_modules/etc |
| `read_file` | `description`, `path`, `start_line?`, `end_line?` | Read text file, optional line range |
| `write_file` | `description`, `path`, `content`, `append?` | Write/append text, auto-creates dirs |
| `str_replace` | `description`, `path`, `old_str`, `new_str`, `replace_all?` | Find-and-replace in file |

Every tool requires a `description` parameter first — used for logging/auditing what the agent intends to do.

**Execution flow for `bash` tool (Local Sandbox):**
```
1. ensure_sandbox_initialized(runtime)     — Lazy acquire or reuse
2. ensure_thread_directories_exist(runtime) — Create workspace/uploads/outputs
3. validate_local_bash_command_paths()      — Security check
4. replace_virtual_paths_in_command()       — Virtual→physical translation
5. sandbox.execute_command(command)         — Execute via subprocess
6. mask_local_paths_in_output()            — Physical→virtual for output
7. Return output string (errors caught, never raised)
```

### Lazy Initialization Flow

Sandboxes aren't created at startup. They're created on **first tool use**:

```
1. Agent decides to call bash("list files", "ls /mnt/user-data")
2. bash_tool() calls ensure_sandbox_initialized(runtime)
3. Check state["sandbox"] — null (first call)
4. provider.acquire(thread_id) — creates/finds sandbox
5. state["sandbox"] = {"sandbox_id": "abc123"}
6. Execute command
7. After agent turn: middleware.after_agent() calls release()
8. Container goes to warm pool (AIO) or no-op (Local)
```

The SandboxMiddleware supports both lazy (default) and eager modes:
- **Lazy** (`lazy_init=True`): Sandbox created when tool first called
- **Eager** (`lazy_init=False`): Sandbox created in `before_agent()` hook

### Sandbox Key Design Insights

| Pattern | Why |
|---------|-----|
| **Virtual paths** | Agent can't accidentally access host files |
| **Bidirectional masking** | Agent never sees real paths, even in error messages |
| **Deterministic IDs** | Multiple LangGraph workers share the same container |
| **Warm pool** | No cold-start penalty on conversation continuation |
| **File locks** | Multiple processes don't create duplicate containers |
| **Idle cleanup** | Containers don't accumulate forever |
| **Pluggable backends** | Same code works for Docker dev, K8s prod |

---

## Step 7: The Skills System

Skills are **SKILL.md files** that give the agent specialized workflows. Think of them as "instruction manuals" the agent reads on-demand.

### Progressive Disclosure

The key insight is that skills load in **three layers** to minimize token usage:

```
Layer 1 — Always in prompt (~50 tokens/skill):
  <skill>
    <name>data-analysis</name>
    <description>Use when user uploads Excel/CSV files...</description>
    <location>/mnt/skills/public/data-analysis/SKILL.md</location>
  </skill>

Layer 2 — Loaded on-demand via read_file (~500-2000 tokens):
  The full SKILL.md body with instructions, workflow steps, examples

Layer 3 — Loaded only when executing (~variable):
  Referenced scripts, templates, guides inside the skill folder
```

The agent sees the skill list in every prompt. When a user request matches, it calls `read_file("/mnt/skills/public/data-analysis/SKILL.md")` to get full instructions, then reads sub-resources as needed.

### Skill Discovery Pipeline

```
deer-flow/skills/
├── public/          ← 17 official skills (committed to repo)
│   ├── bootstrap/
│   ├── data-analysis/
│   ├── podcast-generation/
│   ├── image-generation/
│   └── ...
└── custom/          ← User-installed skills (gitignored)
    └── my-skill/
```

**Loading process:**

```
load_skills(enabled_only=True)
    │
    ├─ os.walk("skills/public/") + os.walk("skills/custom/")
    │  (sorted, hidden dirs skipped)
    │
    ├─ For each dir with SKILL.md:
    │   parse_skill_file() → extract YAML frontmatter
    │   → Skill(name, description, category, relative_path, ...)
    │
    ├─ Read extensions_config.json (always fresh, not cached)
    │   → set skill.enabled based on config state
    │
    └─ Return sorted list[Skill]
```

**Key types:**
```python
@dataclass
class Skill:
    name: str
    description: str
    license: str | None
    skill_dir: Path
    skill_file: Path
    relative_path: Path               # e.g., "public/data-analysis"
    category: str                      # 'public' or 'custom'
    enabled: bool = False

    def get_container_file_path(container_base_path="/mnt/skills") -> str:
        # Returns "/mnt/skills/{category}/{skill_path}/SKILL.md"
```

### SKILL.md Anatomy

```yaml
---
name: data-analysis              # Required, hyphen-case, max 64 chars
description: |                    # Required, max 1024 chars
  Use when user uploads Excel/CSV files and wants data analysis,
  visualization, or insights.
license: MIT                      # Optional
author: DeerFlow                  # Optional
version: 1.0.0                   # Optional
allowed-tools: [bash, read_file]  # Optional
---

# Data Analysis Skill

## Workflow
1. Read uploaded file from /mnt/user-data/uploads/
2. Use pandas to analyze...
3. Generate charts...
4. Save results to /mnt/user-data/outputs/

## References
- See `scripts/analyze.py` for the analysis template
```

**Validation rules:**
- Name: lowercase + digits + hyphens, no `--`, no leading/trailing hyphen, max 64 chars
- Description: no angle brackets (`<`, `>`), max 1024 chars
- Only 8 frontmatter keys allowed: `name`, `description`, `license`, `author`, `version`, `compatibility`, `allowed-tools`, `metadata`

**Example skill directory structures:**
```
skills/public/bootstrap/           skills/public/data-analysis/
├── SKILL.md                       ├── SKILL.md
├── templates/                     └── scripts/
│   └── SOUL.template.md               └── analyze.py
└── references/
    └── conversation-guide.md
```

### How Skills Enter the Prompt

`apply_prompt_template()` calls `get_skills_prompt_section()`:

```xml
<skill_system>
You have access to skills that provide optimized workflows for specific tasks.

**Progressive Loading Pattern:**
1. When a user query matches a skill's use case, immediately call `read_file`
   on the skill's main file using the path attribute
2. Read and understand the skill's workflow and instructions
3. Load referenced resources only when needed during execution
4. Follow the skill's instructions precisely

**Skills are located at:** /mnt/skills

<available_skills>
    <skill>
        <name>data-analysis</name>
        <description>Use when user uploads Excel/CSV files...</description>
        <location>/mnt/skills/public/data-analysis/SKILL.md</location>
    </skill>
    <skill>
        <name>bootstrap</name>
        <description>Generate a personalized SOUL.md through onboarding...</description>
        <location>/mnt/skills/public/bootstrap/SKILL.md</location>
    </skill>
    ...
</available_skills>
</skill_system>
```

The prompt also includes a reminder: *"Skill First: Always load the relevant skill before starting complex tasks."*

### Runtime Flow — Agent Using a Skill

```
User: "Analyze my sales data"
  │
  ▼
Agent sees <available_skills> in system prompt
  → Matches "data-analysis" skill
  │
  ▼
Agent calls: read_file("/mnt/skills/public/data-analysis/SKILL.md")
  │
  ▼
read_file_tool():
  ├─ validate_local_tool_path(path, read_only=True)  ← /mnt/skills allowed
  ├─ _is_skills_path(path) → true
  ├─ _resolve_skills_path(path)
  │   "/mnt/skills/public/data-analysis/SKILL.md"
  │   → "deer-flow/skills/public/data-analysis/SKILL.md"
  ├─ sandbox.read_file(resolved_path)
  └─ mask_local_paths_in_output() ← host paths hidden
  │
  ▼
Agent receives full SKILL.md body with workflow instructions
  │
  ▼
Agent follows instructions:
  ├─ read_file("/mnt/user-data/uploads/sales.csv")
  ├─ bash("python /mnt/skills/public/data-analysis/scripts/analyze.py")
  ├─ write_file("/mnt/user-data/outputs/report.html", ...)
  └─ present_files(["report.html"])
```

### Skills Configuration & State

**Two config files work together:**

| File | Purpose | Caching |
|------|---------|---------|
| `config.yaml` (skills section) | Path to skills dir, container path | mtime-cached |
| `extensions_config.json` | Enabled/disabled per skill | Always read fresh |

```yaml
# config.yaml
skills:
  path: null                    # Default: deer-flow/skills/
  container_path: /mnt/skills   # Virtual path agent sees
```

```json
// extensions_config.json (shared with MCP servers!)
{
  "mcpServers": { ... },
  "skills": {
    "data-analysis": { "enabled": true },
    "bootstrap": { "enabled": false },
    "my-custom-skill": { "enabled": true }
  }
}
```

**Why always-fresh for extensions_config?** The Gateway API (port 8001) and LangGraph server (port 2024) are separate processes. When you toggle a skill via the Gateway, LangGraph reads from disk on next prompt to pick up the change.

### Skill Installation Workflow

The agent can **create new skills** that users install:

```
Step 1: Agent creates skill
  ├─ Writes SKILL.md with proper frontmatter
  ├─ Bundles with scripts/templates
  ├─ Zips into my-skill.skill (ZIP format)
  └─ Saves to /mnt/user-data/outputs/my-skill.skill

Step 2: Agent calls present_files()
  └─ .skill file visible in frontend

Step 3: User installs via frontend
  └─ POST /api/skills/install { thread_id, path }

Step 4: Gateway processes
  ├─ Resolves virtual path → actual file
  ├─ Validates ZIP (no traversal, no symlinks, ≤512MB)
  ├─ Extracts to temp dir
  ├─ Validates SKILL.md frontmatter
  ├─ Copies to skills/custom/{name}/
  └─ Returns success

Step 5: Automatic availability
  └─ Next load_skills() discovers it → appears in prompt
```

**No restart needed!** The skill is available on the very next message.

**Security during installation:**
- ZIP extraction validates all paths (no traversal, no symlinks)
- Zip bomb protection (512 MB max)
- Frontmatter property whitelist (only 8 allowed keys)
- Name validation (hyphen-case, max 64 chars)

### Skills Gateway API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/skills` | GET | List all skills with enabled state |
| `/api/skills/{name}` | GET | Get single skill details |
| `/api/skills/{name}` | PUT | Toggle enabled/disabled |
| `/api/skills/install` | POST | Install from .skill archive |

### Skills Frontend Integration

```typescript
// React hooks (TanStack Query)
const { skills, isLoading } = useSkills()           // GET /api/skills
const { mutate: toggle } = useEnableSkill()          // PUT /api/skills/{name}
await installSkill({ thread_id, path })              // POST /api/skills/install
```

Toggle invalidates the query cache → UI updates immediately.

### Skills Key Design Insights

| Pattern | Why |
|---------|-----|
| **Progressive disclosure** | Only ~50 tokens per skill in prompt; full content loaded on-demand |
| **Virtual paths** | Agent reads `/mnt/skills/...`, never sees host filesystem |
| **Read-only enforcement** | Agent can read skills but never modify them |
| **Separate config files** | `config.yaml` for paths, `extensions_config.json` for state (shared with MCP) |
| **Always-fresh config reads** | Cross-process consistency between Gateway and LangGraph |
| **ZIP validation** | No path traversal, no symlinks, size limit, frontmatter whitelist |
| **Nested skill support** | Skills can be organized as `parent/child-skill` with deep directory scan |
| **Deterministic ordering** | Sorted traversal ensures consistent skill list across runs |

---

## Suggested Learning Schedule

| Day | Focus | Action |
|-----|-------|--------|
| 1 | Setup | `make check && make install && make dev` — get it running |
| 2 | Architecture | Read this guide's architecture section + README.md |
| 3 | Agent core | Read `lead_agent/agent.py`, `thread_state.py` |
| 4 | Middleware | Read 3-4 middleware files in `agents/middlewares/` |
| 5 | Subagents | Read `executor.py`, `task_tool.py`, built-in configs |
| 6 | Memory | Read `updater.py`, `queue.py`, `memory_middleware.py` |
| 7 | Sandbox | Read `sandbox/tools.py`, `local_sandbox.py`, trace a bash call |
| 8 | Skills | Read a SKILL.md file, read `skills/loader.py`, trace skill loading |
| 9 | Frontend | Read `core/threads/hooks.ts`, trace a message flow |

---

## Key Files Reference

### Backend — Agent System
- `backend/langgraph.json` — Entry point config
- `backend/packages/harness/deerflow/agents/lead_agent/agent.py` — Agent factory
- `backend/packages/harness/deerflow/agents/lead_agent/prompt.py` — System prompt
- `backend/packages/harness/deerflow/agents/thread_state.py` — State schema
- `backend/packages/harness/deerflow/agents/middlewares/` — All middleware files

### Backend — Subagents
- `backend/packages/harness/deerflow/subagents/executor.py` — Execution engine
- `backend/packages/harness/deerflow/subagents/config.py` — SubagentConfig
- `backend/packages/harness/deerflow/subagents/builtins/` — Built-in agent configs
- `backend/packages/harness/deerflow/tools/builtins/task_tool.py` — task() tool

### Backend — Memory
- `backend/packages/harness/deerflow/agents/memory/updater.py` — LLM extraction
- `backend/packages/harness/deerflow/agents/memory/queue.py` — Debounce queue
- `backend/packages/harness/deerflow/agents/memory/prompt.py` — LLM prompts
- `backend/packages/harness/deerflow/agents/middlewares/memory_middleware.py` — Capture middleware

### Backend — Other Systems
- `backend/packages/harness/deerflow/models/factory.py` — LLM factory
- `backend/packages/harness/deerflow/tools/tools.py` — Tool assembly
- `backend/packages/harness/deerflow/config/app_config.py` — Config system
- `backend/packages/harness/deerflow/mcp/` — MCP integration

### Backend — Sandbox
- `backend/packages/harness/deerflow/sandbox/tools.py` — Sandbox tools (bash, ls, read_file, write_file, str_replace)
- `backend/packages/harness/deerflow/sandbox/sandbox.py` — Abstract Sandbox base class
- `backend/packages/harness/deerflow/sandbox/sandbox_provider.py` — Provider singleton + abstractions
- `backend/packages/harness/deerflow/sandbox/middleware.py` — SandboxMiddleware (lazy/eager init)
- `backend/packages/harness/deerflow/sandbox/exceptions.py` — Exception hierarchy
- `backend/packages/harness/deerflow/sandbox/local/local_sandbox.py` — LocalSandbox (subprocess)
- `backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py` — Singleton provider
- `backend/packages/harness/deerflow/sandbox/local/list_dir.py` — Directory listing utility
- `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox.py` — AioSandbox (Docker HTTP)
- `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py` — Warm pool + lifecycle
- `backend/packages/harness/deerflow/community/aio_sandbox/backend.py` — Backend abstraction
- `backend/packages/harness/deerflow/community/aio_sandbox/local_backend.py` — Docker container backend
- `backend/packages/harness/deerflow/community/aio_sandbox/remote_backend.py` — K8s provisioner backend
- `backend/packages/harness/deerflow/config/sandbox_config.py` — SandboxConfig + VolumeMountConfig
- `backend/packages/harness/deerflow/config/paths.py` — Paths singleton (base_dir, thread dirs, virtual path resolution)

### Backend — Skills
- `backend/packages/harness/deerflow/skills/loader.py` — Skill discovery (load_skills)
- `backend/packages/harness/deerflow/skills/parser.py` — YAML frontmatter parser (parse_skill_file)
- `backend/packages/harness/deerflow/skills/types.py` — Skill dataclass
- `backend/packages/harness/deerflow/skills/validation.py` — Frontmatter + name validation
- `backend/packages/harness/deerflow/config/skills_config.py` — SkillsConfig (path, container_path)
- `backend/packages/harness/deerflow/config/extensions_config.py` — ExtensionsConfig (MCP + skills state)
- `backend/app/gateway/routers/skills.py` — Gateway REST endpoints (list, get, update, install)
- `skills/` — Actual skill definitions (public/ and custom/)

### Frontend — Skills
- `frontend/src/core/skills/api.ts` — loadSkills, enableSkill, installSkill
- `frontend/src/core/skills/type.ts` — Skill, InstallSkillRequest types
- `frontend/src/core/skills/hooks.ts` — useSkills, useEnableSkill hooks

### Frontend
- `frontend/src/core/threads/hooks.ts` — Thread hooks (primary API interface)
- `frontend/src/core/api/api-client.ts` — LangGraph client singleton
- `frontend/src/app/workspace/chats/[thread_id]/page.tsx` — Chat page
- `frontend/src/env.js` — Environment validation

### Configuration
- `config.yaml` — Main app config (models, tools, sandbox, memory, channels)
- `extensions_config.json` — MCP servers and skills state
- `.env` — API keys and secrets
- `backend/ruff.toml` — Python lint/format rules
- `frontend/eslint.config.js` — TypeScript lint rules

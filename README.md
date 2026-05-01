# aged - agent daemon like a fine wine

`aged` is an agent orchestrator for autonomous development work.

The current implementation is an initial vertical slice:

- Go daemon with SQLite event persistence
- Prompt-driven, Codex-backed, and API-backed orchestrator brain providers
- Codex, Claude, and shell worker runner adapters selected by the orchestrator
- Normalized worker events for logs, results, errors, and worker requests for input
- VCS-pluggable workspace preflight before workers start
- HTTP API and SSE event stream
- React/Vite dashboard for task creation, assistant Q&A, PR publishing, steering, cancellation, and live state

## Run the daemon

```sh
go run ./cmd/aged
```

The daemon listens on `http://127.0.0.1:8787` by default and writes state to `aged.db`.

Useful flags:

```sh
go run ./cmd/aged -addr 127.0.0.1:8787 -db aged.db -worker mock -workdir .
```

The interactive Ask panel is separate from the scheduler brain. Configure it with `-assistant` / `AGED_ASSISTANT`:

- `auto`: use `codex` or `claude` when the default worker is one of those CLIs.
- `codex`: run `codex exec --json` for direct answers.
- `claude`: run `claude --print --output-format stream-json` for direct answers.
- `brain`: fall back to the configured brain provider if it implements Q&A.
- `none`: no CLI assistant.

The dev-control server defaults to `-assistant auto`, so a `-worker codex` dev daemon uses Codex for Ask even when scheduling still uses `-brain prompt`.

Task titles are optional. When `title` is omitted or blank, aged asks the configured assistant for a short 1-8 word title and falls back to a local prompt-derived title if the assistant is unavailable.

Remote execution targets can be registered dynamically through `POST /api/targets` / `PUT /api/targets/{id}` or through the dashboard Targets pane. They are persisted in SQLite and become available to the live scheduler without restarting the daemon.

`-targets` / `AGED_TARGETS` still accept startup seed JSON:

```json
{
  "targets": [
    {
      "id": "perf-1",
      "kind": "ssh",
      "host": "perf-1.internal",
      "user": "aged",
      "identityFile": "/Users/me/.ssh/id_ed25519",
      "insecureIgnoreHostKey": false,
      "workDir": "/srv/aged/repo",
      "workRoot": "/srv/aged/runs",
      "labels": { "role": "benchmark" },
      "capacity": { "maxWorkers": 2, "cpuWeight": 8, "memoryGB": 32 }
    }
  ]
}
```

`identityFile` is optional. Leave it unset to use normal OpenSSH behavior, including `ssh-agent` and `~/.ssh/config`.

SSH targets expect `ssh` and `tmux` to be available. The daemon starts a detached tmux session per worker, writes logs/status under `workRoot`, polls those files back into the normal event stream, and can resume polling after a daemon restart from execution node metadata.
Before starting a fresh remote worker, aged prepares the project checkout on the target: if `workDir` is missing it clones the configured project repo, and if `workDir` is a clean Git checkout it fetches origin and checks out the project `defaultBase` from `origin`. Dirty remote checkouts are rejected instead of being overwritten. Retry/follow-up workers reuse the previous remote workdir so partial work is preserved.
Remote workers also write VCS change summaries and `diff.patch` into the run directory. When the remote workdir is a `jj` or Git checkout, aged reads those artifacts back into the normal `worker.completed.workspaceChanges` projection.
Remote worker patches can be reviewed and applied through the normal worker apply endpoint. The current remote apply path applies `diff.patch` to the task project's local checkout with `git apply`, then records `worker.changes_applied`.

Plugins and external integration descriptors are configured with `-plugins` / `AGED_PLUGINS` using JSON. Enabled plugins with `protocol: "aged-plugin-v1"` and a `command` are probed at daemon startup by running `command... describe`; the command must print a JSON plugin descriptor. This gives external drivers/runners a narrow process lifecycle without loading code into the daemon.

```json
{
  "plugins": [
    {
      "id": "driver:github-issues",
      "name": "GitHub Issue Driver",
      "kind": "driver",
      "protocol": "aged-plugin-v1",
      "enabled": true,
      "command": ["aged-github-driver"],
      "capabilities": ["issues", "pull-requests"]
    }
  ]
}
```

The built-in GitHub driver is enabled separately with `-github-driver` / `AGED_GITHUB_DRIVER`, which accepts either a JSON file path or inline JSON. It uses the local `gh` identity, so a VM bot setup should authenticate `gh` as that bot user before starting aged.

```json
{
  "enabled": true,
  "intervalSeconds": 60,
  "issueLimit": 20,
  "issues": [
    {
      "repo": "nathanwhit/aged",
      "labels": ["aged"],
      "projectId": "aged"
    }
  ],
  "pullRequests": {
    "enabled": true,
    "repos": ["nathanwhit/aged"],
    "autoPublish": true,
    "autoBabysit": true,
    "draft": false
  }
}
```

On each poll, the driver creates idempotent tasks for matching GitHub issues, publishes PRs for succeeded GitHub issue tasks that do not already have a PR, refreshes known PR status, and steers the original PR task when checks fail, reviews request changes, or mergeability is blocked/dirty. It does not merge PRs automatically.

The built-in Discord driver is enabled with `-discord-driver` / `AGED_DISCORD_DRIVER`, which accepts a JSON file path or inline JSON. It polls configured channels with a Discord bot token, answers normal messages through the configured aged assistant, and can either answer, propose a task for later confirmation, or directly create a task when the conversation clearly asks aged to start work. `task: <prompt>` and a later `do it` reply remain supported shortcuts. If the assistant is not configured for useful conversational answers, the driver still lets `do it` create a task from the prior Discord message.

```json
{
  "enabled": true,
  "token": "optional; defaults to DISCORD_BOT_TOKEN",
  "intervalSeconds": 5,
  "messageLimit": 20,
  "processHistory": false,
  "channels": [
    {
      "id": "123456789012345678",
      "defaultProjectId": "aged",
      "allowedUserIds": ["234567890123456789"],
      "requireMention": true,
      "taskPrefix": "task:"
    }
  ]
}
```

The Discord assistant is prompted to return a structured decision: `answer`, `list_projects`, `create_project`, `propose_task`, or `create_task`. Proposed/created tasks include `projectId`, `title`, and `prompt`; the project id must match a configured project, with `defaultProjectId` used as the channel fallback. Project creation uses the same persisted registry as `POST /api/projects`, so Discord can add local checkouts when the user provides an id/name and absolute path. Chat turns run with the selected project's checkout as the assistant working directory and instruct the assistant to inspect code read-only; code changes should be scheduled as tasks. If the daemon restarts after a proposal, a later confirmation can recover the pending task from persisted assistant events.

This is a daemon-hosted Discord transport, not a replacement for a full interactive Codex/Claude agent. The configured aged assistant persists Codex/Claude provider session ids and resumes headless sessions on follow-up messages, so Discord can support continuous conversation when `-assistant codex` or `-assistant claude` is configured. MCP remains the cleaner path when an external agent runtime is available.

Projects are persisted in SQLite. On first startup with an empty database, aged seeds projects from `-projects` / `AGED_PROJECTS`; if omitted, it creates a single default project from `-workdir`.

```json
{
  "defaultProjectId": "aged",
  "projects": [
    {
      "id": "aged",
      "name": "aged",
      "localPath": "/Users/me/Documents/Code/aged",
      "repo": "owner/aged",
      "defaultBase": "main",
      "targetLabels": { "role": "default" }
    },
    {
      "id": "aged-fork",
      "name": "aged fork",
      "localPath": "/Users/me/Documents/Code/aged-fork",
      "repo": "fork-owner/aged",
      "upstreamRepo": "owner/aged",
      "headRepoOwner": "fork-owner",
      "pushRemote": "fork",
      "defaultBase": "main"
    }
  ]
}
```

Tasks may include `projectId`. External drivers can also include metadata such as `"repo": "owner/repo"`; if that repo matches a configured project, the task is routed there. For fork workflows, `repo` is the local checkout/fork repository, `upstreamRepo` is the issue and PR target repository, `headRepoOwner` qualifies fork PR heads, and `pushRemote` selects the remote for pushed bookmarks or branches. Worker workspaces, worker cwd, apply, and PR publishing all resolve through the task's project.

New projects can also be added while the daemon is running:

```sh
curl -X POST http://127.0.0.1:8787/api/projects \
  -H 'content-type: application/json' \
  -d '{
    "id": "node",
    "name": "Node.js",
    "localPath": "/Users/me/Documents/Code/node",
    "repo": "nodejs/node",
    "vcs": "auto",
    "defaultBase": "main"
  }'
```

The daemon also exposes a streamable HTTP MCP endpoint at `POST /mcp`. It is protected by the same Google auth middleware when auth is enabled. MCP clients can use aged as a durable orchestration backend while natural-language interaction stays in Codex, Claude, or another agent runtime. Exposed tools include `aged_snapshot`, `aged_create_task`, `aged_list_projects`, `aged_create_project`, `aged_publish_pr`, `aged_refresh_pr`, `aged_babysit_pr`, `aged_retry_task`, `aged_steer_task`, and `aged_cancel_task`. MCP resources include `aged://snapshot`, `aged://tasks/{id}`, `aged://workers/{id}`, and `aged://pull-requests/{id}`. The HTTP assistant endpoint and Discord driver also persist provider session ids for resumable Codex/Claude headless conversations.

Scheduler behavior:

- `-worker mock` sets the orchestrator's fallback runner when the prompt brain does not choose a different runner.
- Users do not choose workers per task; task creation only supplies the work request.
- Users or external drivers may choose a project per task with `projectId`; worker selection still belongs to the orchestrator.
- Available runner adapters include `mock`, `codex`, `claude`, `shell`, and `benchmark_compare`.
- Each task records a `task.planned` event with the orchestrator's selected `workerKind`, `workerPrompt`, `reasoningEffort`, rationale, steps, approvals, and future spawn hints. Codex workers receive `reasoningEffort` through `model_reasoning_effort`; Claude workers receive it through `--effort`.
- Worker execution is also represented as first-class `execution.node_planned` state in snapshots, including node id, worker id, worker kind, spawn id, dependencies, role, and status.
- Snapshots include a derived per-task orchestration graph with node summaries and parent/dependency edges, so clients do not need to reconstruct the work graph from raw events.
- The service chooses an execution target for each worker. Target labels come from task metadata or project policy, not from the scheduler brain; scheduler-provided `metadata.targetLabels` are ignored and recorded as such. The target registry scores matching VMs by capacity, current running workers, worker size, and target labels.
- The scheduler is expected to support long-lived work by planning bounded worker turns, then using later turns for review, validation, feedback incorporation, or follow-up implementation.
- `spawns` from initial and dynamically replanned plans are executed as a dependency graph after the plan's primary worker succeeds. Independent spawns run in parallel; spawns with `dependsOn` wait for their prerequisite spawn ids. Follow-up prompts include the original request plus prior worker summaries, errors, and changed files.
- Brains that implement dynamic replanning can inspect completed worker turns after follow-ups and return `continue`, `complete`, `wait`, or `fail`. `continue` schedules another worker turn, runs that plan's follow-up graph, and records `task.replanned`.
- Scheduler and replanner plans can use `ask_user` actions when user setup, credentials, VM changes, profiling permissions, or another human-provided answer is needed. Recoverable environment failures such as missing commands, profiler/kernel permission errors, SSH setup problems, and missing repo setup are also converted into waiting-user approval events.
- Before a worker is created, the orchestrator resolves the task project and records `worker.workspace_prepared` with the prepared cwd, source root, current change, dirty status, VCS type, and workspace mode.
- Worker prompts are wrapped with the prepared execution workspace before dispatch. In isolated mode workers are told to edit only that workspace and not the source checkout, even if the scheduler prompt mentions another local path.
- Workspace backends are selected with `-workspace-vcs auto|jj|git`; `auto` prefers `jj` when `.jj` is present and otherwise supports Git repos. If `-workspace-root` is empty, isolated workspaces default to `~/.aged/workspaces`; relative workspace roots are resolved inside the source checkout.
- Isolated mode is the default. Jujutsu repos use `jj workspace add -r @`; Git repos use `git worktree add --detach HEAD` and require a clean source working tree, ignoring aged's own untracked `.aged/` state. Dependent Git workers receive the prior candidate as a committed base revision, including untracked files.
- Workspace cleanup is selected with `-workspace-cleanup retain|delete_on_success|delete_on_terminal`. Cleanup emits `worker.workspace_cleaned` and does not hide the worker's terminal status.
- Worker subprocess output is normalized into `worker.output` events. Plain output becomes log/error events; Codex and Claude JSONL output preserves raw payloads and is classified as `log`, `result`, `error`, or `needs_input`.
- `worker.completed` includes derived run semantics: `summary`, `error`, `needsInput`, `logCount`, and `workspaceChanges` when available. Workspace changes include dirty status, diffstat, and changed files. Workers that emit `needs_input` move the task to `waiting` and retain the workspace.
- Retained isolated workspace changes can be reviewed with `GET /api/workers/{id}/changes` and applied back to the source checkout with `POST /api/workers/{id}/apply`. Jujutsu apply creates a new merge revision with source `@` and the worker workspace revision as parents; Git apply commits the worker worktree and merges that commit. Apply records `worker.changes_applied`.
- Apply policy recommendations can be requested with `POST /api/tasks/{id}/apply-policy`; the service reports whether there are no candidates, exactly one candidate, or multiple competing candidates requiring manual selection or review.
- Applied task changes can be published as GitHub pull requests with `POST /api/tasks/{id}/pull-request`. If exactly one successful worker has unapplied changes, the service applies it first, creates/pushes a branch from the source checkout, opens the PR with `gh`, and records `pull_request.published`.
- Pull request state is first-class snapshot data. `POST /api/pull-requests/{id}/refresh` re-inspects CI/review/merge state, and `POST /api/pull-requests/{id}/babysit` marks the original PR task for monitoring so later checks or review comments steer that same task.
- Running workers receive task steering through `worker.Spec.Steering` when a runner supports it. The built-in Codex and Claude exec adapters do not hold stdin open for steering because those CLIs treat piped stdin as extra initial prompt input and may wait indefinitely.
- Failed or canceled tasks can be retried with `POST /api/tasks/{id}/retry`; retry reuses the persisted plan on the same task id and runs the normal follow-up/replan flow again. This is intended to recover tasks that were marked canceled after a daemon restart.
- `benchmark_compare` compares explicit `baseline`, `candidate`, `threshold_percent`, and `higher_is_better` prompt fields and emits a benchmark comparison report.

API-backed scheduling can be enabled with:

```sh
AGED_BRAIN=api AGED_BRAIN_MODEL=<model> AGED_BRAIN_API_KEY=<key> go run ./cmd/aged
```

The API brain uses an OpenAI-compatible chat completions endpoint. Override it with `AGED_BRAIN_ENDPOINT` or `-brain-endpoint`. If the API brain is not configured or returns invalid structured output, the daemon falls back to the local prompt brain.

Codex-backed scheduling can be enabled with:

```sh
AGED_BRAIN=codex go run ./cmd/aged
```

The Codex brain runs `codex exec --dangerously-bypass-approvals-and-sandbox --json` against `prompts/scheduler.md`, extracts the final agent message as the scheduler plan JSON, validates it, and falls back to the local prompt brain on command or validation failures. Override the binary with `AGED_CODEX_PATH` or `-codex-path`.

## Google authentication

Local development is unauthenticated by default. To expose the daemon on the web, enable Google OAuth and allowlist the Google account that should have access:

```sh
AGED_AUTH=google \
AGED_GOOGLE_CLIENT_ID=<client-id> \
AGED_GOOGLE_CLIENT_SECRET=<client-secret> \
AGED_AUTH_ALLOWED_EMAILS=you@example.com \
AGED_AUTH_SESSION_KEY="$(openssl rand -base64 32)" \
AGED_AUTH_REDIRECT_URL=https://aged.example.com/auth/callback \
go run ./cmd/aged -addr 0.0.0.0:8787
```

Create the OAuth client in Google Cloud Console as a web application and add the same callback URL as an authorized redirect URI:

```text
https://aged.example.com/auth/callback
```

Useful flags and env vars:

- `-auth` / `AGED_AUTH`: `none` or `google`
- `-google-client-id` / `AGED_GOOGLE_CLIENT_ID`
- `-google-client-secret` / `AGED_GOOGLE_CLIENT_SECRET`
- `-auth-allowed-emails` / `AGED_AUTH_ALLOWED_EMAILS`: comma-separated allowlist
- `-auth-session-key` / `AGED_AUTH_SESSION_KEY`: stable random key, at least 32 bytes
- `-auth-redirect-url` / `AGED_AUTH_REDIRECT_URL`: public callback URL when running behind a tunnel or reverse proxy
- `-assistant` / `AGED_ASSISTANT`: interactive assistant provider, `auto`, `brain`, `none`, `codex`, or `claude`
- `-codex-path` / `AGED_CODEX_PATH`
- `-claude-path` / `AGED_CLAUDE_PATH`

When auth is enabled, `/api/health`, `/auth/login`, `/auth/callback`, `/auth/logout`, and `/api/auth/me` remain public enough to complete login/logout. Dashboard pages and operational APIs require a signed session cookie. If `AGED_AUTH_SESSION_KEY` is omitted, the daemon generates an ephemeral key and existing sessions are invalidated on restart.

## Run the dashboard

```sh
cd web
npm install
npm run dev
```

Vite proxies `/api` requests to the daemon. Open `http://127.0.0.1:5173`.

## Build

```sh
go test ./...
cd web && npm run build
```

After `web/dist` exists, the Go daemon serves it from the same origin.

## Dev control server

For self-iteration, run the local dev control server:

```sh
go run ./cmd/aged-dev
```

It listens on `http://127.0.0.1:8790` by default and manages the daemon at `http://127.0.0.1:8787`.

To view the dashboard from a phone or another device on the same network, bind both listeners to all interfaces:

```sh
go run ./cmd/aged-dev -addr 0.0.0.0:8790 -daemon-addr 0.0.0.0:8787
```

Then open `http://<your-lan-ip>:8787` from the device.

- `GET /health` checks the control server.
- `GET /status` returns the last rebuild/restart result.
- `GET /rebuild` or `POST /rebuild` stops the previously managed daemon, kills any process listening on the daemon port, rebuilds `./cmd/aged`, runs `npm run build` in `web`, and starts the rebuilt daemon.

The rebuilt daemon binary and logs live under `.aged/dev/`.

## API sketch

- `GET /api/health`
- `GET /api/auth/me`
- `GET /api/snapshot`
- `GET /api/projects`
- `POST /api/projects`
- `GET /api/plugins`
- `GET /api/events?after=<id>`
- `GET /api/events/stream?after=<id>`
- `POST /api/assistant`
- `GET /api/tasks/lookup?source=<source>&externalId=<id>`
- `POST /api/tasks`
- `POST /api/tasks/clear-terminal`
- `POST /api/tasks/{id}/clear`
- `POST /api/tasks/{id}/steer`
- `POST /api/tasks/{id}/cancel`
- `POST /api/tasks/{id}/pull-request`
- `POST /api/pull-requests/{id}/refresh`
- `POST /api/pull-requests/{id}/babysit`
- `GET /api/workers/{id}/changes`
- `POST /api/workers/{id}/apply`
- `POST /api/workers/{id}/cancel`

External drivers can create tasks through the same endpoint as the UI. Include `source` and `externalId` to make creation idempotent across poller restarts. The built-in GitHub driver uses `source: "github-issue"` and `externalId: "owner/repo#123"`:

```sh
curl -X POST http://127.0.0.1:8787/api/tasks \
  -H 'content-type: application/json' \
  -d '{
    "projectId": "aged",
    "title": "GitHub issue owner/repo#123",
    "prompt": "Fix the issue described at owner/repo#123...",
    "source": "github",
    "externalId": "owner/repo#123",
    "metadata": {
      "repo": "owner/repo",
      "issue": 123,
      "url": "https://github.com/owner/repo/issues/123"
    }
  }'
```

Posting the same `source` and `externalId` again returns the existing visible task instead of scheduling duplicate work.

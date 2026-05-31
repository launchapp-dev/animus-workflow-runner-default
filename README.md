# animus-workflow-runner-default

Reference `workflow_runner` plugin for Animus v0.5, implemented as a lift of the
in-tree `workflow-runner-v2` crate. Speaks the v0.5 plugin JSON-RPC envelope
defined in `animus-plugin-protocol` and the workflow_runner typed surface defined
in `animus-workflow-runner-protocol`.

Methods implemented:

- `workflow/execute`
- `workflow/run_phase`

## Wire surface notes (v0.1.0)

### `workflow/execute.mcp_config` is threaded into phase execution

When the daemon includes `mcp_config` on a `WorkflowExecuteRequest`, the plugin
now plumbs it all the way to the per-phase runtime contract via
`inject_default_stdio_mcp_with_config` (codex P2 #1). Pre-`v0.1.0` the field
parsed correctly but was silently dropped — phase execution always fell back to
`McpRuntimeConfig::default()`.

Fields honored end-to-end:

- `endpoint`
- `transport`
- `stdio_command`
- `stdio_args_json`
- `agent_id`
- `schema_draft`

`workflow/run_phase` does not yet accept `mcp_config` on the wire (the protocol
request does not carry the field). The single-phase scheduler therefore
continues to fall back to the default at this entry point.

### `initialize.init_extensions.memory_mcp_stdio_command`

Optional. When the daemon supplies this init extension, the plugin uses the
provided binary path to construct the `animus.memory` MCP server entry instead
of probing for a sibling `animus` binary next to the plugin process (codex
P2 #4). This enables standalone plugin deployments where the Animus CLI is not
co-located with the plugin binary.

Accepted shapes (host may send any of these):

```json
{ "init_extensions": { "memory_mcp_stdio_command": "/opt/host/bin/animus" } }
```

```json
{ "init_extensions": { "memory_mcp_stdio_command": { "command": "/opt/host/bin/animus" } } }
```

```json
{ "init_extensions": { "memory_mcp_stdio_command": { "path": "/opt/host/bin/animus" } } }
```

Resolution precedence inside the plugin:

1. `init_extensions.memory_mcp_stdio_command` (if set on `initialize`)
2. Sibling `animus` binary next to the running plugin executable
3. Refuse memory MCP injection (the plugin will NOT self-launch as the memory
   MCP server — it does not speak that contract)

Daemon-side: callers SHOULD set `memory_mcp_stdio_command` on every
`initialize` call when memory MCP injection is expected to succeed in
standalone deployments.

### `initialize.init_extensions.project_binding`

Required. Carries `project_root` (string, required) and `repo_scope`
(string, optional). The plugin binds to this project root for the lifetime of
the process; subsequent `workflow/run_phase` calls whose `execution_cwd` does
not nest under the bound root are rejected with `PROJECT_BINDING_MISMATCH`
(-32104).

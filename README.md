# dsh-remote-sandbox

Production-grade remote execution world for [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) (`dsh`): move `ctx.fs` and `ctx.subprocess` into an [E2B](https://e2b.dev) sandbox that **survives crashes, stays alive under load, and keeps your workspace in sync** — the pieces the official `@deepseek-ai/dsh-e2b` proof-of-concept explicitly leaves out.

> Third-party plugin suite. Not affiliated with DeepSeek. MIT licensed.

## Why

dsh's capability seams let one provider swap move the whole execution world to a remote sandbox — Bash, PTY, and LSP follow `ctx.fs`/`ctx.subprocess` with no forks. The official `dsh-e2b` package proves the seam works but is, by its own README, a POC: the sandbox is *"deliberately ephemeral"*, dies after five minutes, keeps a process's complete output in host memory, and *"neither uploads nor synchronizes the host workspace"*. This suite makes that execution world usable for real work.

| | official `dsh-e2b` (POC) | `dsh-remote-sandbox` |
|---|---|---|
| Sandbox lifetime | fixed 5 min, then deleted | heartbeat keep-alive, lives as long as the task |
| Connection drop / pause | none — sandbox death is task death | transparent recovery: resume the paused sandbox (disk retained) |
| Hard crash | none | recreate + restore workspace from the last synced snapshot |
| Host workspace | not uploaded or synced (path-spelling mirror only) | tar IN at open, tar OUT on request/teardown, `.gitignore`-independent excludes |
| Process output memory | complete stream retained in host memory (POC admits it breaks the seam's bound) | bounded in-memory tail + optional host spill file — host memory is capped |
| Round trips per fs op | 3–8 SDK commands per mutation (`realpath`, stage, rename, xattr…) | **one** — the sidecar does read-modify-publish atomically in the sandbox |
| `glob`/`grep` (`tool-fs-search`) | **broken** in a remote world (see below) | works — ripgrep ships with the sidecar |
| Custom sync backend | n/a | `syncIn`/`syncOut` overridable from `cordis.yml` (`!!js`) |

## Architecture

```
host (dsh process)                          sandbox (E2B)
┌───────────────────────────┐               ┌──────────────────────────┐
│ agent loop, LLM key,      │               │ sidecar (one pure-JS      │
│ session, tools            │               │ bundle, no native deps)   │
│                           │  one WSS      │  ├─ fs primitives          │
│ dsh-fs-remote  ───────────┼──────────────►│  ├─ process trees          │
│ dsh-subprocess-remote ────┤  structured   │  └─ ripgrep                │
│ dsh-sandbox-keeper        │  protocol     │                            │
│  ├─ E2B lifecycle         │               │ workspace ◄─ tar IN/OUT    │
│  ├─ heartbeat / recovery  │               └──────────────────────────┘
│  └─ tar sync              │
└───────────────────────────┘
```

- **The sidecar** is one ~160 KB pure-JavaScript file (WebSocket server inlined, no native binding). The keeper uploads it and runs it with the sandbox's own Node. It executes primitives locally, so a filesystem mutation is one round trip instead of a chain of SDK shell commands.
- **The keeper** owns the sandbox: it deploys the sidecar, pushes the pause deadline forward to keep the sandbox alive, and on a dropped connection resumes the paused sandbox (disk, and via E2B's memory snapshot the sidecar itself, retained) or — only when the sandbox is truly gone — recreates it and restores the workspace. LLM and git credentials never enter the sandbox.
- **The providers** are thin adapters: every `ctx.fs`/`ctx.subprocess` call derives the current sidecar client through the keeper, so a recovery swaps the connection underneath without the provider noticing.

## Install

```sh
dsh plugin --profile <name> add dsh-remote-sandbox
export E2B_API_KEY=e2b_...     # get one at https://e2b.dev/dashboard
```

The `dsh-remote-sandbox` bundle layers over `dsh-base`: it disables the local `subprocess` and `fs-sandbox` providers and inserts the keeper plus the remote providers. Bash, terminal, and LSP compose above them unchanged.

To wire it by hand instead, disable `dsh-subprocess-local` and `dsh-fs-sandbox` in your `cordis.yml` and insert:

```yaml
- id: sandbox-keeper
  name: dsh-sandbox-keeper
  config:
    cwd: /home/user/workspace          # workspace path inside the sandbox
    hostWorkspace: !!js process.cwd()  # local project synced in/out
- id: subprocess-remote
  name: dsh-subprocess-remote
- id: fs-remote
  name: dsh-fs-remote
```

## Configuration (`dsh-sandbox-keeper`)

| Key | Default | Meaning |
|---|---|---|
| `apiKey` | `$E2B_API_KEY` | E2B API key; never forwarded into the sandbox |
| `template` | E2B base image | E2B template id (the base image ships Node) |
| `cwd` | `/home/user/workspace` | workspace path inside the sandbox |
| `hostWorkspace` | `process.cwd()` | host project synced in at open, out on request/teardown |
| `timeoutMs` | `300000` | pause deadline; the heartbeat pushes it forward |
| `heartbeatMs` | `30000` | keep-alive and liveness interval |
| `excludes` | `node_modules`, `.pnpm-store`, `.dsh-remote-sidecar` | path segments never synced (`.git` **is** synced) |
| `pauseOnDispose` | `true` | pause (retain disk) rather than kill on teardown |
| `syncIn` / `syncOut` | tar over the sidecar | override with your own function (`!!js`) for git, object storage, a PVC, etc. |

## The two upstream bugs this works around

Found while building against `deepseek-harness@0.1.0-rc.6`:

1. **`tool-fs-search` hardcodes the host ripgrep path.** [`runRipgrep`](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/fs/tool-fs-search/src/search-core.ts) spawns `resolveRgPath()` — the absolute path of the host's `@vscode/ripgrep` binary — as `argv[0]`, without going through `resolveExecutable`. The plugin is in the shipped `dsh-base` bundle, but any remote execution world lacks that host path, so `glob`/`grep` fail on every call. The official e2b example sidesteps it by not mounting the plugin. This suite ships ripgrep with the sidecar so the tool works.
2. **Sandbox permission modes have no remote semantics.** `dsh-fs-sandbox` extends `LocalFileSystem` and `writableRoots()` mixes in the host's `/tmp`/`os.tmpdir()`, so `read-only` and `workspace-write` silently do not apply in a remote world — the e2b example is forced to hardcode `danger-full-access`. The sidecar enforces the mode itself against the sandbox's own paths.

## Known limitations and deferred work

- **Terminal (`spawnTerminal`) is experimental and not yet covered by tests.** The remote sandbox targets non-interactive agent work — read/write/edit, build, test, grep — which never needs a pty; that path is fully tested. Interactive terminals are the rare case, so `spawnTerminal` ships as best-effort: it runs over E2B's native pty (output, input, resize, and kill are real), but that API exposes no foreground process-group or input-waiting facts, so `inspectForeground` reports the session pid and `inputWaiting: false`, and no test exercises it yet. **Planned rework:** move the pty into the sidecar via a prebuilt `node-pty`, giving it a real foreground group (`tcgetpgrp`) and input-wait detection — full parity with the local terminal, and it brings the pty onto the same sidecar channel as everything else. If your deployment does not open interactive terminals, this does not affect you.
- **Recovery is at-least-once for the moments around a drop.** File writes or commands issued in the seconds before a crash may not have taken effect after recovery; the keeper injects a model-visible notice (via `notify` / `consumeRecoveryNotice()`) so the agent re-checks the tree.
- **OUT sync is low-frequency by design.** Recovery relies first on the paused sandbox's retained disk; the tar snapshot is the fallback for a truly deleted sandbox, so there is no per-write sync tax. Enable a periodic OUT via a custom `syncOut` if your deployment needs it.
- **Pre-release upstream.** dsh is in developer preview with no compatibility promise; peer versions are pinned loosely and may need bumping as the harness evolves.

## Development

```sh
pnpm install
pnpm -r run build          # tsc + esbuild the sidecar bundle
pnpm test                  # 37 unit + provider tests (no sandbox needed)
E2B_API_KEY=... pnpm test:e2e   # 6 real-E2B killer scenarios
```

The unit suite spawns the real sidecar bundle as a local process and drives the wire protocol, the providers through the real dsh `FileSystem`/`SubprocessRuntime` base classes, and the keeper's lifecycle/recovery/sync against a fake backend. The E2E suite runs the six scenarios — keep-alive, resume, recreate, bounded grep, tar round-trip, custom sync — against real E2B sandboxes.

## Packages

| Package | Role |
|---|---|
| `dsh-remote-protocol` | wire protocol + codec shared by sidecar and host |
| `dsh-remote-sidecar` | the in-sandbox executor (single bundled file) |
| `dsh-sandbox-keeper` | `ctx.remoteSandbox`: E2B lifecycle, heartbeat, recovery, sync |
| `dsh-fs-remote` | `ctx.fs` provider |
| `dsh-subprocess-remote` | `ctx.subprocess` provider |
| `dsh-remote-sandbox` | one-line bundle that wires all three over `dsh-base` |

## License

[MIT](LICENSE)

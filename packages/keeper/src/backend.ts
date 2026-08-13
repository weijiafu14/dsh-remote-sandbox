/**
 * Backend-neutral sandbox interface plus the E2B implementation. The keeper
 * talks only to `SandboxBackend`/`SandboxHandle`, so a second backend (a
 * self-hosted pool, another cloud) drops in without touching lifecycle,
 * recovery, sync, or the providers. The E2B binding is the one place the SDK is
 * used; providers never import it.
 * @module dsh-sandbox-keeper/backend
 */

import { Sandbox } from 'e2b'

/** A host-side pseudo-terminal in the sandbox, backend-native. */
export interface PtyPrimitive {
  readonly pid: number
  onData(cb: (bytes: Uint8Array) => void): void
  write(bytes: Uint8Array): Promise<void>
  resize(cols: number, rows: number): Promise<void>
  kill(): Promise<void>
  readonly exit: Promise<{ exitCode: number | null; signal: string | null }>
}

/** One live sandbox: lifecycle, port exposure, file bootstrap, sidecar launch, terminals. */
export interface SandboxHandle {
  readonly id: string
  /** WebSocket URL the exposed sidecar port is reachable at from the host. */
  sidecarUrl(port: number): string
  setTimeout(ms: number): Promise<void>
  /** Pause the sandbox, retaining its disk for a later resume (keep-alive without kill). */
  pause(): Promise<void>
  kill(): Promise<void>
  isRunning(): Promise<boolean>
  writeFile(path: string, bytes: Uint8Array): Promise<void>
  /** Launch the sidecar as a background process; returns once started. */
  startSidecar(command: string, env: Record<string, string>): Promise<void>
  openPty(opts: { cwd: string; env: Record<string, string>; rows: number; cols: number }): Promise<PtyPrimitive>
}

/** Creates and reconnects sandboxes for one backend. */
export interface SandboxBackend {
  readonly name: string
  create(opts: { timeoutMs: number; envs: Record<string, string>; metadata: Record<string, string> }): Promise<SandboxHandle>
  /** Reconnect to an existing sandbox by id, resuming it when paused. */
  connect(id: string): Promise<SandboxHandle>
}

/** E2B-backed sandbox handle. */
class E2BHandle implements SandboxHandle {
  constructor(private readonly sandbox: Sandbox, private readonly secure: boolean) {}

  get id(): string { return this.sandbox.sandboxId }

  sidecarUrl(port: number): string {
    const host = this.sandbox.getHost(port)
    return `${this.secure ? 'wss' : 'ws'}://${host}`
  }

  async setTimeout(ms: number): Promise<void> { await this.sandbox.setTimeout(ms) }
  async pause(): Promise<void> { await this.sandbox.betaPause() }
  async kill(): Promise<void> { await this.sandbox.kill() }
  async isRunning(): Promise<boolean> { return this.sandbox.isRunning() }

  async writeFile(path: string, bytes: Uint8Array): Promise<void> {
    const buffer = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
    await this.sandbox.files.write(path, buffer)
  }

  async startSidecar(command: string, env: Record<string, string>): Promise<void> {
    await this.sandbox.commands.run(command, {
      background: true,
      envs: env,
      onStderr: (data: string) => { if (process.env.DSH_REMOTE_DEBUG) process.stderr.write(`[sidecar:err] ${data}`) },
      onStdout: (data: string) => { if (process.env.DSH_REMOTE_DEBUG) process.stderr.write(`[sidecar:out] ${data}`) },
    })
  }

  async openPty(opts: { cwd: string; env: Record<string, string>; rows: number; cols: number }): Promise<PtyPrimitive> {
    let onData: ((bytes: Uint8Array) => void) | undefined
    const handle = await this.sandbox.pty.create({
      cols: opts.cols,
      rows: opts.rows,
      cwd: opts.cwd,
      envs: opts.env,
      onData: bytes => onData?.(bytes),
    })
    const sandbox = this.sandbox
    const pid = handle.pid
    const exit = handle.wait()
      .then(r => ({ exitCode: r.exitCode ?? null, signal: null as string | null }))
      .catch(() => ({ exitCode: null as number | null, signal: 'SIGKILL' as string | null }))
    return {
      pid,
      onData: cb => { onData = cb },
      write: bytes => sandbox.pty.sendInput(pid, bytes),
      resize: (cols, rows) => sandbox.pty.resize(pid, { cols, rows }),
      kill: async () => { await sandbox.pty.kill(pid) },
      exit,
    }
  }
}

/** E2B backend: creates secure sandboxes that pause (not die) on timeout. */
export class E2BBackend implements SandboxBackend {
  readonly name = 'e2b'

  constructor(private readonly apiKey: string, private readonly template: string | undefined, private readonly secure: boolean) {}

  async create(opts: { timeoutMs: number; envs: Record<string, string>; metadata: Record<string, string> }): Promise<SandboxHandle> {
    const sandbox = this.template === undefined
      ? await Sandbox.create({ apiKey: this.apiKey, timeoutMs: opts.timeoutMs, secure: this.secure, envs: opts.envs, metadata: opts.metadata, lifecycle: { onTimeout: 'pause' } })
      : await Sandbox.create(this.template, { apiKey: this.apiKey, timeoutMs: opts.timeoutMs, secure: this.secure, envs: opts.envs, metadata: opts.metadata, lifecycle: { onTimeout: 'pause' } })
    return new E2BHandle(sandbox, this.secure)
  }

  async connect(id: string): Promise<SandboxHandle> {
    const sandbox = await Sandbox.connect(id, { apiKey: this.apiKey })
    return new E2BHandle(sandbox, this.secure)
  }
}

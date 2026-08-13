/**
 * The remote-sandbox lifecycle owner. It creates one sandbox, deploys and runs
 * the sidecar, keeps the sandbox alive by pushing its pause deadline forward,
 * and recovers transparently when the connection drops: resume the paused
 * sandbox (disk, and with E2B's memory snapshot often the sidecar itself,
 * retained) or, only when the sandbox is truly gone, recreate it and restore the
 * last OUT snapshot. Providers read the current client through `rpc()` and never
 * see the swap; the model is told through a one-shot recovery notice.
 * @module dsh-sandbox-keeper/keeper
 */

import { readFile } from 'node:fs/promises'
import { sidecarBundlePath } from 'dsh-remote-sidecar'
import { SidecarClient } from './client.js'
import type { PtyPrimitive, SandboxBackend, SandboxHandle } from './backend.js'
import { DEFAULT_EXCLUDES, defaultSyncIn, defaultSyncOut } from './sync.js'
import type { SyncFn } from './sync.js'

/** The capability the keeper provides to the fs and subprocess providers. */
export interface RemoteSandbox {
  readonly cwd: string
  readonly runtimeRoot: string
  /**
   * The current live sidecar client, recovering the sandbox first if the
   * connection has dropped. Callers derive it per operation and never cache it.
   * @param signal - aborts a recovery wait.
   * @returns a connected client.
   */
  rpc(signal?: AbortSignal): Promise<SidecarClient>
  /** Open a backend-native terminal in the current sandbox. */
  openPty(spec: { cwd: string; env: Record<string, string>; rows: number; cols: number }): Promise<PtyPrimitive>
  /** Force a workspace OUT sync now. */
  syncOut(): Promise<void>
  /** Drain the one-shot recovery notice for the model, if a recovery just happened. */
  consumeRecoveryNotice(): string | undefined
}

/** Validated keeper configuration. */
export interface KeeperOptions {
  cwd: string
  runtimeRoot: string
  sidecarPort: number
  timeoutMs: number
  heartbeatMs: number
  excludes: readonly string[]
  snapshotDir: string
  syncIn: SyncFn
  syncOut: SyncFn
  /** Called with the model-facing recovery notice when a recovery completes; the
   * host wires it to `agent.inject()` with the message shape it owns. */
  notify?: (text: string) => void
}

const CONNECT_RETRIES = 5
const CONNECT_RETRY_DELAY_MS = 400

/** Owns one sandbox's full lifecycle and exposes the {@link RemoteSandbox} capability. */
export class Keeper implements RemoteSandbox {
  private handle: SandboxHandle | undefined
  private client: SidecarClient | undefined
  private recovering: Promise<SidecarClient> | undefined
  private heartbeat: ReturnType<typeof setInterval> | undefined
  private recoveryNotice: string | undefined
  private token = ''
  private disposed = false

  constructor(private readonly backend: SandboxBackend, private readonly opts: KeeperOptions) {}

  get cwd(): string { return this.opts.cwd }
  get runtimeRoot(): string { return this.opts.runtimeRoot }

  /** Create the sandbox, deploy the sidecar, run the initial IN sync, and start keep-alive. */
  async init(): Promise<void> {
    this.handle = await this.backend.create({
      timeoutMs: this.opts.timeoutMs,
      envs: {},
      metadata: { 'dsh-remote-sandbox': '1' },
    })
    this.token = randomToken()
    await this.deploySidecar(this.handle, this.token)
    this.client = await this.connectSidecar(this.handle, this.token)
    await this.ensureDir(this.opts.cwd)
    await this.runSync(this.opts.syncIn)
    this.startHeartbeat()
  }

  /** Create a directory tree in the sandbox (the workspace root before an IN sync). */
  private async ensureDir(dir: string): Promise<void> {
    if (this.client === undefined) throw new Error('ensureDir requires a live client')
    const proc = await this.client.spawn({
      argv: ['mkdir', '-p', dir], cwd: '/',
      stdout: 'ignore', stderr: 'collect', stdin: 'ignore',
      env: { PATH: '/usr/bin:/bin' }, graceMs: 3000,
    })
    const exit = await proc.exit
    if (exit.exitCode !== 0) throw new Error(`mkdir -p ${dir} exited ${exit.exitCode ?? exit.signal}`)
  }

  async rpc(signal?: AbortSignal): Promise<SidecarClient> {
    if (this.disposed) throw new Error('keeper is disposed')
    if (this.client !== undefined && !this.client.closed) return this.client
    if (this.recovering === undefined) this.recovering = this.recover().finally(() => { this.recovering = undefined })
    const recovered = this.recovering
    if (signal === undefined) return recovered
    return Promise.race([
      recovered,
      new Promise<never>((_, reject) => signal.addEventListener('abort', () => reject(new Error('rpc wait aborted')), { once: true })),
    ])
  }

  async openPty(spec: { cwd: string; env: Record<string, string>; rows: number; cols: number }): Promise<PtyPrimitive> {
    await this.rpc() // ensure a live sandbox first
    if (this.handle === undefined) throw new Error('no sandbox handle')
    return this.handle.openPty(spec)
  }

  async syncOut(): Promise<void> {
    await this.runSync(this.opts.syncOut)
  }

  consumeRecoveryNotice(): string | undefined {
    const notice = this.recoveryNotice
    this.recoveryNotice = undefined
    return notice
  }

  /** Pause the sandbox (retaining disk), stop keep-alive, and close the client. */
  async shutdown(pause: boolean): Promise<void> {
    this.disposed = true
    if (this.heartbeat !== undefined) clearInterval(this.heartbeat)
    this.client?.close()
    if (this.handle !== undefined) {
      try {
        if (pause) await this.handle.pause()
        else await this.handle.kill()
      } catch {
        // teardown best-effort; a vanished sandbox needs no cleanup.
      }
    }
  }

  private async recover(): Promise<SidecarClient> {
    // First try to resume the existing sandbox by id: its disk, and via the
    // pause memory snapshot the sidecar process itself, are retained — so
    // reconnect to the surviving sidecar with the SAME token before redeploying.
    if (this.handle !== undefined) {
      try {
        const resumed = await this.backend.connect(this.handle.id)
        if (await resumed.isRunning()) {
          this.handle = resumed
          this.client = await this.connectSidecar(resumed, this.token).catch(async () => {
            this.token = randomToken()
            await this.deploySidecar(resumed, this.token)
            return this.connectSidecar(resumed, this.token)
          })
          await this.ensureDir(this.opts.cwd).catch(() => {})
          this.setRecoveryNotice(recoveryText('resumed', this.opts.cwd))
          this.startHeartbeat()
          return this.client
        }
      } catch {
        // fall through to recreate
      }
    }
    // The sandbox is truly gone: recreate, restore the workspace, and continue.
    const fresh = await this.backend.create({ timeoutMs: this.opts.timeoutMs, envs: {}, metadata: { 'dsh-remote-sandbox': '1' } })
    this.handle = fresh
    this.token = randomToken()
    await this.deploySidecar(fresh, this.token)
    this.client = await this.connectSidecar(fresh, this.token)
    await this.ensureDir(this.opts.cwd)
    await this.runSync(this.opts.syncIn).catch(() => {})
    this.setRecoveryNotice(recoveryText('recreated', this.opts.cwd))
    this.startHeartbeat()
    return this.client
  }

  private setRecoveryNotice(text: string): void {
    this.recoveryNotice = text
    this.opts.notify?.(text)
  }

  /** Upload the sidecar bundle and start it as a background process. */
  private async deploySidecar(handle: SandboxHandle, token: string): Promise<void> {
    const bundle = await readFile(sidecarBundlePath())
    const remoteBundle = `${this.opts.runtimeRoot}/sidecar.cjs`
    await handle.writeFile(remoteBundle, bundle)
    await handle.startSidecar(`node ${remoteBundle}`, {
      DSH_SIDECAR_TOKEN: token,
      DSH_SIDECAR_PORT: String(this.opts.sidecarPort),
      DSH_SIDECAR_CWD: this.opts.cwd,
      DSH_SIDECAR_RUNTIME_ROOT: this.opts.runtimeRoot,
    })
  }

  /** Connect the RPC client to a running sidecar, retrying until it answers `hello`. */
  private async connectSidecar(handle: SandboxHandle, token: string): Promise<SidecarClient> {
    const url = handle.sidecarUrl(this.opts.sidecarPort)
    let lastErr: unknown
    for (let attempt = 0; attempt < CONNECT_RETRIES; attempt++) {
      try {
        const client = await SidecarClient.connect(url, token)
        await client.hello()
        return client
      } catch (err) {
        lastErr = err
        if (process.env['DSH_REMOTE_DEBUG']) process.stderr.write(`[keeper] connect attempt ${attempt} to ${url} failed: ${(err as Error).message}\n`)
        await delay(CONNECT_RETRY_DELAY_MS)
      }
    }
    throw new Error(`sidecar did not become reachable at ${url}: ${(lastErr as Error)?.message}`)
  }

  private startHeartbeat(): void {
    if (this.heartbeat !== undefined) clearInterval(this.heartbeat)
    this.heartbeat = setInterval(() => { void this.beat() }, this.opts.heartbeatMs)
    this.heartbeat.unref?.()
  }

  private async beat(): Promise<void> {
    if (this.disposed || this.handle === undefined) return
    try {
      // Push the pause deadline forward so the sandbox stays alive under load.
      await this.handle.setTimeout(this.opts.timeoutMs)
      if (this.client !== undefined && !this.client.closed) await this.client.ping()
    } catch {
      // A failed beat means the connection is suspect; the next rpc() recovers.
    }
  }

  private async runSync(fn: SyncFn): Promise<void> {
    if (this.client === undefined || this.handle === undefined) throw new Error('sync requires a live sandbox')
    await fn({
      client: this.client,
      sandbox: this.handle,
      sandboxCwd: this.opts.cwd,
      hostDir: this.opts.snapshotDir,
      excludes: this.opts.excludes,
    })
  }
}

function recoveryText(kind: 'resumed' | 'recreated', cwd: string): string {
  const detail = kind === 'resumed'
    ? 'the sandbox was resumed from its retained disk'
    : 'the sandbox was recreated and its workspace restored from the last synced snapshot'
  return `[remote-sandbox] The execution sandbox was recovered after a connection loss: ${detail} at ${cwd}. Any file writes or commands from the moments just before the recovery may not have taken effect — re-check the working tree before relying on them.`
}

function randomToken(): string {
  const bytes = new Uint8Array(24)
  globalThis.crypto.getRandomValues(bytes)
  return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('')
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => { const t = setTimeout(resolve, ms); t.unref?.() })
}

export { DEFAULT_EXCLUDES, defaultSyncIn, defaultSyncOut }

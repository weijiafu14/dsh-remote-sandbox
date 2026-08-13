/**
 * `dsh-subprocess-remote` — a `ctx.subprocess` provider over the sidecar. It
 * spawns managed process trees in the sandbox with real signals and
 * tree-scoped termination, bounds each collected stream to a host cap with an
 * optional spill file, and opens terminals through the keeper's backend-native
 * pty. `spawn` returns its handle synchronously per the seam contract while the
 * pid resolves asynchronously.
 * @module dsh-subprocess-remote
 */

import { PassThrough } from 'node:stream'
import type { Readable, Writable } from 'node:stream'
import { Context } from '@deepseek-ai/cordis'
import { SubprocessRuntime, scrubbedParentEnv } from '@deepseek-ai/dsh-subprocess'
import type {
  SubprocessHandle,
  SubprocessSpawnSpec,
  SubprocessTerminalHandle,
  SubprocessTerminalSignal,
  SubprocessTerminalSpawnSpec,
  SubprocessOutputReader,
} from '@deepseek-ai/dsh-subprocess'
import { RemoteError } from 'dsh-sandbox-keeper'
import type { ProcChannel, SidecarClient } from 'dsh-sandbox-keeper'
import { CollectedStream } from './collected.js'

const SIGNAL_TO_CONTROL: Record<SubprocessTerminalSignal, string> = {
  SIGINT: '\x03', SIGTSTP: '\x1a', SIGKILL: '', SIGTERM: '', SIGHUP: '',
}

/** Subprocess provider over the sidecar; terminals use the keeper's pty. */
export default class RemoteSubprocessRuntime extends SubprocessRuntime {
  /** The keeper must provide `ctx.remoteSandbox` before this provider loads. */
  static inject = ['remoteSandbox']

  private rpc(signal?: AbortSignal): Promise<SidecarClient> {
    return (this.ctx as Context).remoteSandbox.rpc(signal)
  }

  async resolveExecutable(command: string, env?: Readonly<Record<string, string>>, signal?: AbortSignal): Promise<string> {
    const client = await this.rpc(signal)
    try {
      return await client.resolveExecutable(command, { ...scrubbedParentEnv(), ...env })
    } catch (err) {
      if (err instanceof RemoteError) throw new Error(`resolveExecutable: ${err.message}`)
      throw err
    }
  }

  spawn(spec: SubprocessSpawnSpec): SubprocessHandle {
    return new RemoteHandle(this.rpc(spec.signal), spec)
  }

  async spawnTerminal(spec: SubprocessTerminalSpawnSpec): Promise<SubprocessTerminalHandle> {
    const remoteSandbox = (this.ctx as Context).remoteSandbox
    const env = { ...scrubbedParentEnv(), ...spec.env }
    const pty = await remoteSandbox.openPty({ cwd: spec.cwd, env, rows: spec.rows, cols: spec.cols })
    const output = new PassThrough()
    pty.onData(bytes => output.write(Buffer.from(bytes)))
    void pty.exit.then(() => output.end())
    return {
      pid: pty.pid,
      output,
      done: pty.exit.then(o => ({ exitCode: o.exitCode, signal: o.signal as NodeJS.Signals | null })),
      write: data => pty.write(new TextEncoder().encode(data)),
      // The backend pty exposes no foreground-group facts; report the session pid
      // as its own group and leave input-wait unproven (documented limitation).
      inspectForeground: async () => ({ processGroupId: pty.pid, inputWaiting: false }),
      signalForeground: async (signal: SubprocessTerminalSignal) => {
        const control = SIGNAL_TO_CONTROL[signal]
        if (control.length > 0) await pty.write(new TextEncoder().encode(control))
        else await pty.kill()
        return pty.pid
      },
      terminate: async () => { await pty.kill(); await pty.exit },
    }
  }
}

/** A subprocess handle whose channel resolves asynchronously behind a stable façade. */
class RemoteHandle implements SubprocessHandle {
  private _pid = -1
  private channel: ProcChannel | undefined
  private terminateRequested = false
  private readonly stdinBuffer: Uint8Array[] = []
  private stdinClosed = false
  readonly stdin: Writable | undefined
  readonly stdout: Readable | undefined
  readonly stderr: Readable | undefined
  private readonly stdoutPipe: PassThrough | undefined
  private readonly stderrPipe: PassThrough | undefined
  private readonly stdoutCollector: CollectedStream | undefined
  private readonly stderrCollector: CollectedStream | undefined
  readonly done: Promise<{ exitCode: number | null; signal: NodeJS.Signals | null }>
  private settleDone!: (outcome: { exitCode: number | null; signal: NodeJS.Signals | null }) => void

  constructor(clientPromise: Promise<SidecarClient>, spec: SubprocessSpawnSpec) {
    const stdinPipe = spec.stdio.stdin === 'pipe' ? new PassThrough() : undefined
    this.stdin = stdinPipe
    this.stdoutPipe = spec.stdio.stdout === 'pipe' ? new PassThrough() : undefined
    this.stderrPipe = spec.stdio.stderr === 'pipe' ? new PassThrough() : undefined
    this.stdout = this.stdoutPipe
    this.stderr = this.stderrPipe
    this.stdoutCollector = collectorFor(spec.stdio.stdout)
    this.stderrCollector = collectorFor(spec.stdio.stderr)
    this.done = new Promise(resolve => { this.settleDone = resolve })

    if (stdinPipe !== undefined) {
      stdinPipe.on('data', (chunk: Buffer) => this.pushStdin(chunk))
      stdinPipe.on('end', () => this.endStdin())
    }

    void this.launch(clientPromise, spec)
  }

  get pid(): number { return this._pid }

  get collected(): { stdout?: SubprocessOutputReader; stderr?: SubprocessOutputReader } {
    const out: { stdout?: SubprocessOutputReader; stderr?: SubprocessOutputReader } = {}
    if (this.stdoutCollector !== undefined) out.stdout = this.stdoutCollector
    if (this.stderrCollector !== undefined) out.stderr = this.stderrCollector
    return out
  }

  terminate(): void {
    if (this.channel !== undefined) this.channel.terminate()
    else this.terminateRequested = true
  }

  async waitForExit(signal?: AbortSignal): Promise<boolean> {
    if (signal === undefined) { await this.done; return true }
    return Promise.race([
      this.done.then(() => true),
      new Promise<boolean>(resolve => signal.addEventListener('abort', () => resolve(false), { once: true })),
    ])
  }

  private async launch(clientPromise: Promise<SidecarClient>, spec: SubprocessSpawnSpec): Promise<void> {
    let client: SidecarClient
    try {
      client = await clientPromise
    } catch {
      this.settleDone({ exitCode: null, signal: 'SIGABRT' as NodeJS.Signals })
      return
    }
    const stdinData = typeof spec.stdio.stdin === 'object' ? spec.stdio.stdin.data : undefined
    let channel: ProcChannel
    try {
      channel = await client.spawn({
        argv: spec.argv,
        cwd: spec.cwd,
        stdout: dispositionOf(spec.stdio.stdout),
        stderr: dispositionOf(spec.stdio.stderr),
        stdin: stdinData !== undefined ? { data: stdinData } : spec.stdio.stdin === 'pipe' ? 'pipe' : 'ignore',
        env: mergeEnv(spec.env),
        graceMs: spec.graceMs,
      })
    } catch {
      this.settleDone({ exitCode: null, signal: 'SIGABRT' as NodeJS.Signals })
      return
    }
    this.channel = channel
    this._pid = channel.pid
    channel.onStdout(bytes => { this.stdoutPipe?.write(Buffer.from(bytes)); this.stdoutCollector?.push(bytes) })
    channel.onStderr(bytes => { this.stderrPipe?.write(Buffer.from(bytes)); this.stderrCollector?.push(bytes) })
    void channel.exit.then(outcome => {
      this.stdoutPipe?.end(); this.stderrPipe?.end()
      this.stdoutCollector?.finish(); this.stderrCollector?.finish()
      this.settleDone({ exitCode: outcome.exitCode, signal: outcome.signal as NodeJS.Signals | null })
    })
    for (const chunk of this.stdinBuffer) channel.writeStdin(chunk)
    this.stdinBuffer.length = 0
    if (this.stdinClosed) channel.endStdin()
    if (this.terminateRequested) channel.terminate()
    if (spec.signal?.aborted) channel.terminate()
    spec.signal?.addEventListener('abort', () => channel.terminate(), { once: true })
  }

  private pushStdin(chunk: Uint8Array): void {
    if (this.channel !== undefined) this.channel.writeStdin(chunk)
    else this.stdinBuffer.push(chunk)
  }

  private endStdin(): void {
    if (this.channel !== undefined) this.channel.endStdin()
    else this.stdinClosed = true
  }
}

function collectorFor(mode: SubprocessSpawnSpec['stdio']['stdout']): CollectedStream | undefined {
  if (typeof mode !== 'object') return undefined
  return new CollectedStream(mode.maxBytes, mode.spill?.maxBytes)
}

function dispositionOf(mode: SubprocessSpawnSpec['stdio']['stdout']): 'collect' | 'ignore' | 'pipe' {
  if (mode === 'inherit') return 'pipe'
  if (mode === 'pipe') return 'pipe'
  if (typeof mode === 'object') return 'collect'
  return 'ignore'
}

function mergeEnv(env: NodeJS.ProcessEnv | undefined): Record<string, string> {
  const base = scrubbedParentEnv()
  if (env === undefined) return base
  for (const [k, v] of Object.entries(env)) {
    if (v === undefined) delete base[k]
    else base[k] = v
  }
  return base
}

export { RemoteSubprocessRuntime }

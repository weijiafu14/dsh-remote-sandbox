/**
 * Managed process trees inside the sandbox. Each spawn is a detached process
 * group so termination signals the whole tree, output streams to the host as
 * raw bytes (the host owns the bounded buffer and UTF-8 decoding), and exit
 * facts arrive as one event. No output is buffered here beyond the OS pipe, so
 * the sidecar never accumulates a process's complete output in memory.
 * @module dsh-remote-sidecar/proc-ops
 */

import { spawn as nodeSpawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import type { SpawnParams } from 'dsh-remote-protocol'
import { OpError } from './util.js'

/** How the sidecar delivers a managed process's output and lifecycle to the host. */
export interface ProcSink {
  chunk(ch: number, stream: 0 | 1, bytes: Uint8Array): void
  streamEnd(ch: number, stream: 'stdout' | 'stderr'): void
  exit(ch: number, exitCode: number | null, signal: string | null): void
}

interface Managed {
  child: ChildProcess
  pgid: number
  graceMs: number
  killTimer?: NodeJS.Timeout
  exited: boolean
  waiters: Array<() => void>
}

/** Owns every live managed process keyed by channel; enforces tree teardown on disposal. */
export class ProcRegistry {
  private readonly procs = new Map<number, Managed>()

  constructor(private readonly sink: ProcSink) {}

  /**
   * Spawn one managed process group.
   * @param ch - the channel id correlating output and lifecycle frames.
   * @param spec - the fully specified spawn request.
   * @returns the tree-root pid.
   */
  spawn(ch: number, spec: SpawnParams): number {
    const [program, ...args] = spec.argv
    if (program === undefined) throw new OpError('PROC_SPAWN_FAILED', 'argv is empty')
    const stdin = spec.stdin === 'ignore' ? 'ignore' : 'pipe'
    const stdout = spec.stdout === 'ignore' ? 'ignore' : 'pipe'
    const stderr = spec.stderr === 'ignore' ? 'ignore' : 'pipe'
    let child: ChildProcess
    try {
      child = nodeSpawn(program, args, {
        cwd: spec.cwd,
        env: spec.env,
        detached: true,
        stdio: [stdin, stdout, stderr],
      })
    } catch (err) {
      throw new OpError('PROC_SPAWN_FAILED', `spawn failed: ${(err as Error).message}`)
    }
    const pid = child.pid
    if (pid === undefined) throw new OpError('PROC_SPAWN_FAILED', 'spawn produced no pid')

    const managed: Managed = { child, pgid: pid, graceMs: spec.graceMs, exited: false, waiters: [] }
    this.procs.set(ch, managed)

    child.on('error', () => this.settle(ch, managed, null, 'SIGABRT'))

    let stdoutOpen = stdout === 'pipe'
    let stderrOpen = stderr === 'pipe'
    if (child.stdout) {
      child.stdout.on('data', (b: Buffer) => this.sink.chunk(ch, 0, b))
      child.stdout.on('end', () => { stdoutOpen = false; this.sink.streamEnd(ch, 'stdout') })
    }
    if (child.stderr) {
      child.stderr.on('data', (b: Buffer) => this.sink.chunk(ch, 1, b))
      child.stderr.on('end', () => { stderrOpen = false; this.sink.streamEnd(ch, 'stderr') })
    }

    if (typeof spec.stdin === 'object' && child.stdin) {
      child.stdin.end(spec.stdin.data)
    }

    child.on('close', (code, signal) => {
      void stdoutOpen; void stderrOpen
      this.settle(ch, managed, code, signal)
    })
    return pid
  }

  /**
   * Deliver raw input bytes to a piped stdin, byte-accurate.
   * @param ch - the process channel.
   * @param bytes - the bytes to write.
   */
  writeStdin(ch: number, bytes: Uint8Array): void {
    const m = this.procs.get(ch)
    m?.child.stdin?.write(Buffer.from(bytes))
  }

  /** Close a process's stdin. */
  endStdin(ch: number): void {
    const m = this.procs.get(ch)
    m?.child.stdin?.end()
  }

  /**
   * Begin SIGTERM → grace → SIGKILL escalation on the process group. Idempotent
   * and a no-op once the tree has exited (the pid may be reused).
   * @param ch - the process channel.
   */
  terminate(ch: number): void {
    const m = this.procs.get(ch)
    if (m === undefined || m.exited) return
    this.signalGroup(m, 'SIGTERM')
    if (m.killTimer === undefined) {
      m.killTimer = setTimeout(() => {
        if (!m.exited) this.signalGroup(m, 'SIGKILL')
      }, m.graceMs)
      m.killTimer.unref()
    }
  }

  /**
   * Resolve when the process tree has exited.
   * @param ch - the process channel.
   * @returns a promise that settles at tree exit (immediately if already exited).
   */
  waitForExit(ch: number): Promise<void> {
    const m = this.procs.get(ch)
    if (m === undefined || m.exited) return Promise.resolve()
    return new Promise<void>(resolve => m.waiters.push(resolve))
  }

  /** Terminate and await every managed process (service teardown). */
  async disposeAll(): Promise<void> {
    const pending = [...this.procs.keys()].map(async ch => {
      this.terminate(ch)
      await this.waitForExit(ch)
    })
    await Promise.all(pending)
  }

  private signalGroup(m: Managed, sig: NodeJS.Signals): void {
    try {
      process.kill(-m.pgid, sig)
    } catch {
      try {
        m.child.kill(sig)
      } catch {
        // process already gone; the pid may have been reused, so swallow.
      }
    }
  }

  private settle(ch: number, m: Managed, code: number | null, signal: string | null): void {
    if (m.exited) return
    m.exited = true
    if (m.killTimer !== undefined) clearTimeout(m.killTimer)
    this.procs.delete(ch)
    this.sink.exit(ch, code, signal)
    for (const w of m.waiters) w()
    m.waiters.length = 0
  }
}

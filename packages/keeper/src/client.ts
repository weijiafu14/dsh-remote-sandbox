/**
 * Host-side RPC client for the in-sandbox sidecar. One monotonic counter names
 * both request ids and stream channels, so a file-stream chunk and a process
 * chunk never collide. Every fs method is one awaited round trip; process output
 * and large reads arrive as byte-accurate binary frames the caller bounds.
 * @module dsh-sandbox-keeper/client
 */

import { WebSocket } from 'ws'
import {
  BIN_BYTES_REQUEST,
  BIN_BYTES_RESPONSE,
  BIN_STREAM_CHUNK,
  STREAM_STDERR,
  STREAM_STDOUT,
  decodeBinary,
  decodeControl,
  encodeBinary,
  encodeControl,
} from 'dsh-remote-protocol'
import type {
  ControlFrame,
  DirEntry,
  EditResult,
  HelloResult,
  LstatResult,
  StatResult,
  WriteResult,
} from 'dsh-remote-protocol'

/** A typed sidecar failure carrying the wire error code (an FsErrorCode or PROC_* token). */
export class RemoteError extends Error {
  constructor(readonly code: string, message: string) {
    super(message)
    this.name = 'RemoteError'
  }
}

/** Exit facts of a managed process. */
export interface ProcExit {
  exitCode: number | null
  signal: string | null
}

/** A live process on the sidecar: output callbacks, byte-accurate stdin, exit, and termination. */
export interface ProcChannel {
  readonly ch: number
  readonly pid: number
  onStdout(cb: (bytes: Uint8Array) => void): void
  onStderr(cb: (bytes: Uint8Array) => void): void
  writeStdin(bytes: Uint8Array): void
  endStdin(): void
  readonly exit: Promise<ProcExit>
  terminate(): void
  waitForExit(): Promise<void>
}

interface FileStreamSink {
  push(bytes: Uint8Array): void
  end(): void
  fail(err: Error): void
}

class ProcChannelImpl implements ProcChannel {
  pid = -1
  private stdoutCb?: (bytes: Uint8Array) => void
  private stderrCb?: (bytes: Uint8Array) => void
  private settleExit!: (exit: ProcExit) => void
  readonly exit: Promise<ProcExit>

  constructor(readonly ch: number, private readonly client: SidecarClient) {
    this.exit = new Promise<ProcExit>(resolve => { this.settleExit = resolve })
  }

  onStdout(cb: (bytes: Uint8Array) => void): void { this.stdoutCb = cb }
  onStderr(cb: (bytes: Uint8Array) => void): void { this.stderrCb = cb }
  routeChunk(stream: number, bytes: Uint8Array): void {
    if (stream === STREAM_STDOUT) this.stdoutCb?.(bytes)
    else if (stream === STREAM_STDERR) this.stderrCb?.(bytes)
  }
  settle(exit: ProcExit): void { this.settleExit(exit) }
  writeStdin(bytes: Uint8Array): void { this.client.sendBinary(BIN_BYTES_REQUEST, this.ch, bytes) }
  endStdin(): void { this.client.sendControl({ t: 'stdin-end', ch: this.ch }) }
  terminate(): void { void this.client.request('proc.terminate', { ch: this.ch }).catch(() => {}) }
  async waitForExit(): Promise<void> { await this.exit }
}

/** One authenticated RPC session to a sidecar. */
export class SidecarClient {
  private seq = 1
  private readonly pendingCtl = new Map<number, (frame: ControlFrame) => void>()
  private readonly pendingBytes = new Map<number, { resolve: (b: Uint8Array) => void; reject: (e: Error) => void }>()
  private readonly fileStreams = new Map<number, FileStreamSink>()
  private readonly procs = new Map<number, ProcChannelImpl>()
  private closedFlag = false
  private readonly closeCbs: Array<() => void> = []

  private constructor(private readonly ws: WebSocket) {
    ws.on('message', (data, isBinary) => this.onMessage(data as Buffer, isBinary))
    ws.on('close', () => this.onClosed())
    ws.on('error', () => this.onClosed())
  }

  /**
   * Connect and authenticate to a sidecar.
   * @param url - the ws(s) URL the sandbox exposes the sidecar port at.
   * @param token - the shared authentication token.
   * @param signal - aborts the connection attempt.
   * @returns a ready client.
   */
  static connect(url: string, token: string, signal?: AbortSignal): Promise<SidecarClient> {
    const full = `${url}${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}`
    const ws = new WebSocket(full)
    return new Promise<SidecarClient>((resolve, reject) => {
      const onAbort = (): void => { ws.terminate(); reject(new Error('sidecar connect aborted')) }
      if (signal?.aborted) return onAbort()
      signal?.addEventListener('abort', onAbort, { once: true })
      ws.on('open', () => { signal?.removeEventListener('abort', onAbort); resolve(new SidecarClient(ws)) })
      ws.on('error', err => { signal?.removeEventListener('abort', onAbort); reject(err) })
    })
  }

  get closed(): boolean { return this.closedFlag }
  onClose(cb: () => void): void { this.closeCbs.push(cb) }
  nextChannel(): number { return this.seq++ }

  hello(): Promise<HelloResult> { return this.request<HelloResult>('sys.hello', {}) }
  async ping(): Promise<void> { await this.request('sys.ping', {}) }
  realpath(path: string, cwd?: string): Promise<{ canonical: string }> { return this.request('fs.realpath', { path, cwd }) }
  stat(path: string): Promise<StatResult | null> { return this.request('fs.stat', { path }) }
  lstat(path: string, cwd?: string): Promise<LstatResult | null> { return this.request('fs.lstat', { path, cwd }) }
  async readText(path: string): Promise<string> { return (await this.request<{ content: string }>('fs.readText', { path })).content }
  listDir(path: string): Promise<{ entries: DirEntry[] }> { return this.request('fs.listDir', { path }) }
  writeText(params: object): Promise<WriteResult> { return this.request('fs.writeText', params) }
  editText(params: object): Promise<EditResult> { return this.request('fs.editText', params) }
  async resolveExecutable(command: string, env: Record<string, string>): Promise<string> {
    return (await this.request<{ path: string }>('proc.resolveExecutable', { command, env })).path
  }

  /**
   * Read raw bytes with a hard cap, returned as one correlated binary frame.
   * @param path - absolute path in the sandbox.
   * @param maxBytes - inclusive cap; the sidecar fails past it rather than truncating.
   * @param signal - aborts the read.
   * @returns the file bytes.
   */
  readBytes(path: string, maxBytes: number, signal?: AbortSignal): Promise<Uint8Array> {
    const id = this.seq++
    return new Promise<Uint8Array>((resolve, reject) => {
      if (this.closedFlag) return reject(new RemoteError('CLOSED', 'sidecar connection closed'))
      this.pendingBytes.set(id, { resolve, reject })
      this.pendingCtl.set(id, frame => {
        if (frame.t === 'res' && !frame.ok) { this.pendingBytes.delete(id); reject(new RemoteError(frame.error.code, frame.error.message)) }
      })
      signal?.addEventListener('abort', () => { this.sendControl({ t: 'cancel', id }); reject(new RemoteError('FS_ABORTED', 'read aborted')) }, { once: true })
      this.sendControl({ t: 'req', id, method: 'fs.readBytes', params: { path, maxBytes } })
    })
  }

  /**
   * Stream a text file as byte-accurate chunks; the caller decodes and bounds.
   * @param path - absolute path in the sandbox.
   * @param signal - aborts the stream.
   * @returns an async iterable of raw byte chunks.
   */
  streamText(path: string, signal?: AbortSignal): AsyncIterable<Uint8Array> {
    const ch = this.seq++
    const id = this.seq++
    const queue: Uint8Array[] = []
    let done = false
    let failure: Error | undefined
    let wake: (() => void) | undefined
    const sink: FileStreamSink = {
      push: bytes => { queue.push(bytes); wake?.() },
      end: () => { done = true; wake?.() },
      fail: err => { failure = err; done = true; wake?.() },
    }
    this.fileStreams.set(ch, sink)
    this.pendingCtl.set(id, frame => { if (frame.t === 'res' && !frame.ok) sink.fail(new RemoteError(frame.error.code, frame.error.message)) })
    signal?.addEventListener('abort', () => { this.sendControl({ t: 'cancel', id }); sink.fail(new RemoteError('FS_ABORTED', 'read aborted')) }, { once: true })
    this.sendControl({ t: 'req', id, method: 'fs.readTextStream', params: { path, ch } })
    const self = this
    return {
      async *[Symbol.asyncIterator]() {
        try {
          for (;;) {
            while (queue.length > 0) yield queue.shift() as Uint8Array
            if (failure) throw failure
            if (done) return
            await new Promise<void>(r => { wake = r })
          }
        } finally {
          self.fileStreams.delete(ch)
        }
      },
    }
  }

  /**
   * Spawn a managed process on the sidecar.
   * @param params - argv/cwd/stdio/env/grace, minus the channel (allocated here).
   * @returns the live process channel.
   */
  async spawn(params: Omit<import('dsh-remote-protocol').SpawnParams, 'ch'>): Promise<ProcChannel> {
    const ch = this.seq++
    const channel = new ProcChannelImpl(ch, this)
    this.procs.set(ch, channel)
    try {
      const res = await this.request<{ pid: number }>('proc.spawn', { ch, ...params })
      channel.pid = res.pid
    } catch (err) {
      this.procs.delete(ch)
      throw err
    }
    void channel.exit.then(() => this.procs.delete(ch))
    return channel
  }

  /** Close the connection and reject everything in flight. */
  close(): void { this.ws.close(); this.onClosed() }

  /**
   * Send one request and await its typed result.
   * @param method - the method token.
   * @param params - method parameters.
   * @returns the method result.
   */
  request<T>(method: string, params: unknown): Promise<T> {
    const id = this.seq++
    return new Promise<T>((resolve, reject) => {
      if (this.closedFlag) return reject(new RemoteError('CLOSED', 'sidecar connection closed'))
      this.pendingCtl.set(id, frame => {
        this.pendingCtl.delete(id)
        if (frame.t !== 'res') return
        if (frame.ok) resolve(frame.result as T)
        else reject(new RemoteError(frame.error.code, frame.error.message))
      })
      this.sendControl({ t: 'req', id, method: method as never, params })
    })
  }

  sendControl(frame: ControlFrame): void {
    if (this.ws.readyState === this.ws.OPEN) this.ws.send(encodeControl(frame))
  }

  sendBinary(kind: number, id: number, payload: Uint8Array): void {
    if (this.ws.readyState === this.ws.OPEN) this.ws.send(encodeBinary(kind, id, payload))
  }

  private onMessage(data: Buffer, isBinary: boolean): void {
    if (isBinary) {
      const frame = decodeBinary(data)
      if (frame.kind === BIN_BYTES_RESPONSE) {
        const waiter = this.pendingBytes.get(frame.id)
        if (waiter) { this.pendingBytes.delete(frame.id); this.pendingCtl.delete(frame.id); waiter.resolve(new Uint8Array(frame.payload)) }
      } else if (frame.kind === BIN_STREAM_CHUNK) {
        this.fileStreams.get(frame.id)?.push(new Uint8Array(frame.payload))
        this.procs.get(frame.id)?.routeChunk(frame.stream, new Uint8Array(frame.payload))
      }
      return
    }
    let frame: ControlFrame
    try { frame = decodeControl(data.toString()) } catch { return }
    if (frame.t === 'res') this.pendingCtl.get(frame.id)?.(frame)
    else if (frame.t === 'end') this.fileStreams.get(frame.ch)?.end()
    else if (frame.t === 'event' && frame.kind === 'proc-exit') this.procs.get(frame.ch)?.settle({ exitCode: frame.exitCode, signal: frame.signal })
  }

  private onClosed(): void {
    if (this.closedFlag) return
    this.closedFlag = true
    const err = new RemoteError('CLOSED', 'sidecar connection closed')
    for (const waiter of this.pendingCtl.values()) waiter({ t: 'res', id: -1, ok: false, error: { code: 'CLOSED', message: err.message } })
    for (const waiter of this.pendingBytes.values()) waiter.reject(err)
    for (const stream of this.fileStreams.values()) stream.fail(err)
    for (const proc of this.procs.values()) proc.settle({ exitCode: null, signal: 'SIGKILL' })
    this.pendingCtl.clear(); this.pendingBytes.clear(); this.fileStreams.clear(); this.procs.clear()
    for (const cb of this.closeCbs) cb()
  }
}

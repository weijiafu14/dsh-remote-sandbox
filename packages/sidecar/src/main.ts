/**
 * Sidecar entry point: a token-authenticated WebSocket server the keeper
 * uploads and runs inside the sandbox. It dispatches fs and process primitives,
 * streams process output and large reads as byte-accurate binary frames, and
 * keeps its control state in memory only so untrusted code sharing the sandbox
 * cannot forge exit facts or read pending request state.
 * @module dsh-remote-sidecar/main
 */

import { createReadStream } from 'node:fs'
import { open } from 'node:fs/promises'
import { WebSocketServer } from 'ws'
import type { RawData, WebSocket } from 'ws'
import {
  BIN_BYTES_REQUEST,
  BIN_BYTES_RESPONSE,
  BIN_STREAM_CHUNK,
  DEFAULT_SIDECAR_PORT,
  PROTOCOL_VERSION,
  STREAM_DATA,
  decodeBinary,
  decodeControl,
  encodeBinary,
  encodeControl,
} from 'dsh-remote-protocol'
import type { ControlFrame, EditTextParams, HelloResult, ReqFrame, SpawnParams, WriteTextParams } from 'dsh-remote-protocol'
import { OpError, throwFsErrno } from './util.js'
import * as fsOps from './fs-ops.js'
import { ProcRegistry } from './proc-ops.js'

const SIDECAR_VERSION = '0.1.0'
const READ_STREAM_CHUNK = 64 * 1024

const token = process.env['DSH_SIDECAR_TOKEN'] ?? ''
const port = Number(process.env['DSH_SIDECAR_PORT'] ?? DEFAULT_SIDECAR_PORT)
const cwd = process.env['DSH_SIDECAR_CWD'] ?? process.cwd()
const runtimeRoot = process.env['DSH_SIDECAR_RUNTIME_ROOT'] ?? `${cwd}/.dsh-remote-sidecar`

if (token.length === 0) {
  process.stderr.write('dsh-remote-sidecar: DSH_SIDECAR_TOKEN is required\n')
  process.exit(2)
}

const server = new WebSocketServer({ port, host: '0.0.0.0' })
server.on('listening', () => process.stdout.write(`dsh-remote-sidecar: listening on ${port}\n`))

server.on('connection', (ws, req) => {
  const url = new URL(req.url ?? '/', 'http://localhost')
  if (url.searchParams.get('token') !== token) {
    ws.close(4001, 'unauthorized')
    return
  }
  new Session(ws).start()
})

/** One authenticated connection: request dispatch, streaming, and process lifecycle. */
class Session {
  private readonly cancels = new Map<number, AbortController>()
  private readonly procs: ProcRegistry

  constructor(private readonly ws: WebSocket) {
    this.procs = new ProcRegistry({
      chunk: (ch, stream, bytes) => this.sendBinary(BIN_STREAM_CHUNK, ch, bytes, stream),
      streamEnd: (ch, stream) => this.sendControl({ t: 'end', ch, stream }),
      exit: (ch, exitCode, signal) => this.sendControl({ t: 'event', kind: 'proc-exit', ch, exitCode, signal }),
    })
  }

  start(): void {
    this.ws.on('message', (data, isBinary) => { void this.onMessage(data, isBinary) })
    this.ws.on('close', () => { void this.procs.disposeAll() })
  }

  private async onMessage(data: RawData, isBinary: boolean): Promise<void> {
    if (isBinary) {
      this.onBinary(data as Buffer)
      return
    }
    let frame: ControlFrame
    try {
      frame = decodeControl(data.toString())
    } catch {
      return
    }
    if (frame.t === 'req') await this.onRequest(frame)
    else if (frame.t === 'cancel') this.cancels.get(frame.id)?.abort()
    else if (frame.t === 'stdin-end') this.procs.endStdin(frame.ch)
  }

  private onBinary(buf: Buffer): void {
    const frame = decodeBinary(buf)
    if (frame.kind === BIN_BYTES_REQUEST) {
      // Piped stdin bytes for a process channel, byte-accurate.
      this.procs.writeStdin(frame.id, frame.payload)
    }
  }

  private async onRequest(req: ReqFrame): Promise<void> {
    const ac = new AbortController()
    this.cancels.set(req.id, ac)
    try {
      await this.dispatch(req, ac.signal)
    } catch (err) {
      const code = err instanceof OpError ? err.code : 'INTERNAL'
      this.sendControl({ t: 'res', id: req.id, ok: false, error: { code, message: (err as Error).message } })
    } finally {
      this.cancels.delete(req.id)
    }
  }

  private async dispatch(req: ReqFrame, signal: AbortSignal): Promise<void> {
    const p = req.params as Record<string, unknown>
    switch (req.method) {
      case 'sys.hello': {
        const result: HelloResult = { protocolVersion: PROTOCOL_VERSION, sidecarVersion: SIDECAR_VERSION, platform: process.platform, runtimeRoot }
        return this.ok(req.id, result)
      }
      case 'sys.ping':
        return this.ok(req.id, {})
      case 'fs.realpath':
        return this.ok(req.id, await fsOps.opRealpath((p['cwd'] as string | undefined) ?? cwd, p['path'] as string))
      case 'fs.stat':
        return this.ok(req.id, await fsOps.opStat(p['path'] as string))
      case 'fs.lstat':
        return this.ok(req.id, await fsOps.opLstat((p['cwd'] as string | undefined) ?? cwd, p['path'] as string))
      case 'fs.readText':
        return this.ok(req.id, await fsOps.opReadText(p['path'] as string))
      case 'fs.readBytes': {
        const bytes = await fsOps.opReadBytes(p['path'] as string, p['maxBytes'] as number)
        return this.sendBinary(BIN_BYTES_RESPONSE, req.id, bytes)
      }
      case 'fs.readTextStream':
        return this.streamText(req.id, p['ch'] as number, p['path'] as string, signal)
      case 'fs.listDir':
        return this.ok(req.id, await fsOps.opListDir(p['path'] as string))
      case 'fs.writeText':
        return this.ok(req.id, await fsOps.opWriteText(p as unknown as WriteTextParams))
      case 'fs.editText':
        return this.ok(req.id, await fsOps.opEditText(p as unknown as EditTextParams))
      case 'proc.resolveExecutable':
        return this.ok(req.id, await fsOps.opResolveExecutable(p['command'] as string, p['env'] as Record<string, string>))
      case 'proc.spawn': {
        const ch = p['ch'] as number
        const pid = this.procs.spawn(ch, p as unknown as SpawnParams)
        return this.ok(req.id, { ch, pid })
      }
      case 'proc.terminate':
        this.procs.terminate(p['ch'] as number)
        return this.ok(req.id, {})
      case 'proc.waitForExit':
        await this.procs.waitForExit(p['ch'] as number)
        return this.ok(req.id, {})
      default:
        throw new OpError('UNKNOWN_METHOD', `unknown method ${req.method}`)
    }
  }

  /** Stream a text file as byte-accurate DATA chunks on `ch`, validating UTF-8 as a whole. */
  private async streamText(reqId: number, ch: number, path: string, signal: AbortSignal): Promise<void> {
    const handle = await open(path, 'r').catch((err: unknown) => throwFsErrno(err, 'open failed'))
    try {
      const stat = await handle.stat()
      if (stat.isDirectory()) throw new OpError('FS_NOT_REGULAR_FILE', 'path is a directory')
    } finally {
      await handle.close()
    }
    const stream = createReadStream(path, { highWaterMark: READ_STREAM_CHUNK })
    const decoder = new TextDecoder('utf-8', { fatal: true })
    try {
      for await (const chunk of stream) {
        if (signal.aborted) throw new OpError('FS_ABORTED', 'read aborted')
        const bytes = chunk as Buffer
        if (bytes.includes(0)) throw new OpError('FS_NOT_TEXT', 'file contains NUL bytes')
        decoder.decode(bytes, { stream: true }) // validate incrementally; bytes forwarded raw
        this.sendBinary(BIN_STREAM_CHUNK, ch, bytes, STREAM_DATA)
      }
      decoder.decode() // flush; throws on a truncated multibyte sequence
      this.sendControl({ t: 'end', ch, stream: 'data' })
      this.ok(reqId, {})
    } catch (err) {
      if (err instanceof OpError) throw err
      throw new OpError('FS_NOT_TEXT', `file is not valid UTF-8 text: ${(err as Error).message}`)
    }
  }

  private ok(id: number, result: unknown): void {
    this.sendControl({ t: 'res', id, ok: true, result })
  }

  private sendControl(frame: ControlFrame): void {
    if (this.ws.readyState === this.ws.OPEN) this.ws.send(encodeControl(frame))
  }

  private sendBinary(kind: number, id: number, payload: Uint8Array, stream = 0): void {
    if (this.ws.readyState === this.ws.OPEN) this.ws.send(encodeBinary(kind, id, payload, stream))
  }
}

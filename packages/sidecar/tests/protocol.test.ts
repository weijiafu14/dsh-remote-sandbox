import { spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { WebSocket } from 'ws'
import {
  BIN_BYTES_RESPONSE,
  BIN_STREAM_CHUNK,
  decodeBinary,
  decodeControl,
  encodeControl,
} from 'dsh-remote-protocol'
import type { ControlFrame } from 'dsh-remote-protocol'

const bundle = fileURLToPath(new URL('../dist/sidecar.cjs', import.meta.url))
const TOKEN = 'test-token-123'
const PORT = 49331

let child: ChildProcess
let workdir: string

/** One correlated request/response over the live sidecar socket. */
class Client {
  private nextId = 1
  private readonly pendingCtl = new Map<number, (f: ControlFrame) => void>()
  private readonly pendingBin = new Map<number, (b: Uint8Array) => void>()
  readonly chunks = new Map<number, Buffer[]>()
  readonly ended = new Set<number>()
  readonly exits = new Map<number, { exitCode: number | null; signal: string | null }>()

  constructor(private readonly ws: WebSocket) {
    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        const f = decodeBinary(data as Buffer)
        if (f.kind === BIN_BYTES_RESPONSE) this.pendingBin.get(f.id)?.(f.payload)
        else if (f.kind === BIN_STREAM_CHUNK) {
          const list = this.chunks.get(f.id) ?? []
          list.push(Buffer.from(f.payload))
          this.chunks.set(f.id, list)
        }
        return
      }
      const frame = decodeControl(data.toString())
      if (frame.t === 'res') this.pendingCtl.get(frame.id)?.(frame)
      else if (frame.t === 'end') { /* stream end tracked via exits/chunks */ }
      else if (frame.t === 'event' && frame.kind === 'proc-exit') {
        this.exits.set(frame.ch, { exitCode: frame.exitCode, signal: frame.signal })
      }
    })
  }

  req(method: string, params: unknown): Promise<ControlFrame> {
    const id = this.nextId++
    return new Promise(resolve => {
      this.pendingCtl.set(id, resolve)
      this.ws.send(encodeControl({ t: 'req', id, method: method as never, params }))
    })
  }

  reqBytes(method: string, params: unknown): Promise<Uint8Array> {
    const id = this.nextId++
    return new Promise((resolve, reject) => {
      this.pendingBin.set(id, resolve)
      this.pendingCtl.set(id, (f) => { if (f.t === 'res' && !f.ok) reject(new Error(f.error.code)) })
      this.ws.send(encodeControl({ t: 'req', id, method: method as never, params }))
    })
  }
}

beforeAll(async () => {
  workdir = await mkdtemp(join(tmpdir(), 'dsh-sidecar-'))
  child = spawn(process.execPath, [bundle], {
    env: { ...process.env, DSH_SIDECAR_TOKEN: TOKEN, DSH_SIDECAR_PORT: String(PORT), DSH_SIDECAR_CWD: workdir },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('sidecar did not start')), 8000)
    child.stdout?.on('data', (b: Buffer) => { if (b.toString().includes('listening')) { clearTimeout(timer); resolve() } })
    child.on('error', reject)
  })
})

afterAll(async () => {
  child?.kill('SIGKILL')
  await rm(workdir, { recursive: true, force: true })
})

async function connect(): Promise<Client> {
  const ws = new WebSocket(`ws://127.0.0.1:${PORT}/?token=${TOKEN}`)
  await new Promise<void>((resolve, reject) => { ws.on('open', resolve); ws.on('error', reject) })
  return new Client(ws)
}

describe('sidecar wire protocol (live bundle)', () => {
  it('rejects an unauthorized connection', async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${PORT}/?token=wrong`)
    const code = await new Promise<number>((resolve) => { ws.on('close', resolve); ws.on('error', () => {}) })
    expect(code).toBe(4001)
  })

  it('handshakes, writes, reads back, and stats over the wire', async () => {
    const c = await connect()
    const hello = await c.req('sys.hello', {})
    expect(hello.t === 'res' && hello.ok && (hello.result as { protocolVersion: number }).protocolVersion).toBe(1)
    const w = await c.req('fs.writeText', { path: join(workdir, 'w.txt'), content: 'over-the-wire\n' })
    expect(w.t === 'res' && w.ok && (w.result as { operation: string }).operation).toBe('create')
    const r = await c.req('fs.readText', { path: join(workdir, 'w.txt') })
    expect(r.t === 'res' && r.ok && (r.result as { content: string }).content).toBe('over-the-wire\n')
  })

  it('returns raw bytes as a correlated binary frame', async () => {
    const c = await connect()
    await c.req('fs.writeText', { path: join(workdir, 'b.txt'), content: 'binary-read' })
    const bytes = await c.reqBytes('fs.readBytes', { path: join(workdir, 'b.txt'), maxBytes: 1000 })
    expect(Buffer.from(bytes).toString()).toBe('binary-read')
  })

  it('spawns a process and streams byte-accurate split-multibyte output', async () => {
    const c = await connect()
    // Print a multibyte string in two halves so a naive per-chunk decode would corrupt it.
    const res = await c.req('proc.spawn', {
      ch: 1001,
      argv: ['/bin/sh', '-c', "printf '\\344\\275\\240'; printf '\\345\\245\\275'"],
      cwd: workdir, stdout: 'collect', stderr: 'ignore', stdin: 'ignore',
      env: { PATH: '/bin:/usr/bin' }, graceMs: 1000,
    })
    const ch = (res.t === 'res' && res.ok && (res.result as { ch: number }).ch) as number
    await expect.poll(() => c.exits.has(ch), { timeout: 5000 }).toBe(true)
    const assembled = Buffer.concat(c.chunks.get(ch) ?? []).toString('utf-8')
    expect(assembled).toBe('你好')
    expect(c.exits.get(ch)).toEqual({ exitCode: 0, signal: null })
  })

  it('delivers a nonzero exit code', async () => {
    const c = await connect()
    const res = await c.req('proc.spawn', {
      ch: 1002,
      argv: ['/bin/sh', '-c', 'exit 3'], cwd: workdir,
      stdout: 'ignore', stderr: 'ignore', stdin: 'ignore', env: {}, graceMs: 1000,
    })
    const ch = (res.t === 'res' && res.ok && (res.result as { ch: number }).ch) as number
    await expect.poll(() => c.exits.has(ch), { timeout: 5000 }).toBe(true)
    expect(c.exits.get(ch)?.exitCode).toBe(3)
  })
})

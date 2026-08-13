import { spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { SidecarClient } from 'dsh-sandbox-keeper'
import type { RemoteSandbox } from 'dsh-sandbox-keeper'
import { sidecarBundlePath } from 'dsh-remote-sidecar'
import RemoteSubprocessRuntime from '../src/index.js'
import { CollectedStream } from '../src/collected.js'

const PORT = 49412
const TOKEN = 'subproc-token'
let child: ChildProcess
let client: SidecarClient
let workdir: string
let ctx: Context

beforeAll(async () => {
  workdir = await mkdtemp(join(tmpdir(), 'dsh-spprov-'))
  child = spawn(process.execPath, [sidecarBundlePath()], {
    env: { ...process.env, DSH_SIDECAR_TOKEN: TOKEN, DSH_SIDECAR_PORT: String(PORT), DSH_SIDECAR_CWD: workdir },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('sidecar did not start')), 8000)
    child.stdout?.on('data', (b: Buffer) => { if (b.toString().includes('listening')) { clearTimeout(timer); resolve() } })
    child.on('error', reject)
  })
  client = await SidecarClient.connect(`ws://127.0.0.1:${PORT}`, TOKEN)
  const fake: RemoteSandbox = {
    cwd: workdir, runtimeRoot: join(workdir, '.rt'),
    rpc: async () => client,
    openPty: () => { throw new Error('no pty') },
    syncOut: async () => {}, consumeRecoveryNotice: () => undefined,
  }
  ctx = new Context()
  ctx.provide('remoteSandbox', fake as never)
  await ctx.plugin(RemoteSubprocessRuntime)
})

afterAll(async () => {
  client?.close(); child?.kill('SIGKILL')
  await rm(workdir, { recursive: true, force: true })
})

describe('CollectedStream bounds host memory', () => {
  it('keeps only the tail past maxBytes and marks earlier offsets lossy', () => {
    const c = new CollectedStream(4, undefined)
    c.push(new TextEncoder().encode('abcdefgh'))
    const read = c.readFrom(0)
    expect(read.text).toBe('efgh')
    expect(read.lossy).toBe(true)
    expect(read.nextOffset).toBe(8)
  })

  it('serves an incremental non-lossy read from a live offset', () => {
    const c = new CollectedStream(1000, undefined)
    c.push(new TextEncoder().encode('hello '))
    const first = c.readFrom(0)
    c.push(new TextEncoder().encode('world'))
    const next = c.readFrom(first.nextOffset)
    expect(next.text).toBe('world')
    expect(next.lossy).toBe(false)
  })
})

describe('RemoteSubprocessRuntime provider over a live sidecar', () => {
  it('resolves an executable through ctx.subprocess', async () => {
    const sh = await ctx.subprocess.resolveExecutable('sh')
    expect(sh).toMatch(/\/sh$/)
  })

  it('spawns, collects bounded output, and reports the exit code', async () => {
    const handle = ctx.subprocess.spawn({
      argv: ['/bin/sh', '-c', 'printf hello; exit 0'],
      cwd: workdir,
      stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: { maxBytes: 1024 } },
      graceMs: 1000,
      env: {},
    })
    const outcome = await handle.done
    expect(outcome).toEqual({ exitCode: 0, signal: null })
    expect(handle.collected.stdout?.readFrom(0).text).toBe('hello')
  })

  it('streams split-multibyte output byte-accurately', async () => {
    const handle = ctx.subprocess.spawn({
      argv: ['/bin/sh', '-c', "printf '\\344\\275\\240'; printf '\\345\\245\\275'"],
      cwd: workdir,
      stdio: { stdin: 'ignore', stdout: { maxBytes: 1024 }, stderr: 'ignore' },
      graceMs: 1000,
      env: { PATH: '/bin:/usr/bin' },
    })
    await handle.done
    expect(handle.collected.stdout?.readFrom(0).text).toBe('你好')
  })

  it('writes stdin and reads it back', async () => {
    const handle = ctx.subprocess.spawn({
      argv: ['/bin/cat'],
      cwd: workdir,
      stdio: { stdin: { data: 'piped-input' }, stdout: { maxBytes: 1024 }, stderr: 'ignore' },
      graceMs: 1000,
      env: {},
    })
    await handle.done
    expect(handle.collected.stdout?.readFrom(0).text).toBe('piped-input')
  })

  it('terminates a long-running process tree', async () => {
    const handle = ctx.subprocess.spawn({
      argv: ['/bin/sh', '-c', 'sleep 30'],
      cwd: workdir,
      stdio: { stdin: 'ignore', stdout: 'ignore', stderr: 'ignore' },
      graceMs: 500,
      env: {},
    })
    await new Promise(r => setTimeout(r, 300)) // let it start
    handle.terminate()
    const outcome = await handle.done
    expect(outcome.exitCode === null || outcome.exitCode !== 0).toBe(true)
  })
})

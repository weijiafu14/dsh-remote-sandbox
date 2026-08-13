import { spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { FsError } from '@deepseek-ai/dsh-fs'
import { SidecarClient } from 'dsh-sandbox-keeper'
import type { RemoteSandbox } from 'dsh-sandbox-keeper'
import { sidecarBundlePath } from 'dsh-remote-sidecar'
import RemoteFileSystem from '../src/index.js'

const PORT = 49411
const TOKEN = 'provider-token'
let child: ChildProcess
let client: SidecarClient
let workdir: string
let ctx: Context

beforeAll(async () => {
  workdir = await mkdtemp(join(tmpdir(), 'dsh-fsprov-'))
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
    cwd: workdir,
    runtimeRoot: join(workdir, '.rt'),
    rpc: async () => client,
    openPty: () => { throw new Error('no pty in this test') },
    syncOut: async () => {},
    consumeRecoveryNotice: () => undefined,
  }
  ctx = new Context()
  ctx.provide('remoteSandbox', fake as never)
  await ctx.plugin(RemoteFileSystem)
})

afterAll(async () => {
  client?.close()
  child?.kill('SIGKILL')
  await rm(workdir, { recursive: true, force: true })
})

describe('RemoteFileSystem provider over a live sidecar', () => {
  it('resolves, writes, reads, and stats through ctx.fs', async () => {
    const target = await ctx.fs.resolve('note.txt')
    const outcome = await ctx.fs.writeText(target, 'first\n', { kind: 'createIfAbsent' })
    expect(outcome.operation).toBe('create')
    expect(outcome.before).toBeNull()
    expect(await ctx.fs.readText(target)).toBe('first\n')
    const info = await ctx.fs.stat(target)
    expect(info?.type).toBe('file')
    expect(info?.version).toBe(outcome.version)
  })

  it('maps a stale version guard to a typed FsError', async () => {
    const target = await ctx.fs.resolve('guard.txt')
    await ctx.fs.writeText(target, 'x')
    await expect(ctx.fs.writeText(target, 'y', { kind: 'replaceIfVersion', version: 'stale' as never }))
      .rejects.toSatisfy((e: unknown) => e instanceof FsError && e.code === 'FS_STALE_VERSION')
  })

  it('edits literally and computes before/after', async () => {
    const target = await ctx.fs.resolve('edit.txt')
    await ctx.fs.writeText(target, 'hello world')
    const info = await ctx.fs.stat(target)
    const outcome = await ctx.fs.editText(target, { oldString: 'world', newString: 'there', replaceAll: false }, { version: info!.version })
    expect(outcome.before).toBe('hello world')
    expect(outcome.after).toBe('hello there')
  })

  it('lists a directory with resolved child targets', async () => {
    const dir = await ctx.fs.resolve('.')
    await ctx.fs.writeText(await ctx.fs.resolve('a.txt'), 'a')
    const entries = await ctx.fs.listDir(dir)
    expect(entries.some(e => e.name === 'a.txt' && e.type === 'file')).toBe(true)
  })

  it('reads bounded bytes and rejects over the cap', async () => {
    const target = await ctx.fs.resolve('bytes.bin')
    await ctx.fs.writeText(target, 'abcdefghij')
    const bytes = await ctx.fs.readBytes(target, undefined, 100)
    expect(Buffer.from(bytes).toString()).toBe('abcdefghij')
    await expect(ctx.fs.readBytes(target, undefined, 5)).rejects.toMatchObject({ code: 'FS_TOO_LARGE' })
  })

  it('reports a missing file as undefined, not an error', async () => {
    const target = await ctx.fs.resolve('nope.txt')
    expect(await ctx.fs.stat(target)).toBeUndefined()
  })
})

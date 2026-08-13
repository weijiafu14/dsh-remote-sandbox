/**
 * Default workspace IN/OUT: a full tar streamed through the sidecar's process
 * primitive. IN happens once at sandbox open; OUT is low-frequency (task end or
 * explicit request), because crash recovery relies first on the paused
 * sandbox's retained disk and only falls back to the last OUT when the disk is
 * truly gone — so there is no per-write sync tax. A deployment overrides either
 * direction with its own function via config.
 * @module dsh-sandbox-keeper/sync
 */

import { PassThrough } from 'node:stream'
import { create as tarCreate, extract as tarExtract } from 'tar'
import type { SidecarClient } from './client.js'
import type { SandboxHandle } from './backend.js'

/** Context handed to a sync direction: the live client, sandbox, paths, and excludes. */
export interface SyncContext {
  client: SidecarClient
  sandbox: SandboxHandle
  sandboxCwd: string
  hostDir: string
  excludes: readonly string[]
  signal?: AbortSignal
}

/** One overridable sync direction. */
export type SyncFn = (ctx: SyncContext) => Promise<void>

/** The default paths never carried across the boundary; `.git` is intentionally NOT excluded. */
export const DEFAULT_EXCLUDES: readonly string[] = ['node_modules', '.pnpm-store', '.dsh-remote-sidecar']

/**
 * Push the host workspace into the sandbox: tar the host tree (honoring
 * excludes) and stream it into `tar -x` running in the sandbox.
 * @param ctx - the sync context.
 */
export const defaultSyncIn: SyncFn = async (ctx) => {
  const excludeSet = new Set(ctx.excludes)
  const tarStream = tarCreate(
    { gzip: true, cwd: ctx.hostDir, filter: p => !isExcluded(p, excludeSet) },
    ['.'],
  )
  const proc = await ctx.client.spawn({
    argv: ['tar', '-xzf', '-', '-C', ctx.sandboxCwd],
    cwd: ctx.sandboxCwd,
    stdout: 'ignore', stderr: 'collect', stdin: 'pipe',
    env: { PATH: '/usr/bin:/bin' }, graceMs: 5000,
  })
  for await (const chunk of tarStream) {
    if (ctx.signal?.aborted) { proc.terminate(); throw new Error('syncIn aborted') }
    proc.writeStdin(chunk as Uint8Array)
  }
  proc.endStdin()
  const exit = await proc.exit
  if (exit.exitCode !== 0) throw new Error(`syncIn: tar extract exited ${exit.exitCode ?? exit.signal}`)
}

/**
 * Pull the sandbox workspace back to the host: run `tar -c` in the sandbox and
 * extract its byte-accurate stdout into the host directory.
 * @param ctx - the sync context.
 */
export const defaultSyncOut: SyncFn = async (ctx) => {
  const excludeArgs = ctx.excludes.flatMap(e => ['--exclude', `./${e}`])
  const proc = await ctx.client.spawn({
    argv: ['tar', '-czf', '-', '-C', ctx.sandboxCwd, ...excludeArgs, '.'],
    cwd: ctx.sandboxCwd,
    stdout: 'pipe', stderr: 'collect', stdin: 'ignore',
    env: { PATH: '/usr/bin:/bin' }, graceMs: 5000,
  })
  const sink = new PassThrough()
  const extractDone = new Promise<void>((resolve, reject) => {
    const extractor = tarExtract({ cwd: ctx.hostDir })
    sink.pipe(extractor)
    extractor.on('finish', resolve)
    extractor.on('error', reject)
  })
  proc.onStdout(bytes => sink.write(Buffer.from(bytes)))
  const exit = await proc.exit
  sink.end()
  if (exit.exitCode !== 0) throw new Error(`syncOut: tar create exited ${exit.exitCode ?? exit.signal}`)
  await extractDone
}

function isExcluded(entryPath: string, excludes: ReadonlySet<string>): boolean {
  const parts = entryPath.replace(/^\.\//, '').split('/')
  return parts.some(part => excludes.has(part))
}

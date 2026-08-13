/**
 * `dsh-sandbox-keeper` plugin entry. Composes the E2B backend and the keeper,
 * provides `ctx.remoteSandbox` for the fs and subprocess providers, and tears
 * the sandbox down on disposal (pausing by default so its disk survives). Load
 * this before `dsh-fs-remote` and `dsh-subprocess-remote`.
 * @module dsh-sandbox-keeper
 */

import { Context } from '@deepseek-ai/cordis'
import { E2BBackend } from './backend.js'
import { Keeper, DEFAULT_EXCLUDES, defaultSyncIn, defaultSyncOut } from './keeper.js'
import type { KeeperOptions, RemoteSandbox } from './keeper.js'
import type { SyncFn } from './sync.js'
import { DEFAULT_SIDECAR_PORT } from 'dsh-remote-protocol'

declare module '@deepseek-ai/cordis' {
  interface Context {
    remoteSandbox: RemoteSandbox
  }
}

/** Cordis plugin name. */
export const name = 'sandbox-keeper'

/** User-facing configuration for the keeper. `apiKey` falls back to `E2B_API_KEY`. */
export interface Config {
  /** E2B API key; omission reads `E2B_API_KEY`. Never forwarded into the sandbox. */
  apiKey?: string
  /** E2B template id; omitted uses the E2B base image (which ships Node). */
  template?: string
  /** Absolute workspace path INSIDE the sandbox. */
  cwd?: string
  /** Absolute host path whose contents sync in at open and out on request/teardown. */
  hostWorkspace?: string
  /** Sidecar runtime dir inside the sandbox, outside the synced workspace. */
  runtimeRoot?: string
  /** TCP port the sidecar listens on inside the sandbox. */
  sidecarPort?: number
  /** Pause deadline in ms; the heartbeat pushes it forward so the sandbox stays alive. */
  timeoutMs?: number
  /** Heartbeat interval in ms for keep-alive and liveness probing. */
  heartbeatMs?: number
  /** Secure the sandbox controller with an auth token (wss). */
  secure?: boolean
  /** Whether disposal pauses (retain disk) rather than kills the sandbox. */
  pauseOnDispose?: boolean
  /** Whether an OUT sync runs on disposal. */
  syncOutOnDispose?: boolean
  /** Path names never carried across the boundary (each matched as a path segment). */
  excludes?: readonly string[]
  /** Override the workspace IN sync with a deployment-specific function. */
  syncIn?: SyncFn
  /** Override the workspace OUT sync with a deployment-specific function. */
  syncOut?: SyncFn
  /** Wire the recovery notice to the model (e.g. `agent.inject`); also on `consumeRecoveryNotice()`. */
  notify?: (text: string) => void
}

/**
 * Instantiate the keeper, run the initial sandbox bring-up, and expose it.
 * @param ctx - the plugin context.
 * @param config - validated keeper configuration.
 */
export async function apply(ctx: Context, config: Config = {}): Promise<void> {
  const apiKey = config.apiKey ?? process.env['E2B_API_KEY']
  if (apiKey === undefined || apiKey.length === 0) {
    throw new Error('dsh-sandbox-keeper: configure apiKey or set E2B_API_KEY')
  }
  const cwd = config.cwd ?? '/home/user/workspace'
  const options: KeeperOptions = {
    cwd,
    runtimeRoot: config.runtimeRoot ?? '/home/user/.dsh-remote',
    sidecarPort: config.sidecarPort ?? DEFAULT_SIDECAR_PORT,
    timeoutMs: config.timeoutMs ?? 300_000,
    heartbeatMs: config.heartbeatMs ?? 30_000,
    excludes: config.excludes ?? DEFAULT_EXCLUDES,
    snapshotDir: config.hostWorkspace ?? process.cwd(),
    syncIn: config.syncIn ?? defaultSyncIn,
    syncOut: config.syncOut ?? defaultSyncOut,
    ...(config.notify !== undefined ? { notify: config.notify } : {}),
  }
  const backend = new E2BBackend(apiKey, config.template, config.secure ?? true)
  const keeper = new Keeper(backend, options)
  await keeper.init()

  ctx.provide('remoteSandbox', keeper)

  ctx.effect(() => async () => {
    if (config.syncOutOnDispose ?? true) await keeper.syncOut().catch(() => {})
    await keeper.shutdown(config.pauseOnDispose ?? true)
  })
}

export { Keeper, DEFAULT_EXCLUDES, defaultSyncIn, defaultSyncOut }
export type { RemoteSandbox, KeeperOptions } from './keeper.js'
export type { SyncFn, SyncContext } from './sync.js'
export { SidecarClient, RemoteError } from './client.js'
export type { ProcChannel, ProcExit } from './client.js'
export type { SandboxBackend, SandboxHandle, PtyPrimitive } from './backend.js'

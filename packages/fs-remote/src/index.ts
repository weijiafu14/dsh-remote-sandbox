/**
 * `dsh-fs-remote` — a `ctx.fs` provider backed by the remote-sandbox sidecar.
 * Every method is one round trip to the sidecar, which executes the primitive
 * locally in the sandbox; a mutation is atomic in that single call rather than a
 * read-modify-write-metadata sequence of separate exchanges. The current client
 * is derived per operation through `ctx.remoteSandbox.rpc()`, so a transparent
 * sandbox recovery swaps the connection under the provider without a fork.
 * @module dsh-fs-remote
 */

import { posix } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import { FileSystem, FsError, FsTargetKey, FsVersion } from '@deepseek-ai/dsh-fs'
import type {
  FsDirEntry,
  FsEditOutcome,
  FsEditRequest,
  FsInfo,
  FsPathInfo,
  FsTarget,
  FsWriteIntent,
  FsWriteOutcome,
} from '@deepseek-ai/dsh-fs'
import type { SandboxExecutionPolicy, SandboxMode } from '@deepseek-ai/dsh-sandbox'
import { RemoteError } from 'dsh-sandbox-keeper'
import type { SidecarClient } from 'dsh-sandbox-keeper'
import type { FsErrorCode } from '@deepseek-ai/dsh-fs'

const FS_ERROR_CODES = new Set<FsErrorCode>([
  'FS_NOT_FOUND', 'FS_NOT_DIRECTORY', 'FS_NOT_TEXT', 'FS_NOT_REGULAR_FILE', 'FS_TOO_LARGE',
  'FS_PERMISSION_DENIED', 'FS_SANDBOX_DENIED', 'FS_IO_ERROR', 'FS_STALE_VERSION',
  'FS_NOT_OBSERVED', 'FS_AMBIGUOUS_EDIT', 'FS_EDIT_NOT_FOUND', 'FS_ABORTED',
])

/** Map a sidecar failure to the matching typed `FsError`; the sidecar already speaks FsErrorCode. */
function mapError(err: unknown): never {
  if (err instanceof RemoteError) {
    const code: FsErrorCode = FS_ERROR_CODES.has(err.code as FsErrorCode) ? (err.code as FsErrorCode) : 'FS_IO_ERROR'
    throw new FsError(err.message, code)
  }
  throw err
}

async function guard<T>(op: () => Promise<T>): Promise<T> {
  try {
    return await op()
  } catch (err) {
    return mapError(err)
  }
}

/** Filesystem provider over the sidecar; targets are canonical sandbox paths. */
export default class RemoteFileSystem extends FileSystem {
  /** The keeper must provide `ctx.remoteSandbox` before this provider loads. */
  static inject = ['remoteSandbox']

  private readonly defaultMode: SandboxMode | undefined

  constructor(ctx: Context, config: { sandboxMode?: SandboxMode } = {}) {
    super(ctx)
    this.defaultMode = config.sandboxMode
  }

  override get sandboxMode(): SandboxMode | undefined {
    return this.defaultMode
  }

  private rpc(signal?: AbortSignal): Promise<SidecarClient> {
    return (this.ctx as Context).remoteSandbox.rpc(signal)
  }

  private get baseCwd(): string {
    return (this.ctx as Context).remoteSandbox.cwd
  }

  async resolve(path: string, opts?: { cwd?: string; signal?: AbortSignal }): Promise<FsTarget> {
    const client = await this.rpc(opts?.signal)
    const { canonical } = await guard(() => client.realpath(path, opts?.cwd ?? this.baseCwd))
    return { targetKey: FsTargetKey(canonical), displayPath: canonical }
  }

  processPath(target: FsTarget): string {
    return String(target.targetKey)
  }

  fileUrl(target: FsTarget): string {
    const abs = String(target.targetKey)
    return `file://${abs.split('/').map(encodeURIComponent).join('/')}`
  }

  contains(parent: FsTarget, child: FsTarget): boolean {
    const p = String(parent.targetKey)
    const c = String(child.targetKey)
    return c === p || c.startsWith(p.endsWith('/') ? p : p + '/')
  }

  async stat(target: FsTarget, signal?: AbortSignal): Promise<FsInfo | undefined> {
    const client = await this.rpc(signal)
    const result = await guard(() => client.stat(this.processPath(target)))
    if (result === null) return undefined
    return { version: FsVersion(result.version), type: result.type, size: result.size }
  }

  async lstat(path: string, opts?: { cwd?: string }, signal?: AbortSignal): Promise<FsPathInfo | undefined> {
    const client = await this.rpc(signal)
    const result = await guard(() => client.lstat(path, opts?.cwd ?? this.baseCwd))
    if (result === null) return undefined
    return { version: FsVersion(result.version), type: result.type, size: result.size }
  }

  async readText(target: FsTarget, signal?: AbortSignal): Promise<string> {
    const client = await this.rpc(signal)
    return guard(() => client.readText(this.processPath(target)))
  }

  async streamText(target: FsTarget, signal?: AbortSignal): Promise<AsyncIterable<string>> {
    const client = await this.rpc(signal)
    const byteStream = client.streamText(this.processPath(target), signal)
    return {
      async *[Symbol.asyncIterator]() {
        const decoder = new TextDecoder('utf-8', { fatal: true })
        try {
          for await (const bytes of byteStream) yield decoder.decode(bytes, { stream: true })
          const tail = decoder.decode()
          if (tail.length > 0) yield tail
        } catch (err) {
          mapError(err)
        }
      },
    }
  }

  async readBytes(target: FsTarget, signal: AbortSignal | undefined, maxBytes: number): Promise<Uint8Array> {
    const client = await this.rpc(signal)
    return guard(() => client.readBytes(this.processPath(target), maxBytes, signal))
  }

  async listDir(target: FsTarget, signal?: AbortSignal): Promise<FsDirEntry[]> {
    const client = await this.rpc(signal)
    const { entries } = await guard(() => client.listDir(this.processPath(target)))
    const base = this.processPath(target)
    return entries.map(e => ({
      name: e.name,
      type: e.type,
      target: { targetKey: FsTargetKey(posix.join(base, e.name)), displayPath: posix.join(base, e.name) },
      version: FsVersion(e.version),
      size: e.size,
    }))
  }

  async writeText(
    target: FsTarget,
    content: string,
    expected?: FsWriteIntent,
    signal?: AbortSignal,
    sandboxPolicy?: SandboxExecutionPolicy,
  ): Promise<FsWriteOutcome> {
    const client = await this.rpc(signal)
    const result = await guard(() => client.writeText({
      path: this.processPath(target),
      content,
      intent: expected,
      mode: sandboxPolicy?.mode ?? this.defaultMode,
      workspaceRoot: sandboxPolicy?.workspaceRoot,
    }))
    return { operation: result.operation, version: FsVersion(result.version), before: result.before, after: result.after }
  }

  async editText(
    target: FsTarget,
    edit: FsEditRequest,
    expected?: { version: FsVersion },
    signal?: AbortSignal,
    sandboxPolicy?: SandboxExecutionPolicy,
  ): Promise<FsEditOutcome> {
    const client = await this.rpc(signal)
    const result = await guard(() => client.editText({
      path: this.processPath(target),
      oldString: edit.oldString,
      newString: edit.newString,
      replaceAll: edit.replaceAll,
      expectedVersion: expected?.version === undefined ? undefined : String(expected.version),
      mode: sandboxPolicy?.mode ?? this.defaultMode,
      workspaceRoot: sandboxPolicy?.workspaceRoot,
    }))
    return { version: FsVersion(result.version), before: result.before, after: result.after }
  }
}

export { RemoteFileSystem }

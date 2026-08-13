/**
 * Sidecar primitives with no socket or process dependency: UTF-8 text
 * validation, the stat-derived version token, the sandbox-mode write fence, and
 * atomic file publication. Kept dependency-free so they unit-test on the host
 * against a temp directory without a live sandbox.
 * @module dsh-remote-sidecar/util
 */

import { constants } from 'node:fs'
import { open, rename, stat, unlink } from 'node:fs/promises'
import { dirname, join, resolve as pathResolve, sep } from 'node:path'

/** A sidecar operation failure carrying a stable code the host maps to a typed error. */
export class OpError extends Error {
  constructor(readonly code: string, message: string) {
    super(message)
    this.name = 'OpError'
  }
}

const utf8Decoder = new TextDecoder('utf-8', { fatal: true })

/**
 * Decode bytes as UTF-8 text, rejecting invalid sequences and NUL bytes the way
 * a text file never contains them. NUL is valid UTF-8 (U+0000) but marks binary
 * content, so it is rejected separately from decoder failure.
 * @param bytes - the raw file bytes.
 * @returns the decoded string.
 */
export function decodeTextStrict(bytes: Uint8Array): string {
  if (bytes.includes(0)) throw new OpError('FS_NOT_TEXT', 'file contains NUL bytes and is not text')
  try {
    return utf8Decoder.decode(bytes)
  } catch {
    throw new OpError('FS_NOT_TEXT', 'file is not valid UTF-8 text')
  }
}

/**
 * Build the opaque version token from high-resolution stat identity and
 * freshness, mirroring the local dsh backend: it changes on every content write
 * (mtime + size) and distinguishes distinct inodes.
 * @param s - a BigInt stat result.
 * @returns the version token string.
 */
export function versionToken(s: { mtimeNs: bigint; size: bigint; ino: bigint; dev: bigint }): string {
  return `${s.mtimeNs}:${s.size}:${s.ino}:${s.dev}`
}

/** Normalize CR/CRLF to LF for the diff-basis text the write/edit outcomes carry. */
export function normalizeLf(text: string): string {
  return text.replace(/\r\n?/g, '\n')
}

/**
 * Enforce the per-call sandbox mode. `read-only` denies every mutation;
 * `workspace-write` requires the target to sit under the workspace root or the
 * system temp dir; `danger-full-access` and an absent mode allow the write.
 * @param mode - the sandbox mode for this call, or undefined for unfenced.
 * @param targetPath - absolute path being mutated.
 * @param workspaceRoot - the writable workspace root for `workspace-write`.
 */
export function enforceWriteFence(mode: string | undefined, targetPath: string, workspaceRoot: string | undefined): void {
  if (mode === undefined || mode === 'danger-full-access') return
  if (mode === 'read-only') throw new OpError('FS_SANDBOX_DENIED', 'write denied: sandbox is read-only')
  if (mode === 'workspace-write') {
    const abs = pathResolve(targetPath)
    const roots = [workspaceRoot, '/tmp'].filter((r): r is string => typeof r === 'string' && r.length > 0).map(r => pathResolve(r))
    const allowed = roots.some(root => abs === root || abs.startsWith(root + sep))
    if (!allowed) throw new OpError('FS_SANDBOX_DENIED', `write denied: ${abs} is outside the writable roots`)
    return
  }
  throw new OpError('FS_SANDBOX_DENIED', `unknown sandbox mode ${mode}`)
}

/**
 * Atomically publish file content: write a sibling temp file, fsync it, then
 * rename over the target so a reader never observes a partial write.
 * @param targetPath - absolute destination path.
 * @param content - the full new content.
 */
export async function atomicWrite(targetPath: string, content: string): Promise<void> {
  const dir = dirname(targetPath)
  const tmp = join(dir, `.dsh-remote-${process.pid}-${randomSuffix()}.tmp`)
  const handle = await open(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL, 0o600)
  try {
    await handle.writeFile(content, 'utf-8')
    await handle.sync()
  } finally {
    await handle.close()
  }
  try {
    await rename(tmp, targetPath)
  } catch (err) {
    await unlink(tmp).catch(() => {})
    throw err
  }
}

let suffixCounter = 0
function randomSuffix(): string {
  suffixCounter = (suffixCounter + 1) >>> 0
  return `${Date.now().toString(36)}-${suffixCounter.toString(36)}`
}

/**
 * Map a Node fs errno error to a stable OpError code, defaulting to FS_IO_ERROR.
 * @param err - the caught error.
 * @param fallbackMessage - message when the error is not a recognized errno.
 * @returns never; always throws.
 */
export function throwFsErrno(err: unknown, fallbackMessage: string): never {
  const code = (err as { code?: string }).code
  if (code === 'ENOENT') throw new OpError('FS_NOT_FOUND', 'path does not exist')
  if (code === 'ENOTDIR') throw new OpError('FS_NOT_DIRECTORY', 'a path component is not a directory')
  if (code === 'EISDIR') throw new OpError('FS_NOT_REGULAR_FILE', 'path is a directory, not a regular file')
  if (code === 'EACCES' || code === 'EPERM') throw new OpError('FS_PERMISSION_DENIED', 'permission denied')
  throw new OpError('FS_IO_ERROR', `${fallbackMessage}: ${(err as Error).message}`)
}

/**
 * Stat a path with BigInt precision, returning undefined when it does not exist.
 * @param path - absolute path to stat.
 * @param followSymlink - true to follow the final symlink (stat), false for lstat.
 * @returns the BigInt stats or undefined when absent.
 */
export async function statBig(path: string, followSymlink: boolean): Promise<import('node:fs').BigIntStats | undefined> {
  try {
    const { lstat, stat: statFn } = await import('node:fs/promises')
    return followSymlink ? await statFn(path, { bigint: true }) : await lstat(path, { bigint: true })
  } catch (err) {
    if ((err as { code?: string }).code === 'ENOENT') return undefined
    throwFsErrno(err, 'stat failed')
  }
}

export { stat }

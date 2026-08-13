/**
 * Filesystem primitive handlers executed inside the sandbox. Each is one host
 * round trip: a directory listing stats every child locally and returns them
 * together, a write reads-modifies-publishes atomically in one call, so no
 * operation fans out into multiple host exchanges the way per-command SDK
 * transports do.
 * @module dsh-remote-sidecar/fs-ops
 */

import { readFile, readdir, realpath } from 'node:fs/promises'
import { basename, dirname, isAbsolute, join, resolve as pathResolve } from 'node:path'
import { constants } from 'node:fs'
import { access } from 'node:fs/promises'
import type {
  DirEntry,
  EditResult,
  LstatResult,
  StatResult,
  WriteResult,
} from 'dsh-remote-protocol'
import {
  OpError,
  atomicWrite,
  decodeTextStrict,
  enforceWriteFence,
  normalizeLf,
  statBig,
  throwFsErrno,
  versionToken,
} from './util.js'

function fsType(s: import('node:fs').BigIntStats): 'file' | 'directory' | 'other' {
  if (s.isFile()) return 'file'
  if (s.isDirectory()) return 'directory'
  return 'other'
}

/**
 * Canonicalize a path that need not fully exist (the local `realpath -m`
 * behavior dsh resolve relies on for a to-be-created file): realpath the
 * longest existing prefix, then re-append the missing tail lexically.
 * @param cwd - base directory for a relative input path.
 * @param input - absolute or relative path to canonicalize.
 * @returns the canonical absolute path.
 */
export async function realpathMissable(cwd: string, input: string): Promise<string> {
  const abs = isAbsolute(input) ? input : join(cwd, input)
  const parts: string[] = []
  let current = pathResolve(abs)
  for (;;) {
    try {
      const real = await realpath(current)
      return parts.length === 0 ? real : join(real, ...parts.reverse())
    } catch (err) {
      if ((err as { code?: string }).code !== 'ENOENT') throwFsErrno(err, 'realpath failed')
      const parent = dirname(current)
      if (parent === current) return parts.length === 0 ? current : join(current, ...parts.reverse())
      parts.push(basename(current))
      current = parent
    }
  }
}

export async function opRealpath(cwd: string, path: string): Promise<{ canonical: string }> {
  return { canonical: await realpathMissable(cwd, path) }
}

export async function opStat(path: string): Promise<StatResult | null> {
  const s = await statBig(path, true)
  if (s === undefined) return null
  return { type: fsType(s), size: Number(s.size), version: versionToken(s) }
}

export async function opLstat(cwd: string, path: string): Promise<LstatResult | null> {
  const abs = isAbsolute(path) ? path : join(cwd, path)
  const s = await statBig(abs, false)
  if (s === undefined) return null
  const type = s.isSymbolicLink() ? 'symlink' : fsType(s)
  return { type, size: Number(s.size), version: versionToken(s) }
}

export async function opReadText(path: string): Promise<{ content: string }> {
  let bytes: Buffer
  try {
    bytes = await readFile(path)
  } catch (err) {
    throwFsErrno(err, 'read failed')
  }
  return { content: decodeTextStrict(bytes) }
}

export async function opReadBytes(path: string, maxBytes: number): Promise<Uint8Array> {
  const s = await statBig(path, true)
  if (s === undefined) throw new OpError('FS_NOT_FOUND', 'path does not exist')
  if (!s.isFile()) throw new OpError('FS_NOT_REGULAR_FILE', 'path is not a regular file')
  if (Number(s.size) > maxBytes) throw new OpError('FS_TOO_LARGE', `file is ${s.size} bytes, over the ${maxBytes} cap`)
  try {
    return await readFile(path)
  } catch (err) {
    throwFsErrno(err, 'read failed')
  }
}

export async function opListDir(path: string): Promise<{ entries: DirEntry[] }> {
  let names: import('node:fs').Dirent[]
  try {
    names = await readdir(path, { withFileTypes: true })
  } catch (err) {
    throwFsErrno(err, 'listdir failed')
  }
  const entries: DirEntry[] = []
  for (const dirent of names) {
    const child = join(path, dirent.name)
    const s = await statBig(child, true)
    if (s === undefined) continue
    entries.push({ name: dirent.name, type: fsType(s), version: versionToken(s), size: Number(s.size) })
  }
  entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
  return { entries }
}

async function readPriorText(path: string): Promise<string | null> {
  try {
    const bytes = await readFile(path)
    return normalizeLf(decodeTextStrict(bytes))
  } catch {
    return null
  }
}

export interface WriteParamsInternal {
  path: string
  content: string
  intent?: { kind: 'createIfAbsent' } | { kind: 'replaceIfVersion'; version: string }
  mode?: string
  workspaceRoot?: string
}

export async function opWriteText(p: WriteParamsInternal): Promise<WriteResult> {
  enforceWriteFence(p.mode, p.path, p.workspaceRoot)
  const prior = await statBig(p.path, true)
  const existed = prior !== undefined
  if (p.intent?.kind === 'createIfAbsent' && existed) {
    throw new OpError('FS_NOT_OBSERVED', 'createIfAbsent: target already exists')
  }
  if (p.intent?.kind === 'replaceIfVersion') {
    if (!existed) throw new OpError('FS_STALE_VERSION', 'replaceIfVersion: target is absent')
    if (versionToken(prior) !== p.intent.version) throw new OpError('FS_STALE_VERSION', 'replaceIfVersion: version mismatch')
  }
  const before = existed ? await readPriorText(p.path) : null
  await atomicWrite(p.path, p.content)
  const after = await statBig(p.path, true)
  if (after === undefined) throw new OpError('FS_IO_ERROR', 'file vanished immediately after write')
  return {
    operation: existed ? 'update' : 'create',
    version: versionToken(after),
    before,
    after: normalizeLf(p.content),
  }
}

export interface EditParamsInternal {
  path: string
  oldString: string
  newString: string
  replaceAll: boolean
  expectedVersion?: string
  mode?: string
  workspaceRoot?: string
}

export async function opEditText(p: EditParamsInternal): Promise<EditResult> {
  enforceWriteFence(p.mode, p.path, p.workspaceRoot)
  const current = await statBig(p.path, true)
  if (current === undefined) throw new OpError('FS_NOT_FOUND', 'edit target does not exist')
  if (p.expectedVersion !== undefined && versionToken(current) !== p.expectedVersion) {
    throw new OpError('FS_STALE_VERSION', 'edit: version guard mismatch')
  }
  const raw = await readFile(p.path)
  const before = normalizeLf(decodeTextStrict(raw))
  const oldNorm = normalizeLf(p.oldString)
  const newNorm = normalizeLf(p.newString)
  const matches = countOccurrences(before, oldNorm)
  if (matches === 0) throw new OpError('FS_EDIT_NOT_FOUND', 'edit: oldString not found')
  if (matches > 1 && !p.replaceAll) throw new OpError('FS_AMBIGUOUS_EDIT', `edit: oldString matched ${matches} times`)
  const after = p.replaceAll ? before.split(oldNorm).join(newNorm) : before.replace(oldNorm, newNorm)
  await atomicWrite(p.path, after)
  const post = await statBig(p.path, true)
  if (post === undefined) throw new OpError('FS_IO_ERROR', 'file vanished immediately after edit')
  return { version: versionToken(post), before, after }
}

function countOccurrences(haystack: string, needle: string): number {
  if (needle.length === 0) return 0
  let count = 0
  let index = haystack.indexOf(needle)
  while (index !== -1) {
    count += 1
    index = haystack.indexOf(needle, index + needle.length)
  }
  return count
}

/**
 * Resolve one executable in the sandbox: verify an absolute path, reject a
 * relative path containing separators (no defined base), or search the provided
 * PATH for a bare name.
 * @param command - absolute path or bare executable name.
 * @param env - environment supplying PATH for a bare-name search.
 * @returns the canonical executable path.
 */
export async function opResolveExecutable(command: string, env: Record<string, string>): Promise<{ path: string }> {
  if (isAbsolute(command)) {
    await assertExecutable(command)
    return { path: await realpath(command) }
  }
  if (command.includes('/')) throw new OpError('PROC_EXECUTABLE_NOT_FOUND', `relative executable path has no resolution base: ${command}`)
  const pathVar = env['PATH'] ?? process.env['PATH'] ?? ''
  for (const dir of pathVar.split(':')) {
    if (dir.length === 0) continue
    const candidate = join(dir, command)
    try {
      await assertExecutable(candidate)
      return { path: await realpath(candidate) }
    } catch {
      continue
    }
  }
  throw new OpError('PROC_EXECUTABLE_NOT_FOUND', `executable not found on PATH: ${command}`)
}

async function assertExecutable(path: string): Promise<void> {
  await access(path, constants.X_OK)
}

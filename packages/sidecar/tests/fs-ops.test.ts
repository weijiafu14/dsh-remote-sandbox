import { mkdtemp, readFile, rm, mkdir, writeFile, symlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  opEditText,
  opListDir,
  opReadBytes,
  opReadText,
  opRealpath,
  opResolveExecutable,
  opStat,
  opWriteText,
} from '../src/fs-ops.js'
import { OpError, decodeTextStrict, enforceWriteFence, normalizeLf, versionToken } from '../src/util.js'

let dir: string
beforeEach(async () => { dir = await mkdtemp(join(tmpdir(), 'dsh-fsops-')) })
afterEach(async () => { await rm(dir, { recursive: true, force: true }) })

describe('util primitives', () => {
  it('rejects NUL bytes and invalid UTF-8 as non-text', () => {
    expect(() => decodeTextStrict(new Uint8Array([0x61, 0x00, 0x62]))).toThrow(OpError)
    expect(() => decodeTextStrict(new Uint8Array([0xff, 0xfe]))).toThrow(/not valid UTF-8/)
    expect(decodeTextStrict(new TextEncoder().encode('你好'))).toBe('你好')
  })

  it('normalizes CRLF and CR to LF', () => {
    expect(normalizeLf('a\r\nb\rc\n')).toBe('a\nb\nc\n')
  })

  it('version token changes with mtime/size/inode fields', () => {
    const a = versionToken({ mtimeNs: 1n, size: 2n, ino: 3n, dev: 4n })
    expect(a).toBe('1:2:3:4')
    expect(versionToken({ mtimeNs: 9n, size: 2n, ino: 3n, dev: 4n })).not.toBe(a)
  })

  it('write fence: read-only denies, workspace-write fences to root, danger allows', () => {
    expect(() => enforceWriteFence('read-only', '/w/f', '/w')).toThrow(/read-only/)
    expect(() => enforceWriteFence('workspace-write', '/other/f', '/w')).toThrow(/outside/)
    expect(() => enforceWriteFence('workspace-write', '/w/sub/f', '/w')).not.toThrow()
    expect(() => enforceWriteFence('danger-full-access', '/anywhere', undefined)).not.toThrow()
    expect(() => enforceWriteFence(undefined, '/anywhere', undefined)).not.toThrow()
  })
})

describe('fs write/read/edit round trips', () => {
  it('creates, reads, stats, and reports operation + versions', async () => {
    const f = join(dir, 'a.txt')
    const created = await opWriteText({ path: f, content: 'hello\n' })
    expect(created.operation).toBe('create')
    expect(created.before).toBeNull()
    expect(created.after).toBe('hello\n')
    expect(await opReadText(f)).toEqual({ content: 'hello\n' })
    const s = await opStat(f)
    expect(s?.type).toBe('file')
    expect(s?.version).toBe(created.version)
  })

  it('createIfAbsent fails on an existing file with FS_NOT_OBSERVED', async () => {
    const f = join(dir, 'a.txt')
    await opWriteText({ path: f, content: 'x' })
    await expect(opWriteText({ path: f, content: 'y', intent: { kind: 'createIfAbsent' } }))
      .rejects.toMatchObject({ code: 'FS_NOT_OBSERVED' })
  })

  it('replaceIfVersion enforces the freshness guard', async () => {
    const f = join(dir, 'a.txt')
    const w = await opWriteText({ path: f, content: 'one' })
    await expect(opWriteText({ path: f, content: 'two', intent: { kind: 'replaceIfVersion', version: 'stale' } }))
      .rejects.toMatchObject({ code: 'FS_STALE_VERSION' })
    const ok = await opWriteText({ path: f, content: 'two', intent: { kind: 'replaceIfVersion', version: w.version } })
    expect(ok.operation).toBe('update')
    expect(ok.before).toBe('one')
  })

  it('edit replaces literally, guards ambiguity and missing matches', async () => {
    const f = join(dir, 'a.txt')
    await opWriteText({ path: f, content: 'a b a' })
    await expect(opEditText({ path: f, oldString: 'a', newString: 'X', replaceAll: false }))
      .rejects.toMatchObject({ code: 'FS_AMBIGUOUS_EDIT' })
    await expect(opEditText({ path: f, oldString: 'zzz', newString: 'X', replaceAll: false }))
      .rejects.toMatchObject({ code: 'FS_EDIT_NOT_FOUND' })
    const all = await opEditText({ path: f, oldString: 'a', newString: 'X', replaceAll: true })
    expect(all.after).toBe('X b X')
    expect(all.before).toBe('a b a')
  })

  it('edit honors the version guard', async () => {
    const f = join(dir, 'a.txt')
    await opWriteText({ path: f, content: 'hello world' })
    await expect(opEditText({ path: f, oldString: 'world', newString: 'there', replaceAll: false, expectedVersion: 'stale' }))
      .rejects.toMatchObject({ code: 'FS_STALE_VERSION' })
  })

  it('atomic write leaves no temp siblings', async () => {
    const f = join(dir, 'a.txt')
    await opWriteText({ path: f, content: 'data' })
    const { entries } = await opListDir(dir)
    expect(entries.map(e => e.name)).toEqual(['a.txt'])
  })
})

describe('readBytes bound + listDir + resolve', () => {
  it('readBytes fails FS_TOO_LARGE past the cap and returns bytes within it', async () => {
    const f = join(dir, 'big.bin')
    await writeFile(f, Buffer.alloc(100, 7))
    await expect(opReadBytes(f, 50)).rejects.toMatchObject({ code: 'FS_TOO_LARGE' })
    const bytes = await opReadBytes(f, 100)
    expect(bytes.byteLength).toBe(100)
  })

  it('listDir returns sorted children with type and version', async () => {
    await writeFile(join(dir, 'b.txt'), 'b')
    await mkdir(join(dir, 'a-dir'))
    const { entries } = await opListDir(dir)
    expect(entries.map(e => e.name)).toEqual(['a-dir', 'b.txt'])
    expect(entries.find(e => e.name === 'a-dir')?.type).toBe('directory')
    expect(entries.find(e => e.name === 'b.txt')?.version).toMatch(/^\d+:\d+:/)
  })

  it('realpath follows symlinks and tolerates a missing final component', async () => {
    const { realpath } = await import('node:fs/promises')
    const canonicalDir = await realpath(dir) // macOS /var -> /private/var
    const real = join(dir, 'real.txt')
    await writeFile(real, 'r')
    await symlink(real, join(dir, 'link.txt'))
    expect((await opRealpath(dir, 'link.txt')).canonical).toBe(join(canonicalDir, 'real.txt'))
    const missing = await opRealpath(dir, 'does-not-exist.txt')
    expect(missing.canonical).toBe(join(canonicalDir, 'does-not-exist.txt'))
  })

  it('resolveExecutable finds a PATH binary and rejects a separator-relative path', async () => {
    const resolved = await opResolveExecutable('sh', { PATH: '/bin:/usr/bin' })
    expect(resolved.path).toMatch(/\/sh$/)
    await expect(opResolveExecutable('sub/dir/tool', {})).rejects.toMatchObject({ code: 'PROC_EXECUTABLE_NOT_FOUND' })
  })
})

import { spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Keeper, DEFAULT_EXCLUDES, defaultSyncIn, defaultSyncOut } from '../src/index.js'
import type { KeeperOptions } from '../src/index.js'
import type { PtyPrimitive, SandboxBackend, SandboxHandle } from '../src/backend.js'

const PORT = 49402

/** A fake backend that runs the real sidecar bundle as a local process, so the
 * keeper's deploy/connect/sync/recover paths run end-to-end without E2B. */
class FakeBackend implements SandboxBackend {
  readonly name = 'fake'
  readonly gone = new Set<string>()
  private counter = 0
  constructor(private readonly cwd: string) {}

  async create(): Promise<SandboxHandle> {
    return new FakeHandle(`fake-${this.counter++}`, this.cwd, PORT, this.gone)
  }

  async connect(id: string): Promise<SandboxHandle> {
    if (this.gone.has(id)) throw new Error(`sandbox ${id} is gone`)
    return new FakeHandle(id, this.cwd, PORT, this.gone)
  }
}

class FakeHandle implements SandboxHandle {
  private proc: ChildProcess | undefined
  constructor(readonly id: string, private readonly cwd: string, private readonly port: number, private readonly gone: Set<string>) {}

  sidecarUrl(): string { return `ws://127.0.0.1:${this.port}` }
  async setTimeout(): Promise<void> {}
  async pause(): Promise<void> { this.proc?.kill('SIGKILL') }
  async kill(): Promise<void> { this.proc?.kill('SIGKILL'); this.gone.add(this.id) }
  async isRunning(): Promise<boolean> { return this.proc !== undefined && this.proc.exitCode === null }

  async writeFile(path: string, bytes: Uint8Array): Promise<void> {
    await mkdir(dirname(path), { recursive: true })
    await writeFile(path, bytes)
  }

  async startSidecar(command: string, env: Record<string, string>): Promise<void> {
    const bundle = command.split(' ')[1] as string
    const child = spawn(process.execPath, [bundle], { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] })
    this.proc = child
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('fake sidecar did not start')), 8000)
      child.stdout?.on('data', (b: Buffer) => { if (b.toString().includes('listening')) { clearTimeout(timer); resolve() } })
      child.on('error', reject)
      child.on('exit', () => { clearTimeout(timer); reject(new Error('fake sidecar exited early')) })
    })
  }

  openPty(): Promise<PtyPrimitive> { throw new Error('pty unsupported in fake backend') }
}

let sandboxCwd: string
let runtimeRoot: string
let hostWorkspace: string
let outDir: string
let keeper: Keeper

function options(): KeeperOptions {
  return {
    cwd: sandboxCwd,
    runtimeRoot,
    sidecarPort: PORT,
    timeoutMs: 60_000,
    heartbeatMs: 60_000,
    excludes: DEFAULT_EXCLUDES,
    snapshotDir: hostWorkspace,
    syncIn: defaultSyncIn,
    syncOut: defaultSyncOut,
  }
}

beforeEach(async () => {
  const base = await mkdtemp(join(tmpdir(), 'dsh-keeper-'))
  sandboxCwd = join(base, 'sandbox-workspace')
  runtimeRoot = join(base, 'sandbox-runtime')
  hostWorkspace = join(base, 'host-project')
  outDir = join(base, 'host-out')
  await mkdir(hostWorkspace, { recursive: true })
  await mkdir(outDir, { recursive: true })
  await writeFile(join(hostWorkspace, 'seed.txt'), 'seed-content\n')
  await mkdir(join(hostWorkspace, 'node_modules'), { recursive: true })
  await writeFile(join(hostWorkspace, 'node_modules', 'junk.txt'), 'should-not-sync')
})

afterEach(async () => {
  await keeper?.shutdown(false).catch(() => {})
})

describe('keeper lifecycle, sync, and recovery (fake backend, real sidecar)', () => {
  it('brings up a sandbox, syncs the workspace IN, and serves fs over the wire', async () => {
    keeper = new Keeper(new FakeBackend(sandboxCwd), options())
    await keeper.init()
    const client = await keeper.rpc()
    expect(await client.readText(join(sandboxCwd, 'seed.txt'))).toBe('seed-content\n')
  })

  it('excludes node_modules from the IN sync', async () => {
    keeper = new Keeper(new FakeBackend(sandboxCwd), options())
    await keeper.init()
    const client = await keeper.rpc()
    await expect(client.stat(join(sandboxCwd, 'node_modules', 'junk.txt'))).resolves.toBeNull()
  })

  it('recreates the sandbox after a crash, restores the workspace, and notifies the model', async () => {
    const backend = new FakeBackend(sandboxCwd)
    keeper = new Keeper(backend, options())
    await keeper.init()
    const first = await keeper.rpc()
    await first.writeText({ path: join(sandboxCwd, 'seed.txt'), content: 'seed-content\n' })

    // Simulate a hard crash: kill the sidecar and mark the sandbox gone so the
    // keeper's resume attempt fails and it must recreate + re-sync.
    const handle = (keeper as unknown as { handle: SandboxHandle }).handle
    await handle.kill()
    await expect.poll(() => first.closed, { timeout: 5000 }).toBe(true)

    const recovered = await keeper.rpc()
    expect(recovered).not.toBe(first)
    expect(await recovered.readText(join(sandboxCwd, 'seed.txt'))).toBe('seed-content\n')
    const notice = keeper.consumeRecoveryNotice()
    expect(notice).toMatch(/recovered after a connection loss/)
    expect(keeper.consumeRecoveryNotice()).toBeUndefined()
  })

  it('syncs the workspace OUT to a host directory', async () => {
    keeper = new Keeper(new FakeBackend(sandboxCwd), { ...options(), snapshotDir: outDir })
    await keeper.init()
    const client = await keeper.rpc()
    await client.writeText({ path: join(sandboxCwd, 'result.txt'), content: 'produced-in-sandbox\n' })
    await keeper.syncOut()
    expect(await readFile(join(outDir, 'result.txt'), 'utf-8')).toBe('produced-in-sandbox\n')
  })

  it('invokes a custom syncIn/syncOut override', async () => {
    let inCalled = false
    let outCalled = false
    keeper = new Keeper(new FakeBackend(sandboxCwd), {
      ...options(),
      syncIn: async (ctx) => { inCalled = true; await defaultSyncIn(ctx) },
      syncOut: async () => { outCalled = true },
    })
    await keeper.init()
    await keeper.syncOut()
    expect(inCalled).toBe(true)
    expect(outCalled).toBe(true)
  })
})

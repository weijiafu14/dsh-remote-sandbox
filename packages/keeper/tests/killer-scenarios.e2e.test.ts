import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { E2BBackend } from '../src/backend.js'
import { Keeper, DEFAULT_EXCLUDES, defaultSyncIn, defaultSyncOut } from '../src/index.js'
import type { KeeperOptions } from '../src/index.js'

const apiKey = process.env.E2B_API_KEY
const SANDBOX_CWD = '/home/user/workspace'

let hostWorkspace: string
let outDir: string
let keeper: Keeper | undefined

function opts(overrides: Partial<KeeperOptions> = {}): KeeperOptions {
  return {
    cwd: SANDBOX_CWD,
    runtimeRoot: '/home/user/.dsh-remote',
    sidecarPort: 49201,
    timeoutMs: 60_000,
    heartbeatMs: 10_000,
    excludes: DEFAULT_EXCLUDES,
    snapshotDir: hostWorkspace,
    syncIn: defaultSyncIn,
    syncOut: defaultSyncOut,
    ...overrides,
  }
}

beforeEach(async () => {
  const base = await mkdtemp(join(tmpdir(), 'dsh-e2e-'))
  hostWorkspace = join(base, 'project')
  outDir = join(base, 'out')
  await mkdir(hostWorkspace, { recursive: true })
  await mkdir(outDir, { recursive: true })
  await writeFile(join(hostWorkspace, 'README.md'), '# remote sandbox e2e\n')
  await mkdir(join(hostWorkspace, 'node_modules'), { recursive: true })
  await writeFile(join(hostWorkspace, 'node_modules', 'junk.txt'), 'should-not-sync')
})

afterEach(async () => {
  await keeper?.shutdown(false).catch(() => {})
  keeper = undefined
})

describe.skipIf(!apiKey)('remote-sandbox killer scenarios (real E2B)', () => {
  it('scenario 5: tars the workspace IN and OUT with correct diffs and exclusions', async () => {
    keeper = new Keeper(new E2BBackend(apiKey as string, undefined, true), opts({ snapshotDir: hostWorkspace }))
    await keeper.init()
    const client = await keeper.rpc()
    // IN carried the project up but excluded node_modules.
    expect(await client.readText(`${SANDBOX_CWD}/README.md`)).toBe('# remote sandbox e2e\n')
    expect(await client.stat(`${SANDBOX_CWD}/node_modules/junk.txt`)).toBeNull()

    // A file produced in the sandbox comes back on OUT (round-trip to the host mirror).
    await client.writeText({ path: `${SANDBOX_CWD}/generated.txt`, content: 'made-in-sandbox\n' })
    await keeper.syncOut()
    expect(await readFile(join(hostWorkspace, 'generated.txt'), 'utf-8')).toBe('made-in-sandbox\n')
  })

  it('scenario 4: greps a large tree with correct results and bounded host memory', async () => {
    keeper = new Keeper(new E2BBackend(apiKey as string, undefined, true), opts())
    await keeper.init()
    const client = await keeper.rpc()
    // Create 500 files; one holds the needle.
    await client.spawn({
      argv: ['/bin/sh', '-c', `mkdir -p ${SANDBOX_CWD}/big && for i in $(seq 1 500); do echo "line $i" > ${SANDBOX_CWD}/big/f$i.txt; done; echo NEEDLE-42 > ${SANDBOX_CWD}/big/target.txt`],
      cwd: SANDBOX_CWD, stdout: 'ignore', stderr: 'ignore', stdin: 'ignore', env: { PATH: '/usr/bin:/bin' }, graceMs: 30_000,
    }).then(p => p.exit)

    const grep = await client.spawn({
      argv: ['/bin/sh', '-c', `grep -rl NEEDLE-42 ${SANDBOX_CWD}/big`],
      cwd: SANDBOX_CWD, stdout: 'collect', stderr: 'ignore', stdin: 'ignore', env: { PATH: '/usr/bin:/bin' }, graceMs: 30_000,
    })
    const chunks: Buffer[] = []
    grep.onStdout(b => chunks.push(Buffer.from(b)))
    const exit = await grep.exit
    expect(exit.exitCode).toBe(0)
    expect(Buffer.concat(chunks).toString()).toContain('target.txt')
    expect(process.memoryUsage().heapUsed).toBeLessThan(600 * 1024 * 1024)
  })

  it('scenario 1: keeps the sandbox alive past its base timeout via heartbeat', async () => {
    // Base pause deadline 30s; heartbeat every 8s pushes it forward. Wait 45s.
    keeper = new Keeper(new E2BBackend(apiKey as string, undefined, true), opts({ timeoutMs: 30_000, heartbeatMs: 8_000 }))
    await keeper.init()
    await new Promise(r => setTimeout(r, 45_000))
    const client = await keeper.rpc()
    await client.ping()
    expect(await client.readText(`${SANDBOX_CWD}/README.md`)).toBe('# remote sandbox e2e\n')
  })

  it('scenario 2: resumes the same sandbox after a pause, disk retained', async () => {
    keeper = new Keeper(new E2BBackend(apiKey as string, undefined, true), opts())
    await keeper.init()
    const first = await keeper.rpc()
    await first.writeText({ path: `${SANDBOX_CWD}/state.txt`, content: 'survives-pause\n' })
    const handle = (keeper as unknown as { handle: { pause(): Promise<void> } }).handle
    await handle.pause()
    await expect.poll(() => first.closed, { timeout: 15_000 }).toBe(true)
    const resumed = await keeper.rpc()
    expect(await resumed.readText(`${SANDBOX_CWD}/state.txt`)).toBe('survives-pause\n')
  })

  it('scenario 3: recreates the sandbox after a hard kill and restores from the snapshot', async () => {
    keeper = new Keeper(new E2BBackend(apiKey as string, undefined, true), opts())
    await keeper.init()
    const first = await keeper.rpc()
    const handle = (keeper as unknown as { handle: { kill(): Promise<void> } }).handle
    await handle.kill()
    await expect.poll(() => first.closed, { timeout: 20_000 }).toBe(true)
    const recovered = await keeper.rpc()
    expect(recovered).not.toBe(first)
    // README came from the host snapshot IN sync during recreate.
    expect(await recovered.readText(`${SANDBOX_CWD}/README.md`)).toBe('# remote sandbox e2e\n')
    expect(keeper.consumeRecoveryNotice()).toMatch(/recovered after a connection loss/)
  })

  it('scenario 6: invokes a custom syncIn/syncOut override', async () => {
    let inCalled = false
    let outCalled = false
    keeper = new Keeper(new E2BBackend(apiKey as string, undefined, true), opts({
      syncIn: async (c) => { inCalled = true; await defaultSyncIn(c) },
      syncOut: async (c) => { outCalled = true; await defaultSyncOut(c) },
    }))
    await keeper.init()
    await keeper.syncOut()
    expect(inCalled).toBe(true)
    expect(outCalled).toBe(true)
  })
})

import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['packages/*/tests/**/*.e2e.test.ts'],
    testTimeout: 300000,
    hookTimeout: 300000,
    // Real sandboxes and one shared E2B account: run the scenarios serially.
    fileParallelism: false,
    pool: 'forks',
    maxWorkers: 1,
    minWorkers: 1,
  },
})

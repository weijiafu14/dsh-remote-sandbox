import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['packages/*/tests/**/*.test.ts'],
    exclude: ['**/node_modules/**', '**/*.e2e.test.ts'],
    testTimeout: 20000,
    hookTimeout: 20000,
  },
})

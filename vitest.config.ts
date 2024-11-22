import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    globals: true,
    isolate: false,
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
    env: {
      PGTMP_TEST: 'true',
    },
  },
})

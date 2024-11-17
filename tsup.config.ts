import { defineConfig } from 'tsup'

export default defineConfig({
  entry: ['src/mod.ts', 'src/stop.ts', 'src/initdb.ts'],
  format: ['esm'],
  dts: true,
})

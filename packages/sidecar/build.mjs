/**
 * Bundle the sidecar into one self-contained CommonJS file the keeper uploads
 * into the sandbox and runs with the sandbox's own Node. Pure JS only (ws has
 * optional native accelerators it degrades without), so no native binding has
 * to exist in the sandbox image.
 */
import { build } from 'esbuild'
import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const here = dirname(fileURLToPath(import.meta.url))
mkdirSync(join(here, 'dist'), { recursive: true })

await build({
  entryPoints: [join(here, 'src/main.ts')],
  bundle: true,
  platform: 'node',
  target: 'node18',
  format: 'cjs',
  outfile: join(here, 'dist/sidecar.cjs'),
  minify: false,
  legalComments: 'none',
})

console.log('sidecar bundled -> dist/sidecar.cjs')

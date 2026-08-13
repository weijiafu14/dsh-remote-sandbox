/**
 * Host-side entry point for the sidecar package: locates the bundled sidecar
 * program the keeper uploads into a sandbox. The runtime executor itself lives
 * in `main.ts` and is bundled to `dist/sidecar.cjs`; this module only resolves
 * that artifact's path so the keeper never hardcodes a layout.
 * @module dsh-remote-sidecar
 */

import { fileURLToPath } from 'node:url'

/**
 * Absolute path to the bundled sidecar program (`dist/sidecar.cjs`).
 * @returns the on-disk path of the single-file sidecar bundle.
 */
export function sidecarBundlePath(): string {
  return fileURLToPath(new URL('../dist/sidecar.cjs', import.meta.url))
}

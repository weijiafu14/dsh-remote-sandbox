#!/usr/bin/env bash
# Publish all six packages to npm in topological order.
#
# The packages must already be built (lib/ + sidecar dist/ present); this script
# does NOT rebuild, because in the deepseek-harness dev tree the toolchain lives
# at the harness root, not here. If you need to (re)build first, run from the
# harness root:  pnpm --filter './external/remote-sandbox/packages/*' run build
#
# npm 2FA applies: you are prompted for a one-time code, or set an automation
# token with bypass-2FA first:
#   npm config set //registry.npmjs.org/:_authToken=<token>
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f packages/protocol/lib/index.js ] || [ ! -f packages/sidecar/dist/sidecar.cjs ]; then
  echo "error: build outputs missing. Build first from the harness root:" >&2
  echo "  pnpm --filter './external/remote-sandbox/packages/*' run build" >&2
  exit 1
fi

echo "publishing to npm (topological order, may prompt for OTP)..."
pnpm -r --filter './packages/*' publish \
  --no-git-checks --access public \
  --registry=https://registry.npmjs.org

echo "done. verify: npm view dsh-remote-sandbox version"

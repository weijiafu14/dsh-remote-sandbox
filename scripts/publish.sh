#!/usr/bin/env bash
# Publish all six packages to npm in topological order.
#
# The packages must already be built (lib/ + sidecar dist/ present); this script
# does NOT rebuild, because in the deepseek-harness dev tree the toolchain lives
# at the harness root, not here.
#
# npm 2FA: pass your authenticator's current 6-digit code as the first argument.
# One code covers the whole batch (the packages are tiny and publish in seconds,
# well within the code's 30s validity):
#
#   bash scripts/publish.sh 123456
#
# Alternatively, set a bypass-2FA automation token and omit the code:
#   npm config set //registry.npmjs.org/:_authToken=<token>
#   bash scripts/publish.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f packages/protocol/lib/index.js ] || [ ! -f packages/sidecar/dist/sidecar.cjs ]; then
  echo "error: build outputs missing. Build first from the harness root:" >&2
  echo "  pnpm --filter './external/remote-sandbox/packages/*' run build" >&2
  exit 1
fi

OTP_ARG=()
if [ "${1:-}" != "" ]; then
  OTP_ARG=(--otp="$1")
  echo "publishing with the provided OTP (topological order)..."
else
  echo "publishing (no OTP given — relying on an automation token)..."
fi

pnpm -r --filter './packages/*' publish \
  --no-git-checks --access public \
  --registry=https://registry.npmjs.org \
  "${OTP_ARG[@]}"

echo "done. verify: npm view dsh-remote-sandbox version"

#!/usr/bin/env bash
# Publish all six packages to npm in topological order.
# Run from a checkout inside the deepseek-harness source tree (where the
# @deepseek-ai/dsh-* peer packages resolve). npm 2FA applies: you will be
# prompted for a one-time code, or set an automation token with bypass-2FA
# first (npm config set //registry.npmjs.org/:_authToken=<token>).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "building all packages..."
pnpm -r --filter './packages/*' run build

echo "publishing to npm (topological order, may prompt for OTP)..."
pnpm -r --filter './packages/*' publish \
  --no-git-checks --access public \
  --registry=https://registry.npmjs.org

echo "done. verify: npm view dsh-remote-sandbox version"

#!/bin/sh
set -eu

MODE="${1:-dev}"

cd /workspace/dashboard

corepack enable >/dev/null 2>&1 || true

LOCK_HASH="$(sha256sum pnpm-lock.yaml | awk '{print $1}')"
LOCK_MARKER="node_modules/.wa-lockfile-sha"

if [ ! -d node_modules ] || [ ! -f "$LOCK_MARKER" ] || [ "$(cat "$LOCK_MARKER" 2>/dev/null || true)" != "$LOCK_HASH" ]; then
  mkdir -p node_modules
  find node_modules -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  cp -a /opt/dashboard-seed/node_modules/. node_modules/
  pnpm install --frozen-lockfile
  printf '%s' "$LOCK_HASH" > "$LOCK_MARKER"
fi

case "$MODE" in
  npm)
    shift
    echo "El flujo Docker del dashboard usa pnpm. Usa: docker compose run --rm dashboard-build pnpm <comando>" >&2
    exec pnpm "$@"
    ;;
  pnpm)
    shift
    exec pnpm "$@"
    ;;
  dev)
    pnpm run sync:data
    exec pnpm exec vite --host 0.0.0.0 --port "${PORT:-5173}"
    ;;
  build)
    pnpm run build:release
    ;;
  sync)
    pnpm run sync:data
    ;;
  *)
    echo "Unsupported dashboard docker mode: $MODE" >&2
    exit 1
    ;;
esac

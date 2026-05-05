#!/bin/sh
set -eu

MODE="${1:-dev}"

cd /workspace/dashboard

LOCK_HASH="$(sha256sum package-lock.json | awk '{print $1}')"
LOCK_MARKER="node_modules/.wa-lockfile-sha"

if [ ! -d node_modules ] || [ ! -f "$LOCK_MARKER" ] || [ "$(cat "$LOCK_MARKER" 2>/dev/null || true)" != "$LOCK_HASH" ]; then
  mkdir -p node_modules
  find node_modules -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  cp -a /opt/dashboard-seed/node_modules/. node_modules/
  npm ci
  printf '%s' "$LOCK_HASH" > "$LOCK_MARKER"
fi

case "$MODE" in
  npm)
    shift
    exec npm "$@"
    ;;
  dev)
    npm run sync:data
    exec npm run dev -- --host 0.0.0.0 --port "${PORT:-5173}"
    ;;
  build)
    npm run build:release
    ;;
  sync)
    npm run sync:data
    ;;
  *)
    echo "Unsupported dashboard docker mode: $MODE" >&2
    exit 1
    ;;
esac

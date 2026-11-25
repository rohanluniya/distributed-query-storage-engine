#!/usr/bin/env bash
# Capture headless Chrome screenshots of the live Control Tower with concurrency lock.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$REPO_ROOT/docs/assets"
URL="${1:-http://127.0.0.1:8090/}"
mkdir -p "$OUT_DIR"

CHROME_BIN=""
for candidate in \
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  "$(command -v google-chrome 2>/dev/null || true)" \
  "$(command -v chromium 2>/dev/null || true)"; do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    CHROME_BIN="$candidate"
    break
  fi
done

if [ -z "$CHROME_BIN" ]; then
  echo "Headless Chrome binary not found" >&2
  exit 1
fi

while ! mkdir /tmp/chrome_screenshot_lock 2>/dev/null; do sleep 1; done
trap 'rmdir /tmp/chrome_screenshot_lock 2>/dev/null || true' EXIT

"$CHROME_BIN" \
  --headless=new \
  --disable-gpu \
  --no-sandbox \
  --hide-scrollbars \
  --window-size=1520,960 \
  --virtual-time-budget=8000 \
  --screenshot="$OUT_DIR/query_workbench_top.png" \
  "$URL"

sleep 1.5

"$CHROME_BIN" \
  --headless=new \
  --disable-gpu \
  --no-sandbox \
  --hide-scrollbars \
  --window-size=1520,1940 \
  --virtual-time-budget=8000 \
  --screenshot="$OUT_DIR/query_workbench_full.png" \
  "$URL"

echo "Captured screenshots:"
ls -lh "$OUT_DIR/query_workbench_top.png" "$OUT_DIR/query_workbench_full.png"

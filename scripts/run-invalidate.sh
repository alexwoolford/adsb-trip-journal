#!/usr/bin/env bash
# Unlock UTC days so the next collect walk-back replays gzip caches (default)
# or re-GETs (--drop-cache). 12/12 never auto-invalidates on a binary upgrade.
# Usage: run-invalidate.sh --from YYYY-MM-DD [--to YYYY-MM-DD] [--drop-cache]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ADSB_TRIP_JOURNAL_BIN:-$ROOT/bin/adsb-trip-journal}"
STATE="${TRIP_JOURNAL_DATA:-/var/lib/adsb-trip-journal}"
JOURNAL="${TRIP_JOURNAL_SQLITE:-$STATE/trips.sqlite}"
CACHE="${TRIP_JOURNAL_CACHE:-$STATE/cache}"

test -x "$BIN" || {
  echo "missing $BIN — build with: cargo build --release" >&2
  exit 1
}

exec "$BIN" \
  --journal-sqlite "$JOURNAL" \
  --data-dir "$STATE" \
  --cache-dir "$CACHE" \
  invalidate "$@"

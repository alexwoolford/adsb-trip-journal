#!/usr/bin/env bash
# Print journal coverage (fleet snapshot, trips, cursors).
# Paths default to the production layout so this works without sourcing the 600 env file.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ADSB_TRIP_JOURNAL_BIN:-$ROOT/bin/adsb-trip-journal}"
STATE="${TRIP_JOURNAL_DATA:-/var/lib/adsb-trip-journal}"
MAPPING="${TAIL_TO_TICKER_SQLITE:-/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite}"
JOURNAL="${TRIP_JOURNAL_SQLITE:-$STATE/trips.sqlite}"
CACHE="${TRIP_JOURNAL_CACHE:-$STATE/cache}"

test -x "$BIN" || {
  echo "missing $BIN — build with: cargo build --release" >&2
  exit 1
}
test -f "$MAPPING" || {
  echo "missing mapping sqlite $MAPPING" >&2
  exit 1
}

exec "$BIN" \
  --mapping-sqlite "$MAPPING" \
  --journal-sqlite "$JOURNAL" \
  --data-dir "$STATE" \
  --cache-dir "$CACHE" \
  status

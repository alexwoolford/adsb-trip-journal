#!/usr/bin/env bash
# Dry-run (default) or --apply: drop unit-test fixture trips and UTC days that
# never stored callsign / airport-quality ints. Does not unlock 12/12.
# Paths default to the production layout so this works without the 600 env file.
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
  gc "$@"

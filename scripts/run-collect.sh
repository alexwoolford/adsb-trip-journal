#!/usr/bin/env bash
# Oneshot: collect yesterday UTC via 12× GET /flights/all (filter mapped fleet).
# Then leftover credits fill never-started/incomplete days in the 90-day window.
# Wrapper default 800 (laptop). Production host env is 3600. Not gated on seen_airborne.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ADSB_TRIP_JOURNAL_BIN:-$ROOT/bin/adsb-trip-journal}"
STATE="${TRIP_JOURNAL_DATA:-/var/lib/adsb-trip-journal}"
MAPPING="${TAIL_TO_TICKER_SQLITE:-/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite}"
JOURNAL="${TRIP_JOURNAL_SQLITE:-$STATE/trips.sqlite}"
CACHE="${TRIP_JOURNAL_CACHE:-$STATE/cache}"
LOCK="${ADSB_COLLECT_LOCK:-$STATE/.collect.lock}"
MAX_CREDITS="${OPENSKY_MAX_FLIGHTS_CREDITS:-800}"

test -x "$BIN" || {
  echo "missing $BIN — build with: cargo build --release" >&2
  exit 1
}
test -f "$MAPPING" || {
  echo "missing mapping sqlite $MAPPING" >&2
  exit 1
}
test -f "$CACHE/airports.csv" || {
  echo "missing $CACHE/airports.csv — copy OurAirports or run airports-fetch" >&2
  exit 1
}

mkdir -p "$STATE"

acquire_lock() {
  if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOCK"
    if ! flock -n 9; then
      echo "collect already running (lock $LOCK)" >&2
      exit 1
    fi
  else
    if ! mkdir "$LOCK.d" 2>/dev/null; then
      echo "collect already running (lock $LOCK.d)" >&2
      exit 1
    fi
    trap 'rmdir "$LOCK.d" 2>/dev/null || true' EXIT
  fi
}
acquire_lock

echo "== adsb-trip-journal collect (yesterday UTC) =="
echo "bin=$BIN mapping=$MAPPING max_flights_credits=$MAX_CREDITS"
"$BIN" \
  --mapping-sqlite "$MAPPING" \
  --journal-sqlite "$JOURNAL" \
  --data-dir "$STATE" \
  --cache-dir "$CACHE" \
  collect --source opensky --max-flights-credits "$MAX_CREDITS"
echo "journal → $JOURNAL"

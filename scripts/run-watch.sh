#!/usr/bin/env bash
# Long-running OpenSky /states/all watch. Writes seen_airborne for today UTC.
# Optional diagnostic: collect does not read this table. Keep the unit for a
# manual “who is up” poll; disable after nightly /flights/all looks healthy.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${ADSB_TRIP_JOURNAL_BIN:-$ROOT/bin/adsb-trip-journal}"
STATE="${TRIP_JOURNAL_DATA:-/var/lib/adsb-trip-journal}"
MAPPING="${TAIL_TO_TICKER_SQLITE:-/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite}"
JOURNAL="${TRIP_JOURNAL_SQLITE:-$STATE/trips.sqlite}"
CACHE="${TRIP_JOURNAL_CACHE:-$STATE/cache}"
INTERVAL="${WATCH_INTERVAL_SECS:-600}"

test -x "$BIN" || {
  echo "missing $BIN — build with: cargo build --release" >&2
  exit 1
}
test -f "$MAPPING" || {
  echo "missing mapping sqlite $MAPPING" >&2
  exit 1
}

echo "== adsb-trip-journal watch =="
echo "bin=$BIN mapping=$MAPPING interval=${INTERVAL}s"
exec "$BIN" \
  --mapping-sqlite "$MAPPING" \
  --journal-sqlite "$JOURNAL" \
  --data-dir "$STATE" \
  --cache-dir "$CACHE" \
  watch --interval-secs "$INTERVAL"

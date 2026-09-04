#!/usr/bin/env bash
# Install adsb-trip-journal under /opt and enable systemd (Linux).
# Usage (as root): ./deploy/install.sh
#
# Prefer building the release binary as a normal user first:
#   cargo build --release
#   sudo ./deploy/install.sh
# Set FORCE_REBUILD=1 to rebuild even when target/release/adsb-trip-journal exists.
#
# Optional inputs (do not overwrite existing host copies unless FORCE_*=1):
#   ADSB_MAPPING_SQLITE  optional leftover copy under $STATE/mapping (not the live feed)
#   ADSB_AIRPORTS_CSV    path to OurAirports airports.csv
#   ADSB_ENV_FILE        populated env (chmod 600); used only if dest is missing
# Live mapping is TAIL_TO_TICKER_SQLITE in the env file (default
# /var/lib/tail-to-ticker/current/tail_to_ticker.sqlite). Units enable only
# when that file exists; $STATE/mapping/ is not required.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="${ADSB_INSTALL_PREFIX:-/opt/adsb-trip-journal}"
STATE="${ADSB_STATE_DIR:-/var/lib/adsb-trip-journal}"
USER_NAME="${ADSB_RUN_USER:-adsb}"
GROUP_NAME="${ADSB_RUN_GROUP:-$USER_NAME}"
BIN_SRC="$ROOT/target/release/adsb-trip-journal"
ENV_DST="$PREFIX/etc/adsb-trip-journal.env"
MAPPING_DST="$STATE/mapping/tail_to_ticker.sqlite"
AIRPORTS_DST="$STATE/cache/airports.csv"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

build_release() {
  local build_user="${SUDO_USER:-}"
  if [[ -n "$build_user" && "$build_user" != "root" ]] && id -u "$build_user" >/dev/null 2>&1; then
    echo "== build release (as $build_user) =="
    sudo -u "$build_user" -H bash -lc "cd \"$ROOT\" && source \"\$HOME/.cargo/env\" 2>/dev/null || true; cargo build --release"
    return
  fi
  echo "no release binary at $BIN_SRC and no non-root SUDO_USER to build as." >&2
  echo "build first: cargo build --release" >&2
  echo "then re-run: sudo ./deploy/install.sh" >&2
  exit 1
}

if [[ -x "$BIN_SRC" && -z "${FORCE_REBUILD:-}" ]]; then
  echo "== using existing release binary: $BIN_SRC =="
else
  build_release
fi

test -x "$BIN_SRC" || {
  echo "missing $BIN_SRC — build with: cargo build --release" >&2
  exit 1
}

echo "== create user/dirs =="
NLOGIN="/usr/sbin/nologin"
[[ -x "$NLOGIN" ]] || NLOGIN="/sbin/nologin"
if ! id -u "$USER_NAME" >/dev/null 2>&1; then
  useradd --system --home-dir "$STATE" --shell "$NLOGIN" "$USER_NAME" || true
fi
mkdir -p "$PREFIX"/{bin,scripts,etc,docs} \
  "$STATE"/mapping \
  "$STATE"/cache \
  /etc/systemd/system

echo "== install files =="
install -m 0755 "$BIN_SRC" "$PREFIX/bin/adsb-trip-journal"
install -m 0755 "$ROOT/scripts/run-watch.sh" "$PREFIX/scripts/run-watch.sh"
install -m 0755 "$ROOT/scripts/run-collect.sh" "$PREFIX/scripts/run-collect.sh"
install -m 0755 "$ROOT/scripts/run-status.sh" "$PREFIX/scripts/run-status.sh"
install -m 0644 "$ROOT/docs/DAILY_OPS.md" "$PREFIX/docs/DAILY_OPS.md"

if [[ ! -f "$ENV_DST" ]]; then
  if [[ -n "${ADSB_ENV_FILE:-}" && -f "$ADSB_ENV_FILE" ]]; then
    install -m 0600 "$ADSB_ENV_FILE" "$ENV_DST"
  else
    install -m 0600 "$ROOT/deploy/adsb-trip-journal.env.example" "$ENV_DST"
  fi
fi
chmod 0600 "$ENV_DST"

if [[ -n "${ADSB_MAPPING_SQLITE:-}" && -f "$ADSB_MAPPING_SQLITE" ]]; then
  if [[ ! -f "$MAPPING_DST" || -n "${FORCE_MAPPING:-}" ]]; then
    install -m 0644 "$ADSB_MAPPING_SQLITE" "$MAPPING_DST"
  fi
fi
if [[ -n "${ADSB_AIRPORTS_CSV:-}" && -f "$ADSB_AIRPORTS_CSV" ]]; then
  if [[ ! -f "$AIRPORTS_DST" || -n "${FORCE_AIRPORTS:-}" ]]; then
    install -m 0644 "$ADSB_AIRPORTS_CSV" "$AIRPORTS_DST"
  fi
elif [[ -f "$ROOT/cache/airports.csv" ]]; then
  if [[ ! -f "$AIRPORTS_DST" || -n "${FORCE_AIRPORTS:-}" ]]; then
    install -m 0644 "$ROOT/cache/airports.csv" "$AIRPORTS_DST"
  fi
fi

chown -R "$USER_NAME:$GROUP_NAME" "$STATE"
chown -R root:root "$PREFIX"
chown root:"$GROUP_NAME" "$PREFIX/etc" "$ENV_DST"
chmod 0750 "$PREFIX/etc"
chmod 0600 "$ENV_DST"
chmod 0755 "$PREFIX/scripts"/*.sh

install -m 0644 "$ROOT/deploy/systemd/adsb-trip-journal-watch.service" \
  /etc/systemd/system/adsb-trip-journal-watch.service
install -m 0644 "$ROOT/deploy/systemd/adsb-trip-journal-collect.service" \
  /etc/systemd/system/adsb-trip-journal-collect.service
install -m 0644 "$ROOT/deploy/systemd/adsb-trip-journal-collect.timer" \
  /etc/systemd/system/adsb-trip-journal-collect.timer

if command -v restorecon >/dev/null 2>&1; then
  echo "== SELinux restorecon =="
  restorecon -Rv "$PREFIX" "$STATE" || true
fi

systemctl daemon-reload

creds_set=0
if [[ -f "$ENV_DST" ]] && grep -qE '^OPENSKY_CLIENT_SECRET=.+' "$ENV_DST"; then
  creds_set=1
fi

live_mapping="/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite"
if [[ -f "$ENV_DST" ]]; then
  live_val="$(grep -E '^TAIL_TO_TICKER_SQLITE=' "$ENV_DST" | tail -n1 | cut -d= -f2- || true)"
  live_val="${live_val%\"}"
  live_val="${live_val#\"}"
  if [[ -n "$live_val" ]]; then
    live_mapping="$live_val"
  fi
fi

echo "installed:"
echo "  prefix=$PREFIX state=$STATE"
echo "  live mapping: $live_mapping"
echo "  leftover copy: $MAPPING_DST (optional; not the live feed)"
echo "  airports: $AIRPORTS_DST"
echo "  logs: journalctl -u adsb-trip-journal-watch.service -u adsb-trip-journal-collect.service"
echo "  edit: $ENV_DST (chmod 600)"

if [[ ! -f "$live_mapping" ]]; then
  echo "  missing live mapping sqlite $live_mapping" >&2
  echo "  publish tail-to-ticker current/ or set TAIL_TO_TICKER_SQLITE in $ENV_DST" >&2
  echo "  units not enabled." >&2
  exit 1
fi
if [[ ! -f "$AIRPORTS_DST" ]]; then
  echo "  missing $AIRPORTS_DST — copy airports.csv or run airports-fetch" >&2
  echo "  units not enabled." >&2
  exit 1
fi

if [[ "$creds_set" -eq 1 ]]; then
  systemctl enable --now adsb-trip-journal-watch.service
  systemctl enable --now adsb-trip-journal-collect.timer
  echo "  watch: adsb-trip-journal-watch.service enabled"
  echo "  timer: adsb-trip-journal-collect.timer enabled (daily 06:00 UTC + 15m jitter)"
else
  echo "  OPENSKY_CLIENT_SECRET is empty — units not enabled."
  echo "  Fill credentials in $ENV_DST, then:"
  echo "    sudo systemctl enable --now adsb-trip-journal-watch.service"
  echo "    sudo systemctl enable --now adsb-trip-journal-collect.timer"
fi

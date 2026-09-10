# ADS-B trip journal (ops)

## Product

This sibling of **tail-to-ticker** reads `mappings_current` (registrant → ticker) and writes a restartable trip journal. v1 is **collection only** — no alerts, geofences, or scores.

Join key is lowercase **`icao24`**. Default fleet: `aviation_issuer = 0 AND fleet_size BETWEEN 1 AND 6`.

OpenSky Standard REST: **4,000 credits/day per independent bucket** (states / flights / tracks). Historical `GET /flights/aircraft` costs **30** per hex-day and cannot cover a busy fleet. Production collect uses **12× `GET /flights/all`** (2h slices) and keeps mapped hexes. Watch is not a collect gate.

## Scheduler and telemetry

Collect is `adsb-trip-journal-collect.timer` + `Type=oneshot`. Watch (optional) is `Type=simple`. **The OS is the scheduler.** Do not add an in-process cron for collect.

Operator logs: `tracing` on stderr → journald (`SyslogIdentifier` matches the unit). Default `RUST_LOG=info`.

`flights_all_day` / `flights_all_slice` (and collect reports) are capturable domain telemetry, including credit spend. Query them in mosaic.

## Cadence

| Job | Unit | Behavior |
|---|---|---|
| **Watch** | `adsb-trip-journal-watch.service` | **Optional** diagnostic; `install.sh` does **not** enable it. Long-running `/states/all` every 10 min. icao24-filtered calls cost **4** states-credits each (not serial-only 1). Fleet is chunked by 80 hexes, so ~331 hexes = 5 calls ≈ **20** credits/poll. Record `seen_airborne` for today UTC (collect does not use this as an allow-list or resume signal). Reloads the mapping fleet each poll. Enable by hand for a “who is up” poll. |
| **Collect** | `adsb-trip-journal-collect.timer` | Daily **06:00 UTC** + up to 15 min jitter. Twelve `GET /flights/all` slices for yesterday UTC, then leftover flights credits walk **newest-first** over never-started or incomplete days (no 90-day floor). A never-started historical day starts only when remaining ≥ **360**; incomplete days resume. Filter to the mapped fleet; persist fleet-filtered JSON. Day complete only after 12/12. Host cap `OPENSKY_MAX_FLIGHTS_CREDITS` **3600** (~10 UTC days/run, ~400 slack). CLI/laptop default **800**. `install.sh` does not overwrite an existing env file. |

404 on `/flights/all` for a 2h global window is rare (empty interval). 429 does not mark remaining slices complete. Registrant is not operator. Coverage is thinner than ADS-B Exchange (no MLAT).

Do **not** run `probe --opensky --flights-all` or extra `collect --hex` on the production host without a reason (those spend the flights bucket). `probe --opensky --flights-all` is a one-shot measurement (one 2h slice). Do not call `/flights/aircraft`.

## Deploy (systemd)

**Prefer rsync or git clone + [`deploy/install.sh`](../deploy/install.sh) + systemd.** Same `/opt` + `/var/lib` split as other collectors on the host. Do not run production from `$HOME` with cron. Do not collide with unrelated Docker stacks.

Host prerequisites: outbound HTTPS to OpenSky, Rust toolchain on the box (or a prebuilt `aarch64-unknown-linux-gnu` binary), mapping sqlite, OurAirports `airports.csv`, OpenSky OAuth2 client credentials.

```bash
cargo build --release
# live mapping is TAIL_TO_TICKER_SQLITE in the env (producer current/ sqlite)
sudo ADSB_AIRPORTS_CSV=/path/airports.csv \
     ADSB_ENV_FILE=/path/adsb-trip-journal.env \
     ./deploy/install.sh
```

Units in [`deploy/systemd/`](../deploy/systemd/):

| Unit | Schedule |
|---|---|
| `adsb-trip-journal-watch.service` | installed, not enabled (`Restart=on-failure` if started by hand) |
| `adsb-trip-journal-collect.timer` | daily 06:00 UTC (`Persistent=true`) |

Config: `/opt/adsb-trip-journal/etc/adsb-trip-journal.env` (from [`deploy/adsb-trip-journal.env.example`](../deploy/adsb-trip-journal.env.example), **chmod 600**). Install does not overwrite an existing env file.

First collect after a new install covers yesterday via `/flights/all`, then walks **newest-first** over never-started days until leftover credits cannot buy another whole UTC day (360). A partial first UTC day of watch is unrelated to collect completeness. Watch does not change collect order.

## Layout

```
/opt/adsb-trip-journal/
  bin/adsb-trip-journal
  scripts/run-watch.sh
  scripts/run-collect.sh
  scripts/run-status.sh
  scripts/run-gc.sh
  scripts/run-invalidate.sh
  docs/DAILY_OPS.md
  etc/adsb-trip-journal.env
/var/lib/adsb-trip-journal/
  trips.sqlite
  mapping/tail_to_ticker.sqlite   # stale one-shot copy; not the live feed
  cache/airports.csv
/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite  # live mapping (producer)
```

Upgrades: pull/rsync → `cargo build --release` → `sudo ./deploy/install.sh` (env is preserved). Wrappers default to the producer’s `current/` sqlite; `$STATE/mapping/` is not required.

## Verify

```bash
systemctl list-timers 'adsb-trip-journal-*'
journalctl -u adsb-trip-journal-collect.service -n 50 --no-pager
# Watch is optional and off by default:
# systemctl status adsb-trip-journal-watch.service --no-pager

# Does not need the 600 env file (paths are baked into the wrapper).
sudo -u adsb /opt/adsb-trip-journal/scripts/run-status.sh
```

Production journal is **only** `/var/lib/adsb-trip-journal/trips.sqlite`. Do not rsync a laptop `data/trips.sqlite` onto the host (unit-test fixture `abcdef` / `N1` leaked that way once).

## Schema change after 12/12 (invalidate / gc)

Completeness is a **credit lock**, not a schema version. A binary that grows `trips` columns (`callsign`, airport-quality ints) does **not** re-GET complete days. To backfill:

1. Check one gzip: `gzip -dc /var/lib/adsb-trip-journal/cache/flights_all/YYYY-MM-DD/00.json.gz | head` — if `"callsign"` is in the FlightObject, keep the cache.
2. Unlock: `sudo -u adsb /opt/adsb-trip-journal/scripts/run-invalidate.sh --from YYYY-MM-DD --to YYYY-MM-DD` (keep-cache default). Next `collect.timer` newest-first walk replays gzips; **0 flights credits**.
3. To re-GET instead: add `--drop-cache` (360 flights-credits per UTC day).

Forward-only purge (no unlock, no spend): drop the unit-test fixture and UTC days that never stored callsign **and** never stored `dep_airport_horiz_m`:

```bash
sudo -u adsb /opt/adsb-trip-journal/scripts/run-gc.sh
sudo -u adsb /opt/adsb-trip-journal/scripts/run-gc.sh --apply
```

Those days stay 12/12. Mosaic follows via `_outbox` `D` after drain. Do not add a silent ingest-schema bump that spends the flights bucket.

Each `trips` row is one hop. A same-day Tulsa→Houston→Tulsa day is two rows. OpenSky airport labels farther than 8 km are not landings: collect spends **tracks** credits (4 each, 4,000/day bucket) to snap `/tracks` endpoints. Re-GET `/flights/all` (`invalidate --drop-cache`) returns the same FlightObjects and does **not** fix bad idents.

To rebuild a cached UTC day (0 flights credits; tracks only for untrusted legs):

```bash
# After deploying a binary that skips same-ident / far-horiz guesses:
sudo -u adsb /usr/local/bin/sqlite3 /var/lib/adsb-trip-journal/trips.sqlite \
  "DELETE FROM trips WHERE substr(dep_ts,1,10) = 'YYYY-MM-DD';"
sudo -u adsb /opt/adsb-trip-journal/scripts/run-invalidate.sh --from YYYY-MM-DD --to YYYY-MM-DD
sudo -u adsb /opt/adsb-trip-journal/scripts/run-collect.sh --from YYYY-MM-DD --to YYYY-MM-DD
```

Leave the collector running so `_outbox` drains. Do not `--drop-cache` unless the gzip is missing fields. If `/tracks` returns 429, replay with `--no-tracks-fallback` to keep hops whose OpenSky idents are inside 8 km; invalidate and collect again when the tracks bucket resets (4,000/day, separate from flights).

## Timer failed

`Persistent=true` will retry after a reboot. It will not page you.

1. `systemctl is-failed adsb-trip-journal-collect.service` and `systemctl list-timers 'adsb-trip-journal-*'`.
2. `journalctl -u adsb-trip-journal-collect.service -n 80 --no-pager`. 401/403 on OpenSky is credentials. Missing mapping sqlite is `TAIL_TO_TICKER_SQLITE`. 429 should resume from slice cache.
3. Confirm host env `OPENSKY_MAX_FLIGHTS_CREDITS=3600` (wrapper default is 800 if unset). `install.sh` does not overwrite an existing env file.
4. Leave `trips.sqlite` in place. Re-run: `sudo systemctl start adsb-trip-journal-collect.service`.
5. `sudo -u adsb /opt/adsb-trip-journal/scripts/run-status.sh` — yesterday UTC should reach 12/12 slices unless the credit cap stopped the walk-back.

Manual collect:

```bash
sudo systemctl start adsb-trip-journal-collect.service
```

## Mapping refresh

Watch re-opens `TAIL_TO_TICKER_SQLITE` every poll. Production path is the producer’s published file:

```
TAIL_TO_TICKER_SQLITE=/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite
```

`ProtectHome=true` — do not put mapping under a home directory. tail-to-ticker refresh atomically replaces that file daily at 07:00 UTC. `$STATE/mapping/tail_to_ticker.sqlite` is a leftover copy, not the live feed.

## Credit budget

- Watch: ~**20** states-credits/poll × 144 ≈ **2,880**/day of the 4,000 **states** bucket (icao24 filter is 4/call × 5 chunks for a ~331-hex fleet). Independent of flights. Optional; collect does not spend this.
- Collect: **12 × `/flights/all`** per UTC day of the **flights** bucket (**30**/slice measured 2026-09-03 = **360**/day). Host cap **3600** (CLI/laptop **800**). After yesterday, leftover credits fill **newer-first** history (whole UTC days). Do not start a never-started historical day unless remaining ≥ **360**; resume incomplete days. Cached slices cost 0. `--hex` does not shrink the cache. Ingest keeps the full FlightObject on mapped rows: `callsign` (label, often null) plus airport-estimate quality integers. Complete days are not re-GET unless an operator runs `invalidate`. Default `invalidate` keeps gzip caches so the next collect **replays** FlightObjects (no credits). `--drop-cache` re-GETs. `gc --apply` deletes fixture / pre-payload days without unlocking 12/12.
- Tracks fallback when **either** OpenSky airport ident is missing, not in OurAirports, or farther than 8 km (tracks bucket). Successful (and empty) `/tracks` attempts are cached per slice so a 429 resume does not re-call the same `icao24+firstSeen`. Track `callsign` fills the trip only when the FlightObject had none.

## Limits

- OpenSky Standard 4k/day/**bucket**; per-hex `/flights/aircraft` at 30 cannot cover a busy fleet day
- Collect is not gated on `seen_airborne`
- OpenSky resume is `flights_all` 12/12, not per-hex `fetch_cursor`
- 401/403 on `/states/all` exits non-zero so systemd restarts the watch unit

## State capture (prep)

Logical name: `adsb-trip-journal`. Watch the writable journal, not a copy.

| Path | Role |
|---|---|
| `/var/lib/adsb-trip-journal/trips.sqlite` | Watched. `_outbox` + triggers. |
| `/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite` | Mapping input (read-only). Not this utility’s state. |

Capture set: `trips` (full), `seen_airborne` / `flights_all_slice` / `flights_all_day` (after). `fleet_snapshot` is DELETE+reload and is **not** captured. `fetch_cursor` is leftover and is **not** captured.

Outbox/triggers come from [`capturable-state`](https://github.com/alexwoolford/capturable-state) `v0.1.1`, not a copied `capture.rs`.

Env (collector is `state-capture` on this host; missing socket is ignored):

```
STATE_CAPTURE_SOCK=/run/state/collect.sock
STATE_CAPTURE_ANNOUNCE_DIR=/var/lib/state-capture/announce
```

If the announce dir cannot be created, announce is skipped (collector absent). If it exists but is not writable, `install` fails — no sibling `.capturable.json`. `ProtectSystem=strict` therefore includes `/var/lib/state-capture/announce` and `/run/state` in `ReadWritePaths`.

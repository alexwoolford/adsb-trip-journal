# ADS-B trip journal (ops)

## Product

This sibling of **tail-to-ticker** reads `mappings_current` (registrant → ticker) and writes a restartable trip journal. v1 is **collection only** — no alerts, geofences, or scores.

Join key is lowercase **`icao24`**. Default fleet: `aviation_issuer = 0 AND fleet_size BETWEEN 1 AND 6`.

OpenSky Standard REST: **4,000 credits/day per independent bucket** (states / flights / tracks). Historical `GET /flights/aircraft` costs **30** per hex-day and cannot cover a busy fleet. Production collect uses **12× `GET /flights/all`** (2h slices) and keeps mapped hexes. Watch is not a collect gate.

## Cadence

| Job | Unit | Behavior |
|---|---|---|
| **Watch** | `adsb-trip-journal-watch.service` | **Optional** diagnostic. Long-running `/states/all` every 10 min. icao24-filtered calls cost **4** states-credits each (not serial-only 1). Fleet is chunked by 80 hexes, so ~331 hexes = 5 calls ≈ **20** credits/poll. Record `seen_airborne` for today UTC (collect does not use this as an allow-list or resume signal). Reloads the mapping fleet each poll. Disable after nightly `/flights/all` looks healthy; keep the unit for a manual “who is up” poll. |
| **Collect** | `adsb-trip-journal-collect.timer` | Daily **06:00 UTC** + up to 15 min jitter. Twelve `GET /flights/all` slices for yesterday UTC, then leftover flights credits walk back over never-started or incomplete days in a **90-day** repair window. Filter to the mapped fleet; persist fleet-filtered JSON. Day complete only after 12/12. Host cap `OPENSKY_MAX_FLIGHTS_CREDITS` **3600** (~10 UTC days/run, ~400 slack). CLI/laptop default **800**. `install.sh` does not overwrite an existing env file. |

404 on `/flights/all` for a 2h global window is rare (empty interval). 429 does not mark remaining slices complete. Registrant is not operator. Coverage is thinner than ADS-B Exchange (no MLAT).

Do **not** run `probe --opensky` or extra `collect --hex` on the production host without a reason (those spend the flights bucket). `probe --opensky --flights-all` is a one-shot measurement (one 2h slice).

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
| `adsb-trip-journal-watch.service` | always on (`Restart=on-failure`) |
| `adsb-trip-journal-collect.timer` | daily 06:00 UTC (`Persistent=true`) |

Config: `/opt/adsb-trip-journal/etc/adsb-trip-journal.env` (from [`deploy/adsb-trip-journal.env.example`](../deploy/adsb-trip-journal.env.example), **chmod 600**). Install does not overwrite an existing env file.

First collect after a new install covers yesterday via `/flights/all`, then walks back never-started days in the 90-day window until the host cap. A partial first UTC day of watch is unrelated to collect completeness. Watch does not pull collect `--from` backward.

## Layout

```
/opt/adsb-trip-journal/
  bin/adsb-trip-journal
  scripts/run-watch.sh
  scripts/run-collect.sh
  scripts/run-status.sh
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
systemctl status adsb-trip-journal-watch.service --no-pager
systemctl list-timers 'adsb-trip-journal-*'
journalctl -u adsb-trip-journal-watch.service -u adsb-trip-journal-collect.service -n 50 --no-pager

# Does not need the 600 env file (paths are baked into the wrapper).
sudo -u adsb /opt/adsb-trip-journal/scripts/run-status.sh
```

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
- Collect: **12 × `/flights/all`** per UTC day of the **flights** bucket (**30**/slice measured 2026-09-03 = **360**/day). Host cap **3600** (CLI/laptop **800**). After yesterday, leftover credits fill never-started or incomplete days in a **90-day** window (oldest first). When that window is 12/12, leftover credits stay unused. `--hex` does not shrink the cache.
- Tracks fallback only when both airport estimates are missing (tracks bucket). Successful (and empty) `/tracks` attempts are cached per slice so a 429 resume does not re-call the same `icao24+firstSeen`.

## Limits

- OpenSky Standard 4k/day/**bucket**; per-hex `/flights/aircraft` at 30 cannot cover a busy fleet day
- Collect is not gated on `seen_airborne`
- OpenSky resume is `flights_all` 12/12, not per-hex `fetch_cursor`
- 401/403 on `/states/all` exits non-zero so systemd restarts the watch unit

# ADS-B trip journal (ops)

## Product

This sibling of **tail-to-ticker** reads `mappings_current` (registrant → ticker) and writes a restartable trip journal. v1 is **collection only** — no alerts, geofences, or scores.

Join key is lowercase **`icao24`**. Default fleet: `aviation_issuer = 0 AND fleet_size BETWEEN 1 AND 6`.

OpenSky Standard REST: **4,000 credits/day per bucket** (states / flights / tracks). Historical `GET /flights/aircraft` costs **30** flights-credits per call. Full-fleet yesterday would exceed the bucket, so production is **hybrid only**.

## Cadence

| Job | Unit | Behavior |
|---|---|---|
| **Watch** | `adsb-trip-journal-watch.service` | Long-running. Poll `/states/all` every 10 min (~1 states-credit). Record `seen_airborne` for today UTC. Reloads the mapping fleet each poll. |
| **Collect** | `adsb-trip-journal-collect.timer` | Daily **06:00 UTC** + up to 15 min jitter. Fetch yesterday’s flights for hexes in `seen_airborne`. Empty list **skips** (exit 0); no full-fleet fallback. Cap `--max-flights-credits` 3500 ≈ 116 calls. |

404 on `/flights/aircraft` means nothing received, not “did not fly.” Registrant is not operator. Coverage is thinner than ADS-B Exchange (no MLAT).

Do **not** run `probe --opensky` or `collect --hex` on the production host (those spend the flights bucket).

## Deploy (systemd)

**Prefer rsync or git clone + [`deploy/install.sh`](../deploy/install.sh) + systemd.** Same `/opt` + `/var/lib` split as other collectors on the host. Do not run production from `$HOME` with cron. Do not collide with unrelated Docker stacks.

Host prerequisites: outbound HTTPS to OpenSky, Rust toolchain on the box (or a prebuilt `aarch64-unknown-linux-gnu` binary), mapping sqlite, OurAirports `airports.csv`, OpenSky OAuth2 client credentials.

```bash
cargo build --release
# mapping + populated env + airports.csv staged for install.sh
sudo ADSB_MAPPING_SQLITE=/path/tail_to_ticker.sqlite \
     ADSB_AIRPORTS_CSV=/path/airports.csv \
     ADSB_ENV_FILE=/path/adsb-trip-journal.env \
     ./deploy/install.sh
```

Units in [`deploy/systemd/`](../deploy/systemd/):

| Unit | Schedule |
|---|---|
| `adsb-trip-journal-watch.service` | always on (`Restart=on-failure`) |
| `adsb-trip-journal-collect.timer` | daily 06:00 UTC (`Persistent=true`) |

Config: `/opt/adsb-trip-journal/etc/adsb-trip-journal.env` (from [`deploy/adsb-trip-journal.env.example`](../deploy/adsb-trip-journal.env.example), **chmod 600**). Install does not overwrite an existing env file.

First collect after a new install may skip if `seen_airborne` is empty. First *useful* collect is the 06:00 UTC run after watch has recorded the previous UTC day (a partial first UTC day is honest, not a full calendar day).

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

Upgrades: pull/rsync → `cargo build --release` → `sudo ./deploy/install.sh` (env and mapping are preserved unless `FORCE_MAPPING=1`).

## Verify

```bash
systemctl status adsb-trip-journal-watch.service --no-pager
systemctl list-timers 'adsb-trip-journal-*'
journalctl -u adsb-trip-journal-watch.service -u adsb-trip-journal-collect.service -n 50 --no-pager

# Does not need the 600 env file (paths are baked into the wrapper).
sudo -u adsb /opt/adsb-trip-journal/scripts/run-status.sh
```

Manual collect (after a UTC day of watch):

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

- Watch: ~**144** states-credits/day at 10 min
- Collect: **30 × hexes seen yesterday** (flights bucket)
- Tracks fallback only when both airport estimates are missing (tracks bucket)

## Limits

- OpenSky Standard 4k/day/bucket; historical flights calls are 30 credits even for a same-UTC-day window
- Empty watch list does not fall back to the full fleet
- 401/403 on `/states/all` exits non-zero so systemd restarts the watch unit

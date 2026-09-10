# ADS-B trip journal

A **separate** collector from [tail-to-ticker](https://github.com/alexwoolford/tail-to-ticker). That feed stays a mapping-only producer (FAA registrant → listed ticker). This sibling **reads** `mappings_current` and writes a restartable journal of trips (date, departure place, arrival place) for those tails.

v1 is **collection only**. No alerts, scores, geofences, or “uncharacteristic” classifier.

Join key is **`icao24`** (lowercase 24-bit hex). The N-number is copied onto trip rows for humans.

```
tail-to-ticker refresh
        │
        ▼
data/current/tail_to_ticker.sqlite   (READ ONLY)
  mappings_current
        │
        ▼
adsb-trip-journal  →  data/trips.sqlite + cache/flights_all/…
```

## Access reality (read this first)

**Production is OpenSky Standard REST.** Feeding an ADS-B Exchange receiver does not grant a traces API and is not this collector. Do not scrape `globe.adsbexchange.com`.

| Product | What you get | Fits this collector? |
|---|---|---|
| **OpenSky REST** | Estimated origin/dest + timestamps; thinner coverage, no MLAT, no Trino dump | Yes — Standard 4,000 credits/day **per bucket** |
| Feeder UUID / ADSBX community API | Map extras or live positions | No |

**Probe** is a one-shot credit measurement: `adsb-trip-journal probe --opensky --flights-all` (one 2h `GET /flights/all`). Do **not** call `/flights/aircraft` (30 credits per hex-day). 401/402/403/429 are not success. OpenSky REST accumulates a journal one UTC day at a time.

Access notes (this account):

```
historical /flights/aircraft = 30 flights-credits per call (unused by collect)
a 2h /flights/all slice is also 30 — a UTC day is 12 × 30 = 360
gold-set 2026-08-31 --hex CAT/COST/JPM/XOM/CVX: complete pairs XOM/CVX/CAT; COST+JPM 404
404 ≠ did not fly
```

## Build

```bash
cargo build --release
export TAIL_TO_TICKER_SQLITE="../tail-to-ticker/data/current/tail_to_ticker.sqlite"
./target/release/adsb-trip-journal probe --opensky --flights-all
./target/release/adsb-trip-journal airports-fetch
./target/release/adsb-trip-journal collect --from 2024-01-15 --to 2024-01-15
./target/release/adsb-trip-journal status
./target/release/adsb-trip-journal gc
./target/release/adsb-trip-journal invalidate --from 2026-09-03 --to 2026-09-05
```

Collect starts at **yesterday UTC** (or `--to`) and walks **newest-first**. `--from` / `--to` are one-shot bounds; omit `--from` to keep walking older UTC days until leftover flights credits cannot buy another never-started day (360). Watch `seen_airborne` does not change collect order.

## OpenSky collector

OpenSky Standard REST is **4,000 credits/day per independent bucket** (states / flights / tracks). Spending states credits does not buy flights credits. There is **no Trino** on this account — REST is the whole v1 path. Flights endpoints are a nightly batch: collect **yesterday UTC** (and earlier), not today.

**Observed billing (this account):** historical `GET /flights/aircraft` costs **30** flights-credits even for a same-UTC-day window. At 30/call, a busy fleet day cannot finish inside 4,000 (~133 hexes). Collect therefore uses **`GET /flights/all`** (max 2 hours, all aircraft seen in the interval), twelve slices covering yesterday, then leftover flights credits fill **newer-first** history (yesterday’s `pred`, then older). A never-started historical UTC day is not started unless remaining cap ≥ **360**; incomplete days still resume. There is no 90-day floor. Cost is per request, not per tail. A 2h historical slice billed **30** (2026-09-03 12:00–14:00 UTC probe: HTTP 200, ~2.3MB, 2.6s) so a day is **12 × 30 = 360**. `--max-flights-credits` default **800** (laptop; two UTC days). Production host env is **3600** (~10 days/run, ~400 slack). `install.sh` does not overwrite an existing env file. Fleet-filtered slices are cached under `cache/flights_all/YYYY-MM-DD/{00-11}.json.gz` so a retry does not re-spend credits. `--hex` applies only at ingest and does not shrink that cache or mark the day complete. A UTC day is complete only after 12/12 slices; a 429 leaves the rest for the next run. Successful `/tracks` lookups are cached beside the slice so a 429 resume does not re-burn the tracks bucket.

Watch still polls `/states/all` and writes `seen_airborne`. Collect is **not** gated on that list. Watch is optional (~2,880 states-credits/day). `install.sh` installs the unit but does **not** enable it; start it by hand for a “who is up” poll.

| Call | What you get | Cost (observed / assumed) |
|---|---|---|
| `GET /states/all?icao24=…` (hex filter, no huge bbox) | Who in the fleet is on the network **now** (watch; not a collect gate) | **4** per request. Watch chunks 80 hexes → ~20 credits/poll for ~331 hexes |
| `GET /flights/all` 2h historical slice | All flights seen in the window; we keep mapped hexes | **30** (measured 2026-09-03 12:00–14:00 UTC: 200, ~2.3MB, 2.6s; remaining dropped to match a 30-credit call after that day’s `/flights/aircraft` collect) |
| `GET /flights/aircraft` historical UTC day | Completed legs for one hex | **30** — unused; do not probe this |
| `GET /tracks/all?icao24=&time=` | Sparse waypoints when airport estimates cannot be placed | tracks bucket |

Gold-set collect (`--hex` CAT `a12c04`, COST `ab2ec9`, JPM `a7cb30`, XOM `a004b4`, CVX `a15de5` on 2026-08-31): complete pairs XOM LEBL→OTBD, CVX KSGR→KSNS, CAT KOAK→KBUR (COST/JPM 404). OpenSky often emits a second FlightObject a few seconds later with a null arrival ident; ingest now **collapses** those within 180s and keeps the complete ICAO pair. Mapping isolation holds; registrant for N175CT is Caterpillar Inc, matching ticker CAT. Missing day = not received, not “did not fly.” Coverage is thinner than ADS-B Exchange (fewer sensors, **no MLAT**).

Feed the **same** ADSBX Pi later (`readsb` Beast port 30005 → OpenSky feeder) if you want the 8,000-credit tier (still only ~266 historical flights-calls/day). Use the OpenSky **account email**, not the `…-api-client` OAuth client id. `/states/own` is free for *your* antenna only — not the mapped fleet.

```bash
# Credentials: put OPENSKY_CLIENT_ID + OPENSKY_CLIENT_SECRET in a gitignored `.env`
# (loaded from the cwd at startup), or export them. Alternatively:
# export OPENSKY_CREDENTIALS_JSON=./credentials.json
./target/release/adsb-trip-journal probe --opensky
./target/release/adsb-trip-journal probe --opensky --flights-all --date 2026-09-03
./target/release/adsb-trip-journal watch --interval-secs 600
./target/release/adsb-trip-journal collect
```

`watch` writes `seen_airborne` for today UTC (reloads the mapping fleet each poll). `collect` covers yesterday with 12× `/flights/all` and does not use the watch list as an allow-list. Pass `--hex` to ingest only those mapped hexes (the 12 API calls still run; cache stays the full fleet).

## Production (Linux)

Do **not** install these units on a Mac. Production is **git clone / rsync + [`deploy/install.sh`](deploy/install.sh) + systemd**, same `/opt` + `/var/lib` split as the other collectors on the host. Layout, timers, and credit budget: [`docs/DAILY_OPS.md`](docs/DAILY_OPS.md).

```bash
cargo build --release
sudo ADSB_AIRPORTS_CSV=/path/airports.csv \
     ADSB_ENV_FILE=/path/adsb-trip-journal.env \
     ./deploy/install.sh
```

Watch is long-running (`/states/all` every 10 min, ~20 states-credits/poll for a ~331-hex fleet ≈ 2,880/day of the 4,000 states bucket). It is **optional** and **not enabled** by `install.sh` — collect does not read `seen_airborne`. Collect is a daily timer at **06:00 UTC** (yesterday’s 12× `/flights/all`, then leftover credits fill **newer-first** history in whole UTC days; host cap **3600**, CLI **800**). Production mapping path is `/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite` (`TAIL_TO_TICKER_SQLITE`); the `adsb` user must be able to read it. Watch re-opens it every poll. Wrappers default to that path; do not fall back to `$STATE/mapping/`.

Out of this pass: OpenSky feeder (8k tier), analyst HTTP API, alerts/geofences.

## Fleet slice

Default input (same consumer filter as tail-to-ticker):

```sql
SELECT n_number, icao24, ticker, cik, company_name, make, model,
       registrant_name, match_method, aviation_issuer, fleet_size,
       as_of_date
FROM mappings_current
WHERE aviation_issuer = 0
  AND fleet_size BETWEEN 1 AND 6
  AND icao24 IS NOT NULL
  AND trim(icao24) <> '';
```

Empty `icao24` rows are skipped and counted. OEM / lessor fleets (`aviation_issuer = 1`) are out of the default slice. If a tail later drops out of the map, historical trips stay; fetching stops for that hex.

## Journal schema

SQLite at `$TRIP_JOURNAL_SQLITE` (default `data/trips.sqlite`): `fleet_snapshot`, `trips`, `seen_airborne` (watch diagnostic), `flights_all_slice`, `flights_all_day`. OpenSky resume is `flights_all` 12/12 (`flights_all_day.last_error` on halt). `trips.source` is `opensky_flights` or `opensky_track`.

Analyst query:

```sql
SELECT dep_ts, arr_ts, dep_airport, arr_airport, n_number, ticker, callsign
FROM trips
WHERE ticker = '…'
ORDER BY dep_ts;
```

`callsign` is the OpenSky transponder label (sparse; not identity). Airport-estimate quality integers (`dep_airport_horiz_m` and siblings) come from the same FlightObject. A FlightObject whose estimated dep and arr ident are the same is not stored. An ident farther than 8 km (`SNAP_RADIUS_KM`) is not stored as a landing; collect fetches `/tracks` (tracks bucket, 4 credits) and snaps the real endpoints. Each stored row is one hop (A→B and B→A are two rows). Complete UTC days are not re-GET on a binary upgrade. To backfill, `invalidate --from DATE [--to DATE]` (keep gzip cache by default — next collect replays FlightObjects with no flights credits). `--drop-cache` re-GETs (360 credits/day). `gc` / `gc --apply` deletes the unit-test fixture (`abcdef` / `N1`) and UTC days that stored neither callsign nor quality ints, without unlocking 12/12.

Production sqlite is `/var/lib/adsb-trip-journal/trips.sqlite`. Do not rsync a laptop `data/trips.sqlite` onto the host.

Ticker/cik on a trip row are a **snapshot at fetch time**. Re-running a day is idempotent (`PRIMARY KEY (icao24, dep_ts)`).

## Honesty

- **Do not scrape** ADS-B Exchange globe tiles or treat a feeder UUID as an API key.
- **Registrant ≠ owner ≠ operator.** A row means this N-number, then mapped to this ticker, was observed here. It is not proof the CEO flew there.
- **LADD / PIA:** missing days are missing, not “did not fly.”
- Keep this store separate from the FAA/SEC mapping feed.
- This journal is **not investment advice.**

## v2 (not this repo’s default path)

Competitor geofences, “first visit” flags, dashboards, OEM fleets, trust piercing (that work stays in tail-to-ticker).

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
adsb-trip-journal  →  data/trips.sqlite + cache/traces/…
```

## Access reality (read this first)

**Feeding an ADS-B Exchange receiver does not grant the REST API.** The feeder UUID identifies the station for stats/MLAT. It is not `api-auth`. Documented feeder perks are map layers, not historical traces.

| Product | What you get | Fits this collector? |
|---|---|---|
| Feeder UUID | Map extras, feeder stats | No programmatic traces |
| Community API (RapidAPI, ~$10/mo, 10k req) | Live positions, **non-commercial** | Forward-only if licensed |
| Enterprise traces / S3 | Per-hex daily `trace_full_{icao24}.json` | Preferred backfill |
| Daily Flight Events Blend | Origin→destination CSV | Ideal; not assumed |
| **OpenSky REST** | Estimated origin/dest + timestamps; thinner coverage, no MLAT, no Trino dump | Yes — Standard 4,000 credits/day **per bucket** |

ADSBX’s FAQ: a project for a commercial entity needs a commercial license even if you are not selling the output. Alternative-data / investment research is likely commercial. Do not scrape `globe.adsbexchange.com` as a substitute.

**Probe before backfill.** `adsb-trip-journal probe` records HTTP status for live hex → recent trace → one historical day. `adsb-trip-journal probe --opensky` records an OAuth token check, `/states/all`, `/flights/aircraft`, unix `begin`/`end`, and remaining-credit delta. `probe --opensky --flights-all` measures one 2h `GET /flights/all` (bytes, elapsed, remaining-credit delta, fleet overlap vs existing trips). A 404 (no legs that day) is success for the cursor. 401/402/403/429 are not. If ADSBX history is paywalled, v1 is **forward-only** on that source; OpenSky REST still accumulates a journal one UTC day at a time (no Trino dump).

Access probe (OpenSky, 2026-09-01, this tree):

```
token ok
historical /flights/aircraft = 30 flights-credits per call
  (full UTC day and a 6h 12:00–18:00 slice both spent 30 — “Live / < 24 h = 4” is not this path)
gold-set 2026-08-31 --hex CAT/COST/JPM/XOM/CVX:
  6 trip rows (3 complete ICAO pairs + 3 near-duplicates missing arr_airport)
  COST + JPM 404; WMT probe earlier the same day also 404
  404 ≠ did not fly
```

If you only have RapidAPI live: batch the fleet into one `/icao/{hex,hex,…}` per poll. Hundreds of per-hex requests will exhaust a 10k/month quota immediately.

## Build

```bash
cargo build --release
export TAIL_TO_TICKER_SQLITE="../tail-to-ticker/data/current/tail_to_ticker.sqlite"
# optional: ADSBX_API_KEY=…
./target/release/adsb-trip-journal probe
./target/release/adsb-trip-journal probe --opensky
./target/release/adsb-trip-journal airports-fetch
./target/release/adsb-trip-journal collect --from 2024-01-15 --to 2024-01-15
./target/release/adsb-trip-journal status
```

Cached traces under `cache/traces/YYYY-MM-DD/{icao24}.json.gz` are processed even without an API key (the test path, and a way to ingest files you already have a license to store).

`--from` defaults to the oldest incomplete `/flights/all` UTC day (slice resume, or a trip/watch day inside a 14-day lookback), else yesterday UTC. There is no unbounded multi-year loop until probe shows historical traces return 200.

Live fallback (only useful if live works and traces do not):

```bash
./target/release/adsb-trip-journal collect --live --poll-interval-secs 300
```

## OpenSky collector

OpenSky Standard REST is **4,000 credits/day per independent bucket** (states / flights / tracks). Spending states credits does not buy flights credits. There is **no Trino** on this account — REST is the whole v1 path. Flights endpoints are a nightly batch: collect **yesterday UTC** (and earlier), not today.

**Observed billing (this account):** historical `GET /flights/aircraft` costs **30** flights-credits even for a same-UTC-day window. At 30/call, a busy fleet day cannot finish inside 4,000 (~133 hexes). Collect therefore uses **`GET /flights/all`** (max 2 hours, all aircraft seen in the interval), twelve slices covering yesterday, then keeps mapped `icao24`s. Cost is per request, not per tail. A 2h historical slice billed **30** (2026-09-03 12:00–14:00 UTC probe: HTTP 200, ~2.3MB, 2.6s) so a day is **12 × 30 = 360**. `--max-flights-credits` default **500**. Filtered slices are cached under `cache/flights_all/YYYY-MM-DD/{00-11}.json.gz` so a retry does not re-spend credits. A UTC day is complete only after 12/12 slices; a 429 leaves the rest for the next run.

Watch still polls `/states/all` and writes `seen_airborne`. Collect is **not** gated on that list.

| Call | What you get | Cost (observed / assumed) |
|---|---|---|
| `GET /states/all?icao24=…` (hex filter, no huge bbox) | Who in the fleet is on the network **now** (watch; not a collect gate) | **4** per request. Watch chunks 80 hexes → ~20 credits/poll for ~331 hexes |
| `GET /flights/all` 2h historical slice | All flights seen in the window; we keep mapped hexes | **30** (measured 2026-09-03 12:00–14:00 UTC: 200, ~2.3MB, 2.6s; remaining dropped to match a 30-credit call after that day’s `/flights/aircraft` collect) |
| `GET /flights/aircraft` historical UTC day | Completed legs for one hex | **30** (not used by daily collect) |
| `GET /tracks/all?icao24=&time=` | Sparse waypoints when airport estimates cannot be placed | tracks bucket |

Gold-set collect (`--hex` CAT `a12c04`, COST `ab2ec9`, JPM `a7cb30`, XOM `a004b4`, CVX `a15de5` on 2026-08-31): complete pairs XOM LEBL→OTBD, CVX KSGR→KSNS, CAT KOAK→KBUR (COST/JPM 404). OpenSky often emits a second FlightObject a few seconds later with a null arrival ident; ingest now **collapses** those within 180s and keeps the complete ICAO pair. Mapping isolation holds; registrant for N175CT is Caterpillar Inc, matching ticker CAT. Missing day = not received, not “did not fly.” Coverage is thinner than ADS-B Exchange (fewer sensors, **no MLAT**).

Feed the **same** ADSBX Pi later (`readsb` Beast port 30005 → OpenSky feeder) if you want the 8,000-credit tier (still only ~266 historical flights-calls/day). Use the OpenSky **account email**, not the `…-api-client` OAuth client id. `/states/own` is free for *your* antenna only — not the mapped fleet.

```bash
# Credentials: put OPENSKY_CLIENT_ID + OPENSKY_CLIENT_SECRET in a gitignored `.env`
# (loaded from the cwd at startup), or export them. Alternatively:
# export OPENSKY_CREDENTIALS_JSON=./credentials.json
./target/release/adsb-trip-journal probe --opensky
./target/release/adsb-trip-journal probe --opensky --flights-all --date 2026-09-03
./target/release/adsb-trip-journal watch --source opensky --interval-secs 600
./target/release/adsb-trip-journal collect --source opensky
```

`watch` writes `seen_airborne` for today UTC (reloads the mapping fleet each poll). `collect --source opensky` covers yesterday with 12× `/flights/all` and does not use the watch list as an allow-list. Pass `--hex` to ingest only those mapped hexes (the 12 API calls still run).

## Production (Linux)

Do **not** install these units on a Mac. Production is **git clone / rsync + [`deploy/install.sh`](deploy/install.sh) + systemd**, same `/opt` + `/var/lib` split as the other collectors on the host. Layout, timers, and credit budget: [`docs/DAILY_OPS.md`](docs/DAILY_OPS.md).

```bash
cargo build --release
sudo ADSB_AIRPORTS_CSV=/path/airports.csv \
     ADSB_ENV_FILE=/path/adsb-trip-journal.env \
     ./deploy/install.sh
```

Watch is long-running (`/states/all` every 10 min, ~20 states-credits/poll for a ~331-hex fleet ≈ 2,880/day of the 4,000 states bucket). Collect is a daily timer at **06:00 UTC** (12× `/flights/all` for yesterday, ~360 flights-credits if slices bill 30). Production mapping path is `/var/lib/tail-to-ticker/current/tail_to_ticker.sqlite` (`TAIL_TO_TICKER_SQLITE`); the `adsb` user must be able to read it. Watch re-opens it every poll. Wrappers default to that path; do not fall back to `$STATE/mapping/`.

Out of this pass: OpenSky feeder (8k tier), analyst HTTP API, alerts/geofences.

## Fleet slice

Default input (same consumer filter as tail-to-ticker):

```sql
SELECT n_number, icao24, ticker, cik, company_name, make, model,
       registrant_name, match_method, aviation_issuer, fleet_size
FROM mappings_current
WHERE aviation_issuer = 0
  AND fleet_size BETWEEN 1 AND 6
  AND icao24 IS NOT NULL
  AND trim(icao24) <> '';
```

Empty `icao24` rows are skipped and counted. OEM / lessor fleets (`aviation_issuer = 1`) are out of the default slice. If a tail later drops out of the map, historical trips stay; fetching stops for that hex.

## Journal schema

SQLite at `$TRIP_JOURNAL_SQLITE` (default `data/trips.sqlite`): `fleet_snapshot`, `trips`, `fetch_cursor`, `seen_airborne`, `flights_all_slice`, `flights_all_day`. See [`context/adsb-trip-journal-spec.md`](context/adsb-trip-journal-spec.md).

Analyst query:

```sql
SELECT dep_ts, arr_ts, dep_airport, arr_airport, n_number, ticker
FROM trips
WHERE ticker = '…'
ORDER BY dep_ts;
```

Ticker/cik on a trip row are a **snapshot at fetch time**. Re-running a day is idempotent (`PRIMARY KEY (icao24, dep_ts)`).

## Honesty

- **ADS-B Exchange ToS** apply. Community API is typically personal / non-commercial.
- **Registrant ≠ owner ≠ operator.** A row means this N-number, then mapped to this ticker, was observed here. It is not proof the CEO flew there.
- **LADD / PIA:** missing days are missing, not “did not fly.”
- **Do not redistribute** raw ADSBX traces if the license forbids it. Keep this store separate from the FAA/SEC mapping feed.
- This journal is **not investment advice.**

## v2 (not this repo’s default path)

Competitor geofences, “first visit” flags, dashboards, OEM fleets, trust piercing (that work stays in tail-to-ticker).

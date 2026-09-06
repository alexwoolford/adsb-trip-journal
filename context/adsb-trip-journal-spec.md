# ADS-B trip journal (sibling collector)

Local specification for a **separate** project. This file lives under `context/` so it is not part of the tail-to-ticker git tree.

**tail-to-ticker** stays a mapping-only producer: public FAA registrant → listed ticker. **This sibling** consumes that map and builds a journal of trips (dates, origin, destination) for those tails. It does not generate mappings, pierce trusts, or write ADS-B rows back into the mapping SQLite.

```
tail-to-ticker refresh
        │
        ▼
data/current/tail_to_ticker.sqlite   (READ ONLY)
  mappings_current (n_number, icao24, ticker, …)
        │
        ▼
sibling: fleet query  →  OpenSky GET /flights/all (12× 2h)  →  trip journal
        │
        ▼
sibling data/trips.sqlite + cache/flights_all/YYYY-MM-DD/{00-11}.json.gz
```

Join key is **`icao24`** (lowercase 24-bit hex from FAA MASTER), not the N-number. ADS-B radios broadcast the hex; the N-number is a registry label copied onto trip rows for humans.

---

## 1. Purpose and non-goals

### Purpose (v1)

Collect **as much trip history as the API actually allows** for tails that already have a published mapping to a listed ticker. Most important fields: **date**, **departure place**, **arrival place**. Output is a restartable journal an analyst can query later (for example: this company’s jet started flying to a competitor’s airport, which it had not done in the observed window).

v1 is **collection only**. No alerts, no scores, no “uncharacteristic” classifier.

### Non-goals

- Do not rename, expand, or depend-cycle tail-to-ticker. Mapping must not import ADS-B credentials, rate limits, or ToS.
- Do not `INSERT`/`UPDATE` `mappings_current`, `mappings_history`, changelog, or review_queue.
- Do not pierce bank trusts or resolve beneficial owners. If the map says registrant → ticker, that is the join; ADS-B cannot fix a wrong parent.
- Do not store position-by-position history in the mapping database.
- Do not scrape FlightAware / FlightRadar24 as a substitute in v1.
- Do not implement competitor geofences, baseline models, or notifications in v1 (see §10).

---

## 2. Architecture

Two processes, two databases, one env pointer.

| Role | Path | Access |
|---|---|---|
| Mapping feed (producer) | `$TAIL_TO_TICKER_SQLITE` default `…/tail-to-ticker/data/current/tail_to_ticker.sqlite` | **read-only** |
| Trip journal (this sibling) | `$TRIP_JOURNAL_SQLITE` default `data/trips.sqlite` | read/write |
| OpenSky slice cache | `cache/flights_all/YYYY-MM-DD/{00-11}.json.gz` | write; fleet-filtered `/flights/all` body (not `--hex` filtered) |

Daily loop (conceptual):

1. Open mapping SQLite read-only. Run the fleet query (§3). Skip rows with empty `icao24`.
2. Upsert `fleet_snapshot` (copy of keys + ticker/cik as of this run).
3. Cover yesterday UTC with twelve `GET /flights/all` 2-hour slices, then leftover flights credits fill never-started or incomplete days in a **90-day** repair window (oldest first). Filter to the mapped fleet, persist the **full fleet** slice JSON, then ingest (`--hex` only restricts ingest). Incomplete `flights_all_slice` rows older than the window still resume. Watch `seen_airborne` is not a gate and does not pull `--from` backward.
4. When neither airport ident places, optionally `GET /tracks` and cache the attempt beside the slice. Missing arrival stays missing (do not copy dep lat/lon onto arr).
5. Mark a UTC day complete only after 12/12 unfiltered ingest. Never open the mapping DB for write.

If parquet is easier than SQLite for the mapping side, the same columns exist on `data/current/tail_to_ticker.parquet`. Prefer SQLite so the filter SQL in §3 is the contract.

---

## 3. Input contract (from tail-to-ticker)

`mappings_current` columns used here:

| Column | Use |
|---|---|
| `n_number` | Human tail; copied onto trips |
| `icao24` | ADS-B query key; skip if null/empty |
| `ticker` | Snapshot onto each trip |
| `cik` | Snapshot onto each trip |
| `company_name` | Snapshot / display |
| `make`, `model` | Display / filters |
| `aviation_issuer` | Default fleet slice |
| `fleet_size` | Default fleet slice |
| `registrant_name`, `match_method` | Provenance only; do not treat as operator |

Default alt-data fleet (flight departments, small published fleets):

```sql
SELECT n_number, icao24, ticker, cik, company_name, make, model,
       registrant_name, match_method, aviation_issuer, fleet_size,
       as_of_date
FROM mappings_current
WHERE aviation_issuer = 0
  AND fleet_size BETWEEN 1 AND 6
```

Empty `icao24` (null or blank after trim) is skipped in code and counted. `icao24` is stored lowercase. `snapshot_as_of` is `MAX(as_of_date)` on the slice.

OEM / defense / lessor fleets (`aviation_issuer = 1`) are **out of the default slice**. A later flag can include them; they are not “company jet to competitor HQ” in the same sense.

If a tail drops out of `mappings_current` on a later mapping refresh, keep historical trips; stop fetching new days for that hex unless it returns. Do not delete trips when the map shrinks.

---

## 4. Output schema

Sibling SQLite. Names are normative for v1.

### `fleet_snapshot`

One row per hex currently in the fleet query. Replaced (or upserted) each run.

```sql
CREATE TABLE fleet_snapshot (
  icao24 TEXT PRIMARY KEY,
  n_number TEXT NOT NULL,
  ticker TEXT NOT NULL,
  cik TEXT,
  company_name TEXT,
  make TEXT,
  model TEXT,
  aviation_issuer INTEGER NOT NULL,
  fleet_size INTEGER NOT NULL,
  snapshot_as_of TEXT NOT NULL, -- map as_of_date, UTC date YYYY-MM-DD
  recorded_at TEXT NOT NULL     -- write time YYYY-MM-DDTHH:MM:SSZ; same on every row of one replace
);
```

### `trips`

One row per **completed leg**. Identity is hex + departure timestamp (stable if the same trace is reprocessed).

```sql
CREATE TABLE trips (
  icao24 TEXT NOT NULL,
  dep_ts TEXT NOT NULL,          -- UTC instant YYYY-MM-DDTHH:MM:SSZ
  arr_ts TEXT,                   -- same; null if still open at end of day (rare)
  n_number TEXT NOT NULL,
  ticker TEXT NOT NULL,
  cik TEXT,
  dep_lat REAL,
  dep_lon REAL,
  arr_lat REAL,
  arr_lon REAL,
  dep_airport TEXT,              -- ICAO ident if snapped; else null
  arr_airport TEXT,
  dep_place TEXT,                -- ident or "lat,lon" for display
  arr_place TEXT,
  source TEXT NOT NULL,          -- opensky_flights | opensky_track
  fetched_at TEXT NOT NULL,      -- UTC instant YYYY-MM-DDTHH:MM:SSZ
  callsign TEXT,                 -- OpenSky transponder label; sparse; not identity
  dep_airport_horiz_m INTEGER,   -- OpenSky airport-estimate quality (meters / counts)
  dep_airport_vert_m INTEGER,
  arr_airport_horiz_m INTEGER,
  arr_airport_vert_m INTEGER,
  dep_airport_candidates INTEGER,
  arr_airport_candidates INTEGER,
  PRIMARY KEY (icao24, dep_ts)
);
CREATE INDEX idx_trips_ticker_dep ON trips (ticker, dep_ts);
CREATE INDEX idx_trips_n_number ON trips (n_number);
```

Ticker/cik on the trip row are a **snapshot at fetch time**. If the mapping later moves the tail to another ticker, old trip rows stay as fetched. Do not rewrite history from a new map.

`callsign` is what the transponder transmitted (often an N-number, sometimes `DCM`/`FFL` or airline-style). It is **not** a join key and is often null. The six integer columns are OpenSky’s airport-estimate metadata (horizontal/vertical distance in meters and nearby-candidate counts). Existing complete `/flights/all` days stay null until a slice is fetched again.

### `fetch_cursor` (leftover)

The table may still exist on a journal that once ran ADSBX collect. OpenSky does not write it. Status does not print it. Do not wipe the host sqlite to drop it.

OpenSky daily collect is **not** per-hex `/flights/aircraft`. It runs twelve `GET /flights/all` 2-hour slices, filters to the mapped fleet, and marks the UTC day complete only after 12/12. Status leads with `flights_all` 12/12 (`flights_all_day.last_error` on halt). Do not bulk-advance per-hex `last_ok_date` when an OpenSky day completes.

### `flights_all_slice` / `flights_all_day`

```sql
CREATE TABLE flights_all_slice (
  utc_date TEXT NOT NULL,        -- YYYY-MM-DD
  slice_idx INTEGER NOT NULL,    -- 0..11
  completed_at TEXT NOT NULL,    -- UTC instant YYYY-MM-DDTHH:MM:SSZ
  PRIMARY KEY (utc_date, slice_idx)
);
CREATE TABLE flights_all_day (
  utc_date TEXT PRIMARY KEY,
  last_error TEXT,
  updated_at TEXT NOT NULL
);
```

---

## 5. Trip segmentation

Each `/flights/all` FlightObject is one candidate leg (`firstSeen` / `lastSeen`, estimated airport idents). Collapse near-duplicates within 180s and keep the most complete ICAO pair (prefer a non-null `callsign` after airport completeness). Place dep/arr from OurAirports idents; if neither ident places, optional `/tracks` first/last point. Missing arrival stays null — do not invent A→A by copying departure coordinates. Do not emit a trip with no departure coordinates. Short hops that never leave the same airport still count (same `dep_airport` and `arr_airport`).

---

## 6. Airport snap

After dep/arr lat/lon exist, snap to the nearest airport from a public dataset (OurAirports or equivalent: ident, lat, lon, type).

- Use a small radius (on the order of **5–8 km**). Prefer airports with type `large_airport` / `medium_airport` / `small_airport`; skip heliports unless no other hit (many corporate fields are small airports).
- If none within radius, leave `dep_airport` / `arr_airport` null and set `dep_place` / `arr_place` to rounded `"lat,lon"`.
- Do not invent IATA codes. ICAO ident is enough.

v1 does not reverse-geocode to city names. Place names can be joined later from the airport table.

---

## 7. Access, credentials, rate limits

**Production is OpenSky Standard REST** (OAuth2 client credentials). Feeder status does not automatically include multi-year historical archives. Confirm what the account can `GET` before writing a backfill loop.

| Product | Typical contents | v1 use |
|---|---|---|
| OpenSky `GET /flights/all` 2h | All flights in the window; we keep mapped hexes | **Nightly collect** (12 slices/UTC day; historical slice billed **30** on this account) |
| OpenSky `GET /tracks/all` | Sparse waypoints | Fallback when airport idents cannot be placed |
| OpenSky `GET /states/all` | Who is on the network now | Optional watch; not a collect gate |
| OpenSky `GET /flights/aircraft` | Completed legs for one hex | **Unused** (30/call, cannot cover a busy fleet). Do not probe it. |

**First implementer task:** `probe --opensky --flights-all` (one 2h slice). Record remaining-credit delta. OpenSky REST accumulates a journal one UTC day at a time (no Trino dump).

Credentials: environment only (`OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`, or `OPENSKY_CREDENTIALS_JSON`). Never commit keys. Never put them in tail-to-ticker.

Credit cap: CLI/laptop **800** flights-credits per collect run (two UTC days at 30/slice). Production host env **3600** (repair budget; ~10 days/run). Install does not overwrite an existing env file. Lookback is **90** UTC days of never-started or incomplete `/flights/all` days.

Rate limits: honor 429; do not mark remaining `/flights/all` slices complete. Persist slice JSON and tracks attempts so retries do not re-spend. Do not parallel-bomb the API.

Gzip: historical JSON is often gzip without a `.gz` name; clients must send `Accept-Encoding: gzip` and decode.

---

## 8. Legal and honesty

- **Do not scrape** ADS-B Exchange globe tiles or treat a feeder UUID as an API key. This collector uses OpenSky REST only.
- **Registrant ≠ owner ≠ operator.** A trip journal on a published mapping is “this N-number, currently mapped to this ticker, was observed here.” It is not proof the CEO flew there or that a deal is happening.
- **LADD / PIA:** some tails are blocked on FAA-fed sites. OpenSky coverage is receiver-limited and has no MLAT. Missing days are missing, not “did not fly.”
- The mapping feed’s FAA/SEC data is a different license stack; keep the stores separate.
- This journal is **not investment advice**.

---

## 9. v1 success

v1 is done when all of the following are true:

1. Sibling repo (or local tree) reads mapping SQLite **read-only** via `TAIL_TO_TICKER_SQLITE`.
2. Fleet query in §3 is the default input; empty `icao24` rows are skipped and counted.
3. For OpenSky collect, `flights_all_slice` has 12/12 rows for every UTC day in the attempted window (401/429 do not mark remaining slices). Status leads with that 12/12. Halt errors live on `flights_all_day.last_error`.
4. `trips` contains one row per segmented completed leg with `dep_ts`, coordinates, and airport snap when in radius.
5. Re-running a day is idempotent (`PRIMARY KEY (icao24, dep_ts)`).
6. Mapping SQLite file size and `mappings_current` row count are unchanged after a sibling run.
7. Access reality is written down: backfill years vs forward-only.

Analyst query the journal is meant to support (v1 data, v2 product):

```sql
SELECT dep_ts, arr_ts, dep_airport, arr_airport, n_number, ticker, callsign
FROM trips
WHERE ticker = '…'
ORDER BY dep_ts;
```

---

## 10. v2 (not this spec)

Out of scope until the journal exists and access is proven:

- Geofences around competitor HQs, plants, or known meeting airports.
- “First time this ticker’s tails visited airport X in N months.”
- Alerts / dashboards / live maps.
- Including `aviation_issuer = 1` OEM fleets.
- Merging overnight legs across midnight with extra confidence.
- Trust piercing so more tails enter the map (that work stays in tail-to-ticker v2, then this collector just sees a larger `mappings_current`).

Tracking consumes the feed. It does not generate it.

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
sibling: fleet query  →  ADS-B Exchange fetch  →  trip journal
        │
        ▼
sibling data/trips.sqlite + cache/traces/…
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
| Raw traces | `cache/traces/YYYY-MM-DD/{icao24}.json.gz` | write; optional parquet rollup later |

Daily loop (conceptual):

1. Open mapping SQLite read-only. Run the fleet query (§3). Skip rows with empty `icao24`.
2. Upsert `fleet_snapshot` (copy of keys + ticker/cik as of this run).
3. For each hex, consult `fetch_cursor`. Fetch missing days from ADS-B Exchange (§7).
4. Persist raw trace files. Segment into legs (§5). Snap airports (§6). Upsert `trips`. Advance cursor.
5. Never open the mapping DB for write.

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
       registrant_name, match_method, aviation_issuer, fleet_size
FROM mappings_current
WHERE aviation_issuer = 0
  AND fleet_size BETWEEN 1 AND 6
  AND icao24 IS NOT NULL
  AND trim(icao24) <> '';
```

`icao24` is stored lowercase. ADS-B Exchange paths use that hex; folder for traces is the **last two characters** of the hex.

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
  snapshot_as_of TEXT NOT NULL  -- ISO date of the mapping file used
);
```

### `trips`

One row per **completed leg**. Identity is hex + departure timestamp (stable if the same trace is reprocessed).

```sql
CREATE TABLE trips (
  icao24 TEXT NOT NULL,
  dep_ts TEXT NOT NULL,          -- UTC ISO-8601
  arr_ts TEXT,                   -- UTC; null if still open at end of day (rare)
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
  source TEXT NOT NULL,          -- adsbx_trace_hist | adsbx_trace_recent | adsbx_live
  fetched_at TEXT NOT NULL,
  PRIMARY KEY (icao24, dep_ts)
);
CREATE INDEX idx_trips_ticker_dep ON trips (ticker, dep_ts);
CREATE INDEX idx_trips_n_number ON trips (n_number);
```

Ticker/cik on the trip row are a **snapshot at fetch time**. If the mapping later moves the tail to another ticker, old trip rows stay as fetched. Do not rewrite history from a new map.

### `fetch_cursor`

Restartable backfill / increment.

```sql
CREATE TABLE fetch_cursor (
  icao24 TEXT PRIMARY KEY,
  last_ok_date TEXT,             -- last UTC date with a successful fetch (incl. 404 = no trace)
  last_error TEXT,
  updated_at TEXT NOT NULL
);
```

A successful **404** (no trace that day) still advances `last_ok_date`. HTTP 401/402/403/429 must **not** advance the cursor.

---

## 5. Trip segmentation

Prefer ADS-B Exchange **per-aircraft daily traces** over polling live positions.

Trace file (conceptual): `timestamp` (unix seconds, start of day) plus `trace[]` rows:

`[seconds_after_timestamp, lat, lon, altitude_ft_or_"ground"|null, gs, track, flags, …]`

**`flags & 2`**: start of a new leg (their split between landing and subsequent takeoff). That bit is the primary trip boundary.

Additional heuristics if flags are missing or noisy:

- Altitude `"ground"` or near-zero AGL with gs below a taxi threshold: on ground.
- A new airborne segment after a ground dwell longer than ~15 minutes: new trip.
- Ignore isolated points and stale flags (`flags & 1`) when picking dep/arr coordinates.

For each leg:

- **Departure**: first airborne (or first point after a new-leg flag) with a valid lat/lon; `dep_ts = timestamp + seconds`.
- **Arrival**: last point of that leg before the next new-leg flag or end of file; if the aircraft is still airborne at UTC midnight, leave `arr_ts` null and merge with the next day’s first leg when the same hex continues.

Do not emit a trip with no coordinates. Short hops that never leave the same airport still count (same `dep_airport` and `arr_airport`).

Live-poll fallback (if traces are unavailable): treat “was on ground / missing, then airborne, then on ground” as a trip. Quality will be worse; set `source = adsbx_live`.

---

## 6. Airport snap

After dep/arr lat/lon exist, snap to the nearest airport from a public dataset (OurAirports or equivalent: ident, lat, lon, type).

- Use a small radius (on the order of **5–8 km**). Prefer airports with type `large_airport` / `medium_airport` / `small_airport`; skip heliports unless no other hit (many corporate fields are small airports).
- If none within radius, leave `dep_airport` / `arr_airport` null and set `dep_place` / `arr_place` to rounded `"lat,lon"`.
- Do not invent IATA codes. ICAO ident is enough.

v1 does not reverse-geocode to city names. Place names can be joined later from the airport table.

---

## 7. Access, credentials, rate limits

**Feeder status does not automatically include multi-year historical archives.** Confirm what the account can `GET` before writing a backfill loop.

| Product | Typical contents | v1 use |
|---|---|---|
| Community / RapidAPI live | Current positions by hex | Forward collection from “now” if history is closed |
| Recent trace (`trace_recent_{icao}.json` / `trace_full_{icao}.json`) | Short history for one hex | Incremental if licensed |
| Historical trace | `…/traces-hist/{yyyy}/{mm}/{dd}/traces/{lastTwoHex}/trace_full_{icao24}.json` | **Preferred backfill** |
| S3 / “pull data” daily archives | Same traces, bulk | Preferred if cheaper than per-file HTTP |
| Daily Flight Events Blend | Origin→destination CSV | Ideal if licensed; not assumed |

**First implementer task:** with the real key, probe in order: live hex → recent trace → one historical day for a known mapped hex (e.g. a Walmart tail’s `icao24`) → S3 listing if documented. Record 200 vs 402/403. If history is paywalled, v1 is **forward-only** plus an explicit backfill gap in the sibling README.

Credentials: environment only (`ADSBX_API_KEY`, RapidAPI headers, or S3 keys as issued). Never commit keys. Never put them in tail-to-ticker.

Rate limits: one hex-day per request for traces. A few hundred tails × years of days is a large loop. Serialize, honor `Retry-After` / 429, persist cursor after each successful day. Do not parallel-bomb the API.

Gzip: historical JSON is often gzip without a `.gz` name; clients must send `Accept-Encoding: gzip` and decode.

---

## 8. Legal and honesty

- **ADS-B Exchange ToS** apply. Community API is typically personal / non-commercial. Alternative-data use for investment research is likely **commercial**; do not assume a feeder UUID is a commercial license. Pay or get written permission before treating this as a product.
- **Registrant ≠ owner ≠ operator.** A trip journal on a published mapping is “this N-number, currently mapped to this ticker, was observed here.” It is not proof the CEO flew there or that a deal is happening.
- **LADD / PIA:** some tails are blocked on FAA-fed sites. ADS-B Exchange is independent of those lists, but coverage is still receiver-limited. Missing days are missing, not “did not fly.”
- **Do not redistribute** raw ADSBX traces if the license forbids it. The mapping feed’s FAA/SEC data is a different license stack; keep the stores separate.
- This journal is **not investment advice**.

---

## 9. v1 success

v1 is done when all of the following are true:

1. Sibling repo (or local tree) reads mapping SQLite **read-only** via `TAIL_TO_TICKER_SQLITE`.
2. Fleet query in §3 is the default input; empty `icao24` rows are skipped and counted.
3. For every hex in that fleet, `fetch_cursor.last_ok_date` covers every UTC day in the attempted window (including documented 404s).
4. `trips` contains one row per segmented completed leg with `dep_ts`, coordinates, and airport snap when in radius.
5. Re-running a day is idempotent (`PRIMARY KEY (icao24, dep_ts)`).
6. Mapping SQLite file size and `mappings_current` row count are unchanged after a sibling run.
7. Access reality is written down: backfill years vs forward-only.

Analyst query the journal is meant to support (v1 data, v2 product):

```sql
SELECT dep_ts, arr_ts, dep_airport, arr_airport, n_number, ticker
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

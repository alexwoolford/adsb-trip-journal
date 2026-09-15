# Capture contract (work sqlite)

Decision: **capture the trip trickle, not the OpenSky hose.**
Work sqlite: `/var/lib/adsb-trip-journal/trips.sqlite`. Logical name: `adsb-trip-journal`.
Pin: capturable-state git tag v0.1.1.

## What is captured

| Table | Mode | Why |
|---|---|---|
| `trips` | full | Canonical hops `(icao24, dep_ts)` |
| `seen_airborne` | after | Watch diagnostic (who was up that UTC day) |
| `flights_all_slice` | after | Completeness lock per 2h slice |
| `flights_all_day` | after | Day error / 12/12 telemetry |

Uncaptured: `fleet_snapshot` (DELETE+reload each collect/watch), leftover `fetch_cursor`, gzip `cache/flights_all/` (hose). Do not `collect --snapshot` this database.

## Identity

Grain: `trips` primary key `(icao24, dep_ts)`. Upsert `ON CONFLICT DO UPDATE`, not REPLACE. Soft-delete: no — `gc` hard-deletes fixture/payloadless rows; mosaic follows `_outbox` `D`.

## Clocks

Facts TEXT `YYYY-MM-DD` / `YYYY-MM-DDTHH:MM:SSZ` via `utc_iso`. Envelope `_outbox.ts` INTEGER Unix seconds. Order by `seq`.

## Announce / nudge

`install()` on the work sqlite. `nudge.send()` after commit. `ReadWritePaths` include `/var/lib/state-capture/announce` (required) and `-/run/state`.
Collector host inventory lives in mosaic `deploy/ct-firehose/`, not this crate.

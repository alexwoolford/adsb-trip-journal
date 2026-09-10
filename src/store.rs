//! Sibling SQLite: `fleet_snapshot`, `trips`, `flights_all_*`, leftover `fetch_cursor`.

use std::path::Path;

use anyhow::{Context, Result};
use capturable_state::{
    apply_runtime_pragmas, install, table_is_strict, CaptureConfig, CaptureMode, Nudge, TableSpec,
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension};

use crate::fleet::FleetRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TripSource {
    OpenskyFlights,
    OpenskyTrack,
}

impl TripSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenskyFlights => "opensky_flights",
            Self::OpenskyTrack => "opensky_track",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TripRow {
    pub icao24: String,
    pub dep_ts: String,
    pub arr_ts: Option<String>,
    pub n_number: String,
    pub ticker: String,
    pub cik: Option<String>,
    pub dep_lat: Option<f64>,
    pub dep_lon: Option<f64>,
    pub arr_lat: Option<f64>,
    pub arr_lon: Option<f64>,
    pub dep_airport: Option<String>,
    pub arr_airport: Option<String>,
    pub dep_place: Option<String>,
    pub arr_place: Option<String>,
    pub source: String,
    pub fetched_at: String,
    /// OpenSky transponder label. Sparse; not identity; not a join key.
    pub callsign: Option<String>,
    pub dep_airport_horiz_m: Option<i64>,
    pub dep_airport_vert_m: Option<i64>,
    pub arr_airport_horiz_m: Option<i64>,
    pub arr_airport_vert_m: Option<i64>,
    pub dep_airport_candidates: Option<i64>,
    pub arr_airport_candidates: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct FetchCursor {
    pub icao24: String,
    pub last_ok_date: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct JournalStatus {
    pub path: String,
    pub fleet_hexes: i64,
    pub snapshot_as_of: Option<String>,
    pub recorded_at: Option<String>,
    pub trips: i64,
    pub flights_all_complete_max: Option<String>,
    pub flights_all_incomplete: Vec<(String, i64)>,
    pub flights_all_errors: Vec<(String, String)>,
}

/// Unit-test aircraft (`store` tests / isolation fixtures). Valid Mode S hex;
/// not banned at upsert. Production collect never maps it.
pub const TEST_FIXTURE_ICAO24: &str = "abcdef";
pub const TEST_FIXTURE_N_NUMBER: &str = "N1";

#[derive(Debug, Clone, Default)]
pub struct GcReport {
    pub dry_run: bool,
    pub fixture_trips: u64,
    pub payloadless_days: Vec<String>,
    pub payloadless_trips: u64,
    pub trips_deleted: u64,
}

#[derive(Debug, Clone, Default)]
pub struct InvalidateReport {
    pub from: String,
    pub to: String,
    pub slices_deleted: u64,
    pub day_rows_deleted: u64,
    pub cache_files_removed: u64,
    pub drop_cache: bool,
}

pub struct JournalDb {
    conn: Connection,
    nudge: Nudge,
}

impl JournalDb {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("open journal sqlite {}", path.display()))?;
        apply_runtime_pragmas(&conn)?;
        conn.execute_batch(JOURNAL_DDL)?;
        ensure_column(&conn, "trips", "callsign", "TEXT")?;
        ensure_column(&conn, "trips", "dep_airport_horiz_m", "INTEGER")?;
        ensure_column(&conn, "trips", "dep_airport_vert_m", "INTEGER")?;
        ensure_column(&conn, "trips", "arr_airport_horiz_m", "INTEGER")?;
        ensure_column(&conn, "trips", "arr_airport_vert_m", "INTEGER")?;
        ensure_column(&conn, "trips", "dep_airport_candidates", "INTEGER")?;
        ensure_column(&conn, "trips", "arr_airport_candidates", "INTEGER")?;
        migrate_strict(&conn)?;
        conn.execute_batch(JOURNAL_DDL)?;
        let nudge = install_capture(&conn, path)?;
        Ok(Self { conn, nudge })
    }

    pub fn replace_fleet_snapshot(
        &mut self,
        rows: &[FleetRow],
        snapshot_as_of: &str,
    ) -> Result<()> {
        let recorded_at = utc_iso(Utc::now());
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM fleet_snapshot", [])?;
        {
            let mut stmt = tx.prepare(
                r#"
                INSERT INTO fleet_snapshot (
                  icao24, n_number, ticker, cik, company_name, make, model,
                  aviation_issuer, fleet_size, snapshot_as_of, recorded_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                "#,
            )?;
            for r in rows {
                stmt.execute(params![
                    r.icao24,
                    r.n_number,
                    r.ticker,
                    r.cik,
                    r.company_name,
                    r.make,
                    r.model,
                    r.aviation_issuer,
                    r.fleet_size,
                    snapshot_as_of,
                    recorded_at,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn upsert_trip(&self, row: &TripRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO trips (
              icao24, dep_ts, arr_ts, n_number, ticker, cik,
              dep_lat, dep_lon, arr_lat, arr_lon,
              dep_airport, arr_airport, dep_place, arr_place,
              source, fetched_at,
              callsign,
              dep_airport_horiz_m, dep_airport_vert_m,
              arr_airport_horiz_m, arr_airport_vert_m,
              dep_airport_candidates, arr_airport_candidates
            ) VALUES (
              ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
              ?17, ?18, ?19, ?20, ?21, ?22, ?23
            )
            ON CONFLICT(icao24, dep_ts) DO UPDATE SET
              arr_ts = excluded.arr_ts,
              n_number = excluded.n_number,
              ticker = excluded.ticker,
              cik = excluded.cik,
              dep_lat = excluded.dep_lat,
              dep_lon = excluded.dep_lon,
              arr_lat = excluded.arr_lat,
              arr_lon = excluded.arr_lon,
              dep_airport = excluded.dep_airport,
              arr_airport = excluded.arr_airport,
              dep_place = excluded.dep_place,
              arr_place = excluded.arr_place,
              source = excluded.source,
              fetched_at = excluded.fetched_at,
              callsign = excluded.callsign,
              dep_airport_horiz_m = excluded.dep_airport_horiz_m,
              dep_airport_vert_m = excluded.dep_airport_vert_m,
              arr_airport_horiz_m = excluded.arr_airport_horiz_m,
              arr_airport_vert_m = excluded.arr_airport_vert_m,
              dep_airport_candidates = excluded.dep_airport_candidates,
              arr_airport_candidates = excluded.arr_airport_candidates
            "#,
            params![
                row.icao24,
                row.dep_ts,
                row.arr_ts,
                row.n_number,
                row.ticker,
                row.cik,
                row.dep_lat,
                row.dep_lon,
                row.arr_lat,
                row.arr_lon,
                row.dep_airport,
                row.arr_airport,
                row.dep_place,
                row.arr_place,
                row.source,
                row.fetched_at,
                row.callsign,
                row.dep_airport_horiz_m,
                row.dep_airport_vert_m,
                row.arr_airport_horiz_m,
                row.arr_airport_vert_m,
                row.dep_airport_candidates,
                row.arr_airport_candidates,
            ],
        )?;
        self.nudge.send();
        Ok(())
    }

    pub fn get_cursor(&self, icao24: &str) -> Result<Option<FetchCursor>> {
        self.conn
            .query_row(
                "SELECT icao24, last_ok_date, last_error, updated_at FROM fetch_cursor WHERE icao24 = ?1",
                [icao24],
                |row| {
                    Ok(FetchCursor {
                        icao24: row.get(0)?,
                        last_ok_date: row.get(1)?,
                        last_error: row.get(2)?,
                        updated_at: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn last_ok_date(&self, icao24: &str) -> Result<Option<NaiveDate>> {
        Ok(self
            .get_cursor(icao24)?
            .and_then(|c| c.last_ok_date)
            .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()))
    }

    /// Successful fetch including HTTP 404 (no trace that day). Advances `last_ok_date`.
    pub fn advance_cursor_ok(&self, icao24: &str, date: NaiveDate) -> Result<()> {
        let now = utc_iso(Utc::now());
        self.conn.execute(
            r#"
            INSERT INTO fetch_cursor (icao24, last_ok_date, last_error, updated_at)
            VALUES (?1, ?2, NULL, ?3)
            ON CONFLICT(icao24) DO UPDATE SET
              last_ok_date = excluded.last_ok_date,
              last_error = NULL,
              updated_at = excluded.updated_at
            "#,
            params![icao24, date.to_string(), now],
        )?;
        Ok(())
    }

    /// 401/402/403/429 (or transport errors): do **not** advance `last_ok_date`.
    pub fn set_cursor_error(&self, icao24: &str, err: &str) -> Result<()> {
        let now = utc_iso(Utc::now());
        self.conn.execute(
            r#"
            INSERT INTO fetch_cursor (icao24, last_ok_date, last_error, updated_at)
            VALUES (?1, NULL, ?2, ?3)
            ON CONFLICT(icao24) DO UPDATE SET
              last_error = excluded.last_error,
              updated_at = excluded.updated_at
            "#,
            params![icao24, err, now],
        )?;
        Ok(())
    }

    pub fn trip_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM trips", [], |r| r.get(0))?)
    }

    /// Clear `/flights/all` completeness for `from..=to` (inclusive UTC dates).
    /// Trips and gzip caches are left alone; the next collect walk-back replays
    /// cache (or re-GETs if the operator also deleted the gzips).
    pub fn invalidate_flights_all_range(
        &self,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<(u64, u64)> {
        let _ = utc_dates_inclusive(from, to)?;
        let slices = self.conn.execute(
            "DELETE FROM flights_all_slice WHERE utc_date >= ?1 AND utc_date <= ?2",
            params![from.to_string(), to.to_string()],
        )? as u64;
        let days = self.conn.execute(
            "DELETE FROM flights_all_day WHERE utc_date >= ?1 AND utc_date <= ?2",
            params![from.to_string(), to.to_string()],
        )? as u64;
        self.nudge.send();
        Ok((slices, days))
    }

    pub fn count_fixture_trips(&self) -> Result<u64> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM trips WHERE icao24 = ?1 AND n_number = ?2",
            params![TEST_FIXTURE_ICAO24, TEST_FIXTURE_N_NUMBER],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    /// UTC calendar days of `dep_ts` where no trip stored a callsign and no trip
    /// stored `dep_airport_horiz_m` (the FlightObject quality ints added in the
    /// same upsert as callsign).
    pub fn list_payloadless_days(&self) -> Result<Vec<NaiveDate>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT substr(dep_ts, 1, 10) AS utc_date
            FROM trips
            GROUP BY 1
            HAVING MAX(CASE WHEN callsign IS NOT NULL AND trim(callsign) <> '' THEN 1 ELSE 0 END) = 0
               AND MAX(CASE WHEN dep_airport_horiz_m IS NOT NULL THEN 1 ELSE 0 END) = 0
            ORDER BY 1
            "#,
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for date in rows {
            let s = date?;
            let d = NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .with_context(|| format!("payloadless utc_date {s}"))?;
            out.push(d);
        }
        Ok(out)
    }

    pub fn count_trips_on_day(&self, date: NaiveDate) -> Result<u64> {
        let (start, end) = utc_day_bounds(date);
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM trips WHERE dep_ts >= ?1 AND dep_ts < ?2",
            params![start, end],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    pub fn preview_gc(&self) -> Result<GcReport> {
        let days = self.list_payloadless_days()?;
        let mut payloadless_trips = 0u64;
        let mut payloadless_days = Vec::new();
        for d in &days {
            payloadless_trips += self.count_trips_on_day(*d)?;
            payloadless_days.push(d.to_string());
        }
        Ok(GcReport {
            dry_run: true,
            fixture_trips: self.count_fixture_trips()?,
            payloadless_days,
            payloadless_trips,
            trips_deleted: 0,
        })
    }

    /// Hard-delete the unit-test fixture and trips on payloadless UTC days.
    /// Does not touch `flights_all_slice` (those days stay 12/12).
    pub fn apply_gc(&self) -> Result<GcReport> {
        let mut report = self.preview_gc()?;
        report.dry_run = false;
        let mut deleted = self.conn.execute(
            "DELETE FROM trips WHERE icao24 = ?1 AND n_number = ?2",
            params![TEST_FIXTURE_ICAO24, TEST_FIXTURE_N_NUMBER],
        )? as u64;
        for s in &report.payloadless_days {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
            let (start, end) = utc_day_bounds(d);
            deleted += self.conn.execute(
                "DELETE FROM trips WHERE dep_ts >= ?1 AND dep_ts < ?2",
                params![start, end],
            )? as u64;
        }
        report.trips_deleted = deleted;
        self.nudge.send();
        Ok(report)
    }

    pub fn get_trip(&self, icao24: &str, dep_ts: &str) -> Result<Option<TripRow>> {
        self.conn
            .query_row(
                r#"
                SELECT icao24, dep_ts, arr_ts, n_number, ticker, cik,
                       dep_lat, dep_lon, arr_lat, arr_lon,
                       dep_airport, arr_airport, dep_place, arr_place,
                       source, fetched_at, callsign,
                       dep_airport_horiz_m, dep_airport_vert_m,
                       arr_airport_horiz_m, arr_airport_vert_m,
                       dep_airport_candidates, arr_airport_candidates
                FROM trips WHERE icao24 = ?1 AND dep_ts = ?2
                "#,
                params![icao24, dep_ts],
                trip_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn in_fleet(&self, icao24: &str) -> Result<bool> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM fleet_snapshot WHERE icao24 = ?1",
            [icao24],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    pub fn mark_seen_airborne(&self, icao24: &str, date: NaiveDate) -> Result<()> {
        let now = utc_iso(Utc::now());
        let hex = icao24.trim().to_ascii_lowercase();
        self.conn.execute(
            r#"
            INSERT INTO seen_airborne (icao24, utc_date, first_seen_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(icao24, utc_date) DO NOTHING
            "#,
            params![hex, date.to_string(), now],
        )?;
        self.nudge.send();
        Ok(())
    }

    pub fn seen_airborne_on(&self, date: NaiveDate) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT icao24 FROM seen_airborne WHERE utc_date = ?1 ORDER BY icao24")?;
        let rows = stmt.query_map([date.to_string()], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn flights_all_slice_complete(&self, date: NaiveDate, slice_idx: u32) -> Result<bool> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM flights_all_slice WHERE utc_date = ?1 AND slice_idx = ?2",
            params![date.to_string(), slice_idx],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    pub fn flights_all_slices_done(&self, date: NaiveDate) -> Result<u32> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM flights_all_slice WHERE utc_date = ?1",
            [date.to_string()],
            |r| r.get(0),
        )?;
        Ok(n as u32)
    }

    pub fn flights_all_day_complete(&self, date: NaiveDate, n_slices: u32) -> Result<bool> {
        Ok(self.flights_all_slices_done(date)? >= n_slices)
    }

    pub fn mark_flights_all_slice_ok(&self, date: NaiveDate, slice_idx: u32) -> Result<()> {
        let now = utc_iso(Utc::now());
        self.conn.execute(
            r#"
            INSERT INTO flights_all_slice (utc_date, slice_idx, completed_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(utc_date, slice_idx) DO UPDATE SET
              completed_at = excluded.completed_at
            "#,
            params![date.to_string(), slice_idx, now],
        )?;
        self.conn.execute(
            r#"
            INSERT INTO flights_all_day (utc_date, last_error, updated_at)
            VALUES (?1, NULL, ?2)
            ON CONFLICT(utc_date) DO UPDATE SET
              last_error = NULL,
              updated_at = excluded.updated_at
            "#,
            params![date.to_string(), now],
        )?;
        self.nudge.send();
        Ok(())
    }

    pub fn set_flights_all_error(&self, date: NaiveDate, err: &str) -> Result<()> {
        let now = utc_iso(Utc::now());
        self.conn.execute(
            r#"
            INSERT INTO flights_all_day (utc_date, last_error, updated_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(utc_date) DO UPDATE SET
              last_error = excluded.last_error,
              updated_at = excluded.updated_at
            "#,
            params![date.to_string(), err, now],
        )?;
        self.nudge.send();
        Ok(())
    }

    pub fn has_trips_on(&self, date: NaiveDate) -> Result<bool> {
        Ok(self.count_trips_on_day(date)? > 0)
    }

    /// `(icao24, firstSeen unix)` for trips whose `dep_ts` falls in `[begin, end]` inclusive.
    pub fn trip_keys_between(&self, begin_unix: i64, end_unix: i64) -> Result<Vec<(String, i64)>> {
        let Some(start) = unix_to_iso(begin_unix) else {
            return Ok(Vec::new());
        };
        let Some(end) = unix_to_iso(end_unix) else {
            return Ok(Vec::new());
        };
        let mut stmt = self
            .conn
            .prepare("SELECT icao24, dep_ts FROM trips WHERE dep_ts >= ?1 AND dep_ts <= ?2")?;
        let rows = stmt.query_map(params![start, end], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (hex, dep_ts) = row?;
            if let Some(ts) = iso_to_unix(&dep_ts) {
                out.push((hex, ts));
            }
        }
        Ok(out)
    }

    pub fn status(&self, path: &Path) -> Result<JournalStatus> {
        let fleet_hexes: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM fleet_snapshot", [], |r| r.get(0))?;
        let snapshot_as_of: Option<String> = self
            .conn
            .query_row(
                "SELECT snapshot_as_of FROM fleet_snapshot LIMIT 1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let recorded_at: Option<String> = self
            .conn
            .query_row("SELECT recorded_at FROM fleet_snapshot LIMIT 1", [], |r| {
                r.get(0)
            })
            .optional()?;
        let trips = self.trip_count()?;
        let flights_all_complete_max: Option<String> = self
            .conn
            .query_row(
                r#"
                SELECT utc_date FROM flights_all_slice
                GROUP BY utc_date
                HAVING COUNT(*) >= 12
                ORDER BY utc_date DESC
                LIMIT 1
                "#,
                [],
                |r| r.get(0),
            )
            .optional()?;
        let mut inc_stmt = self.conn.prepare(
            r#"
            SELECT utc_date, COUNT(*) FROM flights_all_slice
            GROUP BY utc_date
            HAVING COUNT(*) < 12
            ORDER BY utc_date
            "#,
        )?;
        let flights_all_incomplete = inc_stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut err_stmt = self.conn.prepare(
            r#"
            SELECT utc_date, last_error FROM flights_all_day
            WHERE last_error IS NOT NULL AND last_error <> ''
            ORDER BY utc_date
            "#,
        )?;
        let flights_all_errors = err_stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(JournalStatus {
            path: path.display().to_string(),
            fleet_hexes,
            snapshot_as_of,
            recorded_at,
            trips,
            flights_all_complete_max,
            flights_all_incomplete,
            flights_all_errors,
        })
    }
}

const JOURNAL_DDL: &str = r#"
            CREATE TABLE IF NOT EXISTS fleet_snapshot (
              icao24 TEXT PRIMARY KEY,
              n_number TEXT NOT NULL,
              ticker TEXT NOT NULL,
              cik TEXT,
              company_name TEXT,
              make TEXT,
              model TEXT,
              aviation_issuer INTEGER NOT NULL,
              fleet_size INTEGER NOT NULL,
              snapshot_as_of TEXT NOT NULL,
              recorded_at TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS trips (
              icao24 TEXT NOT NULL,
              dep_ts TEXT NOT NULL,
              arr_ts TEXT,
              n_number TEXT NOT NULL,
              ticker TEXT NOT NULL,
              cik TEXT,
              dep_lat REAL,
              dep_lon REAL,
              arr_lat REAL,
              arr_lon REAL,
              dep_airport TEXT,
              arr_airport TEXT,
              dep_place TEXT,
              arr_place TEXT,
              source TEXT NOT NULL,
              fetched_at TEXT NOT NULL,
              callsign TEXT,
              dep_airport_horiz_m INTEGER,
              dep_airport_vert_m INTEGER,
              arr_airport_horiz_m INTEGER,
              arr_airport_vert_m INTEGER,
              dep_airport_candidates INTEGER,
              arr_airport_candidates INTEGER,
              PRIMARY KEY (icao24, dep_ts)
            ) STRICT;
            CREATE INDEX IF NOT EXISTS idx_trips_ticker_dep ON trips (ticker, dep_ts);
            CREATE INDEX IF NOT EXISTS idx_trips_n_number ON trips (n_number);
            CREATE TABLE IF NOT EXISTS fetch_cursor (
              icao24 TEXT PRIMARY KEY,
              last_ok_date TEXT,
              last_error TEXT,
              updated_at TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS seen_airborne (
              icao24 TEXT NOT NULL,
              utc_date TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              PRIMARY KEY (icao24, utc_date)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS flights_all_slice (
              utc_date TEXT NOT NULL,
              slice_idx INTEGER NOT NULL,
              completed_at TEXT NOT NULL,
              PRIMARY KEY (utc_date, slice_idx)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS flights_all_day (
              utc_date TEXT PRIMARY KEY,
              last_error TEXT,
              updated_at TEXT NOT NULL
            ) STRICT;
            "#;

const DB_NAME: &str = "adsb-trip-journal";

fn install_capture(conn: &Connection, path: &Path) -> Result<Nudge> {
    let tables = [
        TableSpec::new("trips", CaptureMode::Full),
        TableSpec::new("seen_airborne", CaptureMode::After),
        TableSpec::new("flights_all_slice", CaptureMode::After),
        TableSpec::new("flights_all_day", CaptureMode::After),
    ];
    install(conn, &CaptureConfig::new(DB_NAME, path, &tables))
}

fn migrate_strict(conn: &Connection) -> Result<()> {
    if table_is_strict(conn, "trips")? {
        return Ok(());
    }
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    conn.execute_batch(
        r#"
        CREATE TABLE fleet_snapshot_strict (
          icao24 TEXT PRIMARY KEY,
          n_number TEXT NOT NULL,
          ticker TEXT NOT NULL,
          cik TEXT,
          company_name TEXT,
          make TEXT,
          model TEXT,
          aviation_issuer INTEGER NOT NULL,
          fleet_size INTEGER NOT NULL,
          snapshot_as_of TEXT NOT NULL,
          recorded_at TEXT NOT NULL
        ) STRICT;
        INSERT INTO fleet_snapshot_strict SELECT * FROM fleet_snapshot;
        DROP TABLE fleet_snapshot;
        ALTER TABLE fleet_snapshot_strict RENAME TO fleet_snapshot;

        CREATE TABLE trips_strict (
          icao24 TEXT NOT NULL,
          dep_ts TEXT NOT NULL,
          arr_ts TEXT,
          n_number TEXT NOT NULL,
          ticker TEXT NOT NULL,
          cik TEXT,
          dep_lat REAL,
          dep_lon REAL,
          arr_lat REAL,
          arr_lon REAL,
          dep_airport TEXT,
          arr_airport TEXT,
          dep_place TEXT,
          arr_place TEXT,
          source TEXT NOT NULL,
          fetched_at TEXT NOT NULL,
          callsign TEXT,
          dep_airport_horiz_m INTEGER,
          dep_airport_vert_m INTEGER,
          arr_airport_horiz_m INTEGER,
          arr_airport_vert_m INTEGER,
          dep_airport_candidates INTEGER,
          arr_airport_candidates INTEGER,
          PRIMARY KEY (icao24, dep_ts)
        ) STRICT;
        INSERT INTO trips_strict SELECT * FROM trips;
        DROP TABLE trips;
        ALTER TABLE trips_strict RENAME TO trips;

        CREATE TABLE fetch_cursor_strict (
          icao24 TEXT PRIMARY KEY,
          last_ok_date TEXT,
          last_error TEXT,
          updated_at TEXT NOT NULL
        ) STRICT;
        INSERT INTO fetch_cursor_strict SELECT * FROM fetch_cursor;
        DROP TABLE fetch_cursor;
        ALTER TABLE fetch_cursor_strict RENAME TO fetch_cursor;

        CREATE TABLE seen_airborne_strict (
          icao24 TEXT NOT NULL,
          utc_date TEXT NOT NULL,
          first_seen_at TEXT NOT NULL,
          PRIMARY KEY (icao24, utc_date)
        ) STRICT;
        INSERT INTO seen_airborne_strict SELECT * FROM seen_airborne;
        DROP TABLE seen_airborne;
        ALTER TABLE seen_airborne_strict RENAME TO seen_airborne;

        CREATE TABLE flights_all_slice_strict (
          utc_date TEXT NOT NULL,
          slice_idx INTEGER NOT NULL,
          completed_at TEXT NOT NULL,
          PRIMARY KEY (utc_date, slice_idx)
        ) STRICT;
        INSERT INTO flights_all_slice_strict SELECT * FROM flights_all_slice;
        DROP TABLE flights_all_slice;
        ALTER TABLE flights_all_slice_strict RENAME TO flights_all_slice;

        CREATE TABLE flights_all_day_strict (
          utc_date TEXT PRIMARY KEY,
          last_error TEXT,
          updated_at TEXT NOT NULL
        ) STRICT;
        INSERT INTO flights_all_day_strict SELECT * FROM flights_all_day;
        DROP TABLE flights_all_day;
        ALTER TABLE flights_all_day_strict RENAME TO flights_all_day;
        "#,
    )?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn trip_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TripRow> {
    Ok(TripRow {
        icao24: row.get(0)?,
        dep_ts: row.get(1)?,
        arr_ts: row.get(2)?,
        n_number: row.get(3)?,
        ticker: row.get(4)?,
        cik: row.get(5)?,
        dep_lat: row.get(6)?,
        dep_lon: row.get(7)?,
        arr_lat: row.get(8)?,
        arr_lon: row.get(9)?,
        dep_airport: row.get(10)?,
        arr_airport: row.get(11)?,
        dep_place: row.get(12)?,
        arr_place: row.get(13)?,
        source: row.get(14)?,
        fetched_at: row.get(15)?,
        callsign: row.get(16)?,
        dep_airport_horiz_m: row.get(17)?,
        dep_airport_vert_m: row.get(18)?,
        arr_airport_horiz_m: row.get(19)?,
        arr_airport_vert_m: row.get(20)?,
        dep_airport_candidates: row.get(21)?,
        arr_airport_candidates: row.get(22)?,
    })
}

fn ensure_column(conn: &Connection, table: &str, column: &str, decl: &str) -> Result<()> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .with_context(|| format!("pragma table_info {table}"))?;
    let exists = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .any(|name| name.as_deref() == Ok(column));
    if !exists {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"),
            [],
        )
        .with_context(|| format!("add {table}.{column}"))?;
    }
    Ok(())
}

pub fn utc_iso(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

pub fn utc_dates_inclusive(from: NaiveDate, to: NaiveDate) -> Result<Vec<NaiveDate>> {
    if to < from {
        anyhow::bail!("--to {to} is before --from {from}");
    }
    let mut out = Vec::new();
    let mut d = from;
    loop {
        out.push(d);
        if d == to {
            break;
        }
        d = d
            .succ_opt()
            .ok_or_else(|| anyhow::anyhow!("date overflow after {d}"))?;
    }
    Ok(out)
}

fn utc_day_bounds(date: NaiveDate) -> (String, String) {
    let start = format!("{date}T00:00:00Z");
    let end = date
        .succ_opt()
        .map(|n| format!("{n}T00:00:00Z"))
        .unwrap_or_else(|| format!("{date}T23:59:59Z"));
    (start, end)
}

fn unix_to_iso(ts: i64) -> Option<String> {
    Utc.timestamp_opt(ts, 0).single().map(utc_iso)
}

fn iso_to_unix(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp())
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                .ok()
                .map(|ndt| ndt.and_utc().timestamp())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::FleetRow;
    use chrono::TimeZone;
    use tempfile::tempdir;

    /// Instant TEXT: `YYYY-MM-DDTHH:MM:SSZ` (not offset RFC3339, no fraction).
    fn assert_utc_instant(s: &str) {
        assert!(
            s.len() == 20
                && s.as_bytes()[4] == b'-'
                && s.as_bytes()[7] == b'-'
                && s.as_bytes()[10] == b'T'
                && s.as_bytes()[13] == b':'
                && s.as_bytes()[16] == b':'
                && s.ends_with('Z')
                && s.as_bytes()[..4].iter().all(u8::is_ascii_digit)
                && s.as_bytes()[5..7].iter().all(u8::is_ascii_digit)
                && s.as_bytes()[8..10].iter().all(u8::is_ascii_digit)
                && s.as_bytes()[11..13].iter().all(u8::is_ascii_digit)
                && s.as_bytes()[14..16].iter().all(u8::is_ascii_digit)
                && s.as_bytes()[17..19].iter().all(u8::is_ascii_digit),
            "expected YYYY-MM-DDTHH:MM:SSZ, got {s:?}"
        );
        assert!(!s.contains("+00:00"), "{s}");
        assert!(!s.contains('.'), "{s}");
    }

    #[test]
    fn utc_iso_is_zulu_second_resolution() {
        let dt = Utc.with_ymd_and_hms(2026, 9, 1, 21, 19, 58).unwrap();
        let s = utc_iso(dt);
        assert_eq!(s, "2026-09-01T21:19:58Z");
        assert_utc_instant(&s);
        let rfc = dt.to_rfc3339();
        assert!(rfc.contains("+00:00") || rfc.contains('.'));
        assert_ne!(s, rfc);
    }

    fn row() -> FleetRow {
        FleetRow {
            n_number: "N1".into(),
            icao24: "abcdef".into(),
            ticker: "AAA".into(),
            cik: None,
            company_name: None,
            make: None,
            model: None,
            registrant_name: None,
            match_method: None,
            aviation_issuer: 0,
            fleet_size: 1,
            as_of_date: Some("2026-08-31".into()),
        }
    }

    #[test]
    fn cursor_404_advances_auth_does_not() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        db.advance_cursor_ok("abcdef", d).unwrap();
        assert_eq!(db.last_ok_date("abcdef").unwrap(), Some(d));
        db.set_cursor_error("abcdef", "HTTP 403").unwrap();
        assert_eq!(
            db.last_ok_date("abcdef").unwrap(),
            Some(d),
            "auth failure must not rewind or skip last_ok_date"
        );
        let c = db.get_cursor("abcdef").unwrap().unwrap();
        assert_eq!(c.last_error.as_deref(), Some("HTTP 403"));
        assert_eq!(c.last_ok_date.as_deref(), Some("2024-01-15"));
        assert_utc_instant(&c.updated_at);
    }

    #[test]
    fn status_reports_flights_all_errors_not_leftover_cursors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let db = JournalDb::open(&path).unwrap();
        let d = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        db.set_cursor_error("ac7f36", "max-flights-credits cap")
            .unwrap();
        db.set_flights_all_error(d, "tracks HTTP 429 remaining=Some(0)")
            .unwrap();
        let status = db.status(&path).unwrap();
        assert_eq!(
            status.flights_all_errors,
            vec![(
                "2026-09-05".into(),
                "tracks HTTP 429 remaining=Some(0)".into()
            )]
        );
        assert!(db.get_cursor("ac7f36").unwrap().is_some());
    }

    #[test]
    fn trip_upsert_is_idempotent() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let t = TripRow {
            icao24: "abcdef".into(),
            dep_ts: "2024-01-15T12:00:00Z".into(),
            arr_ts: Some("2024-01-15T13:00:00Z".into()),
            n_number: "N1".into(),
            ticker: "AAA".into(),
            cik: None,
            dep_lat: Some(1.0),
            dep_lon: Some(2.0),
            arr_lat: Some(3.0),
            arr_lon: Some(4.0),
            dep_airport: Some("KAPA".into()),
            arr_airport: Some("KJFK".into()),
            dep_place: Some("KAPA".into()),
            arr_place: Some("KJFK".into()),
            source: TripSource::OpenskyFlights.as_str().into(),
            fetched_at: "2026-08-31T00:00:00Z".into(),
            callsign: None,
            dep_airport_horiz_m: None,
            dep_airport_vert_m: None,
            arr_airport_horiz_m: None,
            arr_airport_vert_m: None,
            dep_airport_candidates: None,
            arr_airport_candidates: None,
        };
        db.upsert_trip(&t).unwrap();
        db.upsert_trip(&t).unwrap();
        assert_eq!(db.trip_count().unwrap(), 1);
        let trips: Vec<(String, String)> = outbox_ops(&db)
            .into_iter()
            .filter(|(tbl, _)| tbl == "trips")
            .collect();
        assert_eq!(
            trips,
            vec![("trips".into(), "I".into()), ("trips".into(), "U".into())]
        );
    }

    fn outbox_ops(db: &JournalDb) -> Vec<(String, String)> {
        let mut stmt = db
            .conn
            .prepare("SELECT tbl, op FROM _outbox ORDER BY seq")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn fleet_snapshot_replace_writes_no_outbox_rows() {
        let dir = tempdir().unwrap();
        let mut db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        db.replace_fleet_snapshot(&[row()], "2026-08-31").unwrap();
        let n: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM _outbox WHERE tbl = 'fleet_snapshot'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 0);
        assert!(outbox_ops(&db).is_empty());
    }

    #[test]
    fn fleet_snapshot_replaced_not_appended() {
        let dir = tempdir().unwrap();
        let mut db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let a = row();
        let mut b = row();
        b.icao24 = "bbbbbb".into();
        db.replace_fleet_snapshot(&[a.clone(), b], "2026-08-31")
            .unwrap();
        let first: Vec<(String, String, String)> = {
            let mut stmt = db
                .conn
                .prepare(
                    "SELECT icao24, snapshot_as_of, recorded_at FROM fleet_snapshot ORDER BY icao24",
                )
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(first.len(), 2);
        assert!(first.iter().all(|(_, as_of, _)| as_of == "2026-08-31"));
        let recorded = &first[0].2;
        assert_utc_instant(recorded);
        assert!(first.iter().all(|(_, _, r)| r == recorded));

        let mut gone = row();
        gone.icao24 = "ffffff".into();
        db.replace_fleet_snapshot(&[gone], "2026-08-31").unwrap();
        assert!(!db.in_fleet("abcdef").unwrap());
        assert!(db.in_fleet("ffffff").unwrap());
        let (as_of, rec2): (String, String) = db
            .conn
            .query_row(
                "SELECT snapshot_as_of, recorded_at FROM fleet_snapshot",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(as_of, "2026-08-31");
        assert_utc_instant(&rec2);
        let n: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(DISTINCT recorded_at) FROM fleet_snapshot",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn seen_airborne_is_idempotent_per_day() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        db.mark_seen_airborne("abcdef", d).unwrap();
        db.mark_seen_airborne("abcdef", d).unwrap();
        db.mark_seen_airborne("ffffff", d).unwrap();
        let seen = db.seen_airborne_on(d).unwrap();
        assert_eq!(seen, vec!["abcdef".to_string(), "ffffff".to_string()]);
        assert!(db
            .seen_airborne_on(NaiveDate::from_ymd_opt(2024, 1, 16).unwrap())
            .unwrap()
            .is_empty());
        let (utc_date, first_seen_at): (String, String) = db
            .conn
            .query_row(
                "SELECT utc_date, first_seen_at FROM seen_airborne WHERE icao24 = 'abcdef'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(utc_date, "2024-01-15");
        assert_utc_instant(&first_seen_at);
    }

    #[test]
    fn flights_all_slice_resume_until_12() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let d = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();
        db.mark_flights_all_slice_ok(d, 0).unwrap();
        db.mark_flights_all_slice_ok(d, 1).unwrap();
        assert_eq!(db.flights_all_slices_done(d).unwrap(), 2);
        assert!(!db.flights_all_day_complete(d, 12).unwrap());
        for i in 2..12 {
            db.mark_flights_all_slice_ok(d, i).unwrap();
        }
        assert!(db.flights_all_day_complete(d, 12).unwrap());
    }

    #[test]
    fn flights_all_429_does_not_mark_day_complete() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let d = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();
        for i in 0..11 {
            db.mark_flights_all_slice_ok(d, i).unwrap();
        }
        assert!(!db.flights_all_day_complete(d, 12).unwrap());
        db.set_flights_all_error(d, "flights/all HTTP 429").unwrap();
        assert!(!db.flights_all_day_complete(d, 12).unwrap());
    }

    #[test]
    fn seen_airborne_does_not_mark_flights_all_complete() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let d = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();
        db.mark_seen_airborne("abcdef", d).unwrap();
        assert!(!db.flights_all_day_complete(d, 12).unwrap());
        assert_eq!(db.flights_all_slices_done(d).unwrap(), 0);
    }

    #[test]
    fn trip_keys_between_uses_utc_iso() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let t = TripRow {
            icao24: "abcdef".into(),
            dep_ts: "2026-09-03T12:30:00Z".into(),
            arr_ts: None,
            n_number: "N1".into(),
            ticker: "AAA".into(),
            cik: None,
            dep_lat: Some(1.0),
            dep_lon: Some(2.0),
            arr_lat: Some(3.0),
            arr_lon: Some(4.0),
            dep_airport: Some("KAPA".into()),
            arr_airport: Some("KJFK".into()),
            dep_place: Some("KAPA".into()),
            arr_place: Some("KJFK".into()),
            source: TripSource::OpenskyFlights.as_str().into(),
            fetched_at: "2026-09-04T06:00:00Z".into(),
            callsign: None,
            dep_airport_horiz_m: None,
            dep_airport_vert_m: None,
            arr_airport_horiz_m: None,
            arr_airport_vert_m: None,
            dep_airport_candidates: None,
            arr_airport_candidates: None,
        };
        db.upsert_trip(&t).unwrap();
        let begin = iso_to_unix("2026-09-03T12:00:00Z").unwrap();
        let end = iso_to_unix("2026-09-03T13:59:59Z").unwrap();
        let keys = db.trip_keys_between(begin, end).unwrap();
        assert_eq!(keys, vec![("abcdef".into(), begin + 30 * 60)]);
        assert!(db
            .has_trips_on(NaiveDate::from_ymd_opt(2026, 9, 3).unwrap())
            .unwrap());
        assert!(!db
            .has_trips_on(NaiveDate::from_ymd_opt(2026, 9, 4).unwrap())
            .unwrap());
    }

    #[test]
    fn ensure_column_adds_callsign_on_old_trips_table() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("old.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE trips (
                  icao24 TEXT NOT NULL,
                  dep_ts TEXT NOT NULL,
                  arr_ts TEXT,
                  n_number TEXT NOT NULL,
                  ticker TEXT NOT NULL,
                  cik TEXT,
                  dep_lat REAL,
                  dep_lon REAL,
                  arr_lat REAL,
                  arr_lon REAL,
                  dep_airport TEXT,
                  arr_airport TEXT,
                  dep_place TEXT,
                  arr_place TEXT,
                  source TEXT NOT NULL,
                  fetched_at TEXT NOT NULL,
                  PRIMARY KEY (icao24, dep_ts)
                );
                "#,
            )
            .unwrap();
        }
        let db = JournalDb::open(&path).unwrap();
        let t = TripRow {
            icao24: "abcdef".into(),
            dep_ts: "2024-01-15T12:00:00Z".into(),
            arr_ts: None,
            n_number: "N1".into(),
            ticker: "AAA".into(),
            cik: None,
            dep_lat: Some(1.0),
            dep_lon: Some(2.0),
            arr_lat: Some(3.0),
            arr_lon: Some(4.0),
            dep_airport: Some("KAPA".into()),
            arr_airport: Some("KJFK".into()),
            dep_place: Some("KAPA".into()),
            arr_place: Some("KJFK".into()),
            source: TripSource::OpenskyFlights.as_str().into(),
            fetched_at: "2026-09-04T06:00:00Z".into(),
            callsign: Some("DCM1".into()),
            dep_airport_horiz_m: Some(10),
            dep_airport_vert_m: Some(20),
            arr_airport_horiz_m: Some(30),
            arr_airport_vert_m: Some(40),
            dep_airport_candidates: Some(1),
            arr_airport_candidates: Some(2),
        };
        db.upsert_trip(&t).unwrap();
        let got = db
            .get_trip("abcdef", "2024-01-15T12:00:00Z")
            .unwrap()
            .unwrap();
        assert_eq!(got.callsign.as_deref(), Some("DCM1"));
        assert_eq!(got.dep_airport_horiz_m, Some(10));
        assert_eq!(got.arr_airport_candidates, Some(2));
    }

    #[test]
    fn opens_with_wal() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let mode: String = db
            .conn
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "wal");
    }

    fn trip_on(
        icao24: &str,
        n_number: &str,
        dep_ts: &str,
        callsign: Option<&str>,
        horiz: Option<i64>,
    ) -> TripRow {
        TripRow {
            icao24: icao24.into(),
            dep_ts: dep_ts.into(),
            arr_ts: None,
            n_number: n_number.into(),
            ticker: "AAA".into(),
            cik: None,
            dep_lat: Some(1.0),
            dep_lon: Some(2.0),
            arr_lat: None,
            arr_lon: None,
            dep_airport: Some("KAPA".into()),
            arr_airport: None,
            dep_place: Some("KAPA".into()),
            arr_place: None,
            source: TripSource::OpenskyFlights.as_str().into(),
            fetched_at: "2026-09-05T06:00:00Z".into(),
            callsign: callsign.map(str::to_string),
            dep_airport_horiz_m: horiz,
            dep_airport_vert_m: None,
            arr_airport_horiz_m: None,
            arr_airport_vert_m: None,
            dep_airport_candidates: None,
            arr_airport_candidates: None,
        }
    }

    #[test]
    fn invalidate_clears_slice_lock_not_trips() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        let d = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();
        for i in 0..12 {
            db.mark_flights_all_slice_ok(d, i).unwrap();
        }
        assert!(db.flights_all_day_complete(d, 12).unwrap());
        db.upsert_trip(&trip_on(
            "a12c04",
            "N175CT",
            "2026-09-03T12:00:00Z",
            None,
            None,
        ))
        .unwrap();
        let (slices, days) = db.invalidate_flights_all_range(d, d).unwrap();
        assert_eq!(slices, 12);
        assert_eq!(days, 1);
        assert!(!db.flights_all_day_complete(d, 12).unwrap());
        assert_eq!(db.trip_count().unwrap(), 1);
        let ops = outbox_ops(&db);
        assert!(ops
            .iter()
            .any(|(tbl, op)| tbl == "flights_all_slice" && op == "D"));
    }

    #[test]
    fn utc_dates_inclusive_rejects_inverted_range() {
        let from = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 9, 3).unwrap();
        assert!(utc_dates_inclusive(from, to).is_err());
        assert_eq!(utc_dates_inclusive(to, from).unwrap().len(), 3);
    }

    #[test]
    fn gc_drops_payloadless_and_fixture_keeps_payload_days() {
        let dir = tempdir().unwrap();
        let db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        db.upsert_trip(&trip_on(
            TEST_FIXTURE_ICAO24,
            TEST_FIXTURE_N_NUMBER,
            "2024-01-15T12:00:00Z",
            None,
            None,
        ))
        .unwrap();
        db.upsert_trip(&trip_on(
            "a12c04",
            "N175CT",
            "2026-09-03T08:00:00Z",
            None,
            None,
        ))
        .unwrap();
        db.upsert_trip(&trip_on(
            "a66d27",
            "N513FX",
            "2026-06-08T22:08:39Z",
            Some("LXJ513"),
            Some(8457),
        ))
        .unwrap();
        db.upsert_trip(&trip_on(
            "adc694",
            "N987QS",
            "2026-09-07T10:00:00Z",
            None,
            Some(100),
        ))
        .unwrap();
        let preview = db.preview_gc().unwrap();
        assert!(preview.dry_run);
        assert_eq!(preview.fixture_trips, 1);
        assert_eq!(preview.payloadless_days, vec!["2024-01-15", "2026-09-03"]);
        assert_eq!(preview.trips_deleted, 0);
        assert_eq!(db.trip_count().unwrap(), 4);

        let applied = db.apply_gc().unwrap();
        assert!(!applied.dry_run);
        assert_eq!(applied.trips_deleted, 2);
        assert_eq!(db.trip_count().unwrap(), 2);
        assert!(db
            .get_trip("a66d27", "2026-06-08T22:08:39Z")
            .unwrap()
            .is_some());
        assert!(db
            .get_trip("adc694", "2026-09-07T10:00:00Z")
            .unwrap()
            .is_some());
        assert!(db
            .get_trip(TEST_FIXTURE_ICAO24, "2024-01-15T12:00:00Z")
            .unwrap()
            .is_none());
        assert!(outbox_ops(&db)
            .iter()
            .any(|(tbl, op)| tbl == "trips" && op == "D"));
    }
}

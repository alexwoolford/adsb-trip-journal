//! Sibling SQLite: `fleet_snapshot`, `trips`, `fetch_cursor`.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{params, Connection, OptionalExtension};

use crate::fleet::FleetRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TripSource {
    AdsBxTraceHist,
    AdsBxTraceRecent,
    AdsBxLive,
    OpenskyFlights,
    OpenskyTrack,
}

impl TripSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AdsBxTraceHist => "adsbx_trace_hist",
            Self::AdsBxTraceRecent => "adsbx_trace_recent",
            Self::AdsBxLive => "adsbx_live",
            Self::OpenskyFlights => "opensky_flights",
            Self::OpenskyTrack => "opensky_track",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "adsbx_trace_hist" => Some(Self::AdsBxTraceHist),
            "adsbx_trace_recent" => Some(Self::AdsBxTraceRecent),
            "adsbx_live" => Some(Self::AdsBxLive),
            "opensky_flights" => Some(Self::OpenskyFlights),
            "opensky_track" => Some(Self::OpenskyTrack),
            _ => None,
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
    pub trips: i64,
    pub cursors: i64,
    pub cursors_with_error: i64,
    pub last_ok_min: Option<String>,
    pub last_ok_max: Option<String>,
    pub errors: Vec<(String, String)>,
}

pub struct JournalDb {
    conn: Connection,
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
        conn.busy_timeout(Duration::from_millis(5_000))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(
            r#"
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
              snapshot_as_of TEXT NOT NULL
            );
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
              PRIMARY KEY (icao24, dep_ts)
            );
            CREATE INDEX IF NOT EXISTS idx_trips_ticker_dep ON trips (ticker, dep_ts);
            CREATE INDEX IF NOT EXISTS idx_trips_n_number ON trips (n_number);
            CREATE TABLE IF NOT EXISTS fetch_cursor (
              icao24 TEXT PRIMARY KEY,
              last_ok_date TEXT,
              last_error TEXT,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS seen_airborne (
              icao24 TEXT NOT NULL,
              utc_date TEXT NOT NULL,
              first_seen_at TEXT NOT NULL,
              PRIMARY KEY (icao24, utc_date)
            );
            "#,
        )?;
        Ok(Self { conn })
    }

    pub fn replace_fleet_snapshot(
        &mut self,
        rows: &[FleetRow],
        snapshot_as_of: &str,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM fleet_snapshot", [])?;
        {
            let mut stmt = tx.prepare(
                r#"
                INSERT INTO fleet_snapshot (
                  icao24, n_number, ticker, cik, company_name, make, model,
                  aviation_issuer, fleet_size, snapshot_as_of
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
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
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn upsert_trip(&self, row: &TripRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT OR REPLACE INTO trips (
              icao24, dep_ts, arr_ts, n_number, ticker, cik,
              dep_lat, dep_lon, arr_lat, arr_lon,
              dep_airport, arr_airport, dep_place, arr_place,
              source, fetched_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
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
            ],
        )?;
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
        let now = Utc::now().to_rfc3339();
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
        let now = Utc::now().to_rfc3339();
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

    pub fn open_trip(&self, icao24: &str) -> Result<Option<TripRow>> {
        self.conn
            .query_row(
                r#"
                SELECT icao24, dep_ts, arr_ts, n_number, ticker, cik,
                       dep_lat, dep_lon, arr_lat, arr_lon,
                       dep_airport, arr_airport, dep_place, arr_place,
                       source, fetched_at
                FROM trips
                WHERE icao24 = ?1 AND arr_ts IS NULL
                ORDER BY dep_ts DESC
                LIMIT 1
                "#,
                [icao24],
                trip_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn close_open_trip(
        &self,
        icao24: &str,
        dep_ts: &str,
        arr_ts: Option<&str>,
        arr_lat: Option<f64>,
        arr_lon: Option<f64>,
        arr_airport: Option<&str>,
        arr_place: Option<&str>,
        fetched_at: &str,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE trips SET
              arr_ts = ?1,
              arr_lat = ?2,
              arr_lon = ?3,
              arr_airport = ?4,
              arr_place = ?5,
              fetched_at = ?6
            WHERE icao24 = ?7 AND dep_ts = ?8
            "#,
            params![
                arr_ts,
                arr_lat,
                arr_lon,
                arr_airport,
                arr_place,
                fetched_at,
                icao24,
                dep_ts,
            ],
        )?;
        Ok(())
    }

    pub fn trip_count(&self) -> Result<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM trips", [], |r| r.get(0))?)
    }

    pub fn get_trip(&self, icao24: &str, dep_ts: &str) -> Result<Option<TripRow>> {
        self.conn
            .query_row(
                r#"
                SELECT icao24, dep_ts, arr_ts, n_number, ticker, cik,
                       dep_lat, dep_lon, arr_lat, arr_lon,
                       dep_airport, arr_airport, dep_place, arr_place,
                       source, fetched_at
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
        let now = Utc::now().to_rfc3339();
        let hex = icao24.trim().to_ascii_lowercase();
        self.conn.execute(
            r#"
            INSERT INTO seen_airborne (icao24, utc_date, first_seen_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(icao24, utc_date) DO NOTHING
            "#,
            params![hex, date.to_string(), now],
        )?;
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
        let trips = self.trip_count()?;
        let cursors: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM fetch_cursor", [], |r| r.get(0))?;
        let cursors_with_error: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM fetch_cursor WHERE last_error IS NOT NULL AND last_error <> ''",
            [],
            |r| r.get(0),
        )?;
        let last_ok_min: Option<String> =
            self.conn
                .query_row("SELECT MIN(last_ok_date) FROM fetch_cursor", [], |r| {
                    r.get(0)
                })?;
        let last_ok_max: Option<String> =
            self.conn
                .query_row("SELECT MAX(last_ok_date) FROM fetch_cursor", [], |r| {
                    r.get(0)
                })?;
        let mut err_stmt = self.conn.prepare(
            "SELECT icao24, last_error FROM fetch_cursor WHERE last_error IS NOT NULL AND last_error <> '' ORDER BY icao24",
        )?;
        let errors = err_stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(JournalStatus {
            path: path.display().to_string(),
            fleet_hexes,
            snapshot_as_of,
            trips,
            cursors,
            cursors_with_error,
            last_ok_min,
            last_ok_max,
            errors,
        })
    }
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
    })
}

pub fn utc_iso(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::FleetRow;
    use tempfile::tempdir;

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
            source: TripSource::AdsBxTraceHist.as_str().into(),
            fetched_at: "2026-08-31T00:00:00Z".into(),
        };
        db.upsert_trip(&t).unwrap();
        db.upsert_trip(&t).unwrap();
        assert_eq!(db.trip_count().unwrap(), 1);
    }

    #[test]
    fn fleet_snapshot_replaced_not_appended() {
        let dir = tempdir().unwrap();
        let mut db = JournalDb::open(&dir.path().join("t.sqlite")).unwrap();
        db.replace_fleet_snapshot(&[row()], "2026-08-31").unwrap();
        let mut gone = row();
        gone.icao24 = "ffffff".into();
        db.replace_fleet_snapshot(&[gone], "2026-08-31").unwrap();
        assert!(!db.in_fleet("abcdef").unwrap());
        assert!(db.in_fleet("ffffff").unwrap());
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
}

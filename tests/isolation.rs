//! Mapping DB isolation + idempotent reprocess (spec §9).

use std::fs;
use std::path::Path;

use adsb_trip_journal::collect::{
    collect_from_cache, trace_cache_path, write_trace_cache, CollectReport,
};
use adsb_trip_journal::store::JournalDb;
use chrono::NaiveDate;
use rusqlite::Connection;
use tempfile::tempdir;

const DAY1: &str = "2024-01-15";
const DAY2: &str = "2024-01-16";
const HEX: &str = "abcdef";

fn mapping_sql() -> &'static str {
    r#"
    CREATE TABLE mappings_current (
      n_number TEXT PRIMARY KEY,
      icao24 TEXT,
      ticker TEXT NOT NULL,
      cik TEXT,
      company_name TEXT,
      make TEXT,
      model TEXT,
      registrant_name TEXT,
      match_method TEXT,
      as_of_date TEXT,
      fleet_size INTEGER NOT NULL,
      aviation_issuer INTEGER NOT NULL
    );
    INSERT INTO mappings_current VALUES
      ('N1','ABCDEF','AAA','0001','A Co','GULFSTREAM','GVI',NULL,'exact_legal_name','2026-08-31',1,0),
      ('N2','','AAA',NULL,'A Co',NULL,NULL,NULL,NULL,'2026-08-31',1,0),
      ('N3',NULL,'AAA',NULL,'A Co',NULL,NULL,NULL,NULL,'2026-08-31',2,0),
      ('N4','aabbcc','BIG',NULL,'Big Co',NULL,NULL,NULL,NULL,'2026-08-31',20,0),
      ('N5','ddeeff','OEM',NULL,'Oem Co',NULL,NULL,NULL,NULL,'2026-08-31',1,1);
    "#
}

fn write_mapping(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(mapping_sql()).unwrap();
}

/// 2024-01-15 00:00:00 UTC. Two legs: KAPA short hop, then airborne toward JFK
/// still open at midnight (no landing).
fn day1_trace() -> String {
    r#"{
      "icao": "abcdef",
      "timestamp": 1705276800,
      "trace": [
        [100, 39.5701, -104.6737, "ground", 8, 90, 0],
        [120, 39.5701, -104.6737, 500, 110, 90, 2],
        [400, 39.5720, -104.6700, 800, 130, 90, 0],
        [900, 39.5701, -104.6737, "ground", 10, 90, 0],
        [2000, 39.5701, -104.6737, 600, 140, 80, 2],
        [80000, 41.0, -90.0, 35000, 420, 90, 0]
      ]
    }"#
    .to_string()
}

/// Continuation after midnight, then land KJFK.
fn day2_trace() -> String {
    r#"{
      "icao": "abcdef",
      "timestamp": 1705363200,
      "trace": [
        [100, 41.2, -80.0, 34000, 400, 90, 0],
        [4000, 40.6399, -73.7787, 800, 180, 90, 0],
        [4200, 40.6399, -73.7787, "ground", 8, 90, 0]
      ]
    }"#
    .to_string()
}

fn airports_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/airports.csv")
}

fn seed_cache(cache: &Path) {
    let d1 = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
    write_trace_cache(&trace_cache_path(cache, d1, HEX), day1_trace().as_bytes()).unwrap();
    write_trace_cache(&trace_cache_path(cache, d2, HEX), day2_trace().as_bytes()).unwrap();
}

async fn run(dir: &Path) -> CollectReport {
    let mapping = dir.join("map.sqlite");
    let journal = dir.join("trips.sqlite");
    let cache = dir.join("cache");
    if !mapping.exists() {
        write_mapping(&mapping);
    }
    seed_cache(&cache);
    collect_from_cache(
        &mapping,
        &journal,
        &cache,
        Some(&airports_path()),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn mapping_sqlite_unchanged_after_collect() {
    let dir = tempdir().unwrap();
    let mapping = dir.path().join("map.sqlite");
    write_mapping(&mapping);
    let size_before = fs::metadata(&mapping).unwrap().len();
    let count_before: i64 = {
        let c = Connection::open(&mapping).unwrap();
        c.query_row("SELECT COUNT(*) FROM mappings_current", [], |r| r.get(0))
            .unwrap()
    };

    let report = run(dir.path()).await;
    assert_eq!(report.skipped_empty_icao24, 2);
    assert_eq!(report.fleet_hexes, 1);

    let size_after = fs::metadata(&mapping).unwrap().len();
    let count_after: i64 = {
        let c = Connection::open(&mapping).unwrap();
        c.query_row("SELECT COUNT(*) FROM mappings_current", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(size_before, size_after);
    assert_eq!(count_before, count_after);
}

#[tokio::test]
async fn reprocess_is_idempotent_and_snaps_airports() {
    let dir = tempdir().unwrap();
    let first = run(dir.path()).await;
    let journal = dir.path().join("trips.sqlite");
    let db = JournalDb::open(&journal).unwrap();
    let n1 = db.trip_count().unwrap();
    assert!(
        n1 >= 2,
        "expected short hop + overnight-merged long hop, got {n1}"
    );

    let short = db
        .get_trip(HEX, "2024-01-15T00:02:00Z")
        .unwrap()
        .expect("short hop dep at +120s");
    assert_eq!(short.dep_airport.as_deref(), Some("KAPA"));
    assert_eq!(short.arr_airport.as_deref(), Some("KAPA"));
    assert_eq!(short.ticker, "AAA");
    assert_eq!(short.source, "adsbx_trace_hist");

    let long = db
        .get_trip(HEX, "2024-01-15T00:33:20Z")
        .unwrap()
        .expect("second leg dep at +2000s");
    assert_eq!(long.dep_airport.as_deref(), Some("KAPA"));
    assert_eq!(long.arr_airport.as_deref(), Some("KJFK"));
    assert!(
        long.arr_ts.is_some(),
        "overnight merge should close arr_ts on day 2"
    );

    let second = run(dir.path()).await;
    let n2 = JournalDb::open(&journal).unwrap().trip_count().unwrap();
    assert_eq!(n1, n2, "PRIMARY KEY (icao24, dep_ts) must be idempotent");
    assert_eq!(first.fleet_hexes, second.fleet_hexes);

    let cur = JournalDb::open(&journal)
        .unwrap()
        .last_ok_date(HEX)
        .unwrap()
        .unwrap();
    assert_eq!(cur.to_string(), DAY2);
}

#[tokio::test]
async fn dropped_tails_keep_trips_but_leave_snapshot() {
    let dir = tempdir().unwrap();
    run(dir.path()).await;
    let journal = dir.path().join("trips.sqlite");
    let trips_before = JournalDb::open(&journal).unwrap().trip_count().unwrap();

    let mapping = dir.path().join("map.sqlite");
    let conn = Connection::open(&mapping).unwrap();
    conn.execute("DELETE FROM mappings_current WHERE n_number = 'N1'", [])
        .unwrap();
    drop(conn);

    let cache = dir.path().join("cache");
    collect_from_cache(
        &mapping,
        &journal,
        &cache,
        Some(&airports_path()),
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        NaiveDate::from_ymd_opt(2024, 1, 16).unwrap(),
    )
    .await
    .unwrap();

    let db = JournalDb::open(&journal).unwrap();
    assert_eq!(db.trip_count().unwrap(), trips_before);
    assert!(!db.in_fleet(HEX).unwrap());
}

#[test]
fn day_constants_match_unix() {
    let _ = (DAY1, DAY2);
    assert_eq!(
        chrono::DateTime::from_timestamp(1_705_276_800, 0)
            .unwrap()
            .date_naive()
            .to_string(),
        "2024-01-15"
    );
    assert_eq!(
        chrono::DateTime::from_timestamp(1_705_363_200, 0)
            .unwrap()
            .date_naive()
            .to_string(),
        "2024-01-16"
    );
}

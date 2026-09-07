//! OpenSky ingest: mapping DB isolation, airport ident lookup, leftover fetch_cursor APIs.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use adsb_trip_journal::airports::AirportIndex;
use adsb_trip_journal::collect::ingest_opensky_flights;
use adsb_trip_journal::fleet::{self, query_fleet};
use adsb_trip_journal::opensky::{parse_flights, TrackEnds};
use adsb_trip_journal::store::JournalDb;
use chrono::NaiveDate;
use rusqlite::Connection;
use tempfile::tempdir;

const HEX: &str = "abcdef";
const DAY: &str = "2024-01-15";

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
      aviation_issuer INTEGER NOT NULL,
      deleted_at INTEGER
    );
    INSERT INTO mappings_current VALUES
      ('N1','ABCDEF','AAA','0001','A Co','GULFSTREAM','GVI',NULL,'exact_legal_name','2026-08-31',1,0,NULL),
      ('N2','','AAA',NULL,'A Co',NULL,NULL,NULL,NULL,'2026-08-31',1,0,NULL),
      ('N3',NULL,'AAA',NULL,'A Co',NULL,NULL,NULL,NULL,'2026-08-31',2,0,NULL),
      ('N4','aabbcc','BIG',NULL,'Big Co',NULL,NULL,NULL,NULL,'2026-08-31',20,0,NULL),
      ('N5','ddeeff','OEM',NULL,'Oem Co',NULL,NULL,NULL,NULL,'2026-08-31',1,1,NULL);
    "#
}

fn write_mapping(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(mapping_sql()).unwrap();
}

fn airports() -> AirportIndex {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/airports.csv");
    AirportIndex::load_csv(&p).unwrap()
}

fn apa_jfk_flights() -> Vec<adsb_trip_journal::opensky::Flight> {
    parse_flights(
        br#"[{
          "icao24":"ABCDEF",
          "firstSeen":1705276800,
          "lastSeen":1705280400,
          "estDepartureAirport":"KAPA",
          "estArrivalAirport":"KJFK",
          "callsign":"TEST1",
          "estDepartureAirportHorizDistance": 100
        }]"#,
    )
    .unwrap()
}

#[test]
fn mapping_sqlite_unchanged_after_opensky_ingest() {
    let dir = tempdir().unwrap();
    let mapping = dir.path().join("map.sqlite");
    write_mapping(&mapping);
    let size_before = fs::metadata(&mapping).unwrap().len();
    let count_before: i64 = {
        let c = Connection::open(&mapping).unwrap();
        c.query_row("SELECT COUNT(*) FROM mappings_current", [], |r| r.get(0))
            .unwrap()
    };

    let conn = fleet::open_mapping_ro(&mapping).unwrap();
    let fleet = query_fleet(&conn).unwrap();
    assert_eq!(fleet.rows.len(), 1);
    assert_eq!(fleet.skipped_empty_icao24, 2);
    let row = &fleet.rows[0];
    assert_eq!(row.icao24, HEX);

    let journal = dir.path().join("trips.sqlite");
    let db = JournalDb::open(&journal).unwrap();
    let (up, skip) = ingest_opensky_flights(
        &db,
        row,
        &apa_jfk_flights(),
        &airports(),
        &HashMap::new(),
        "2026-09-01T00:00:00Z",
    )
    .unwrap();
    assert_eq!(up, 1);
    assert_eq!(skip, 0);

    let trip = db
        .get_trip(HEX, "2024-01-15T00:00:00Z")
        .unwrap()
        .expect("mapped OpenSky leg");
    assert_eq!(trip.dep_airport.as_deref(), Some("KAPA"));
    assert_eq!(trip.arr_airport.as_deref(), Some("KJFK"));
    assert_eq!(trip.ticker, "AAA");
    assert_eq!(trip.source, "opensky_flights");
    assert_eq!(trip.callsign.as_deref(), Some("TEST1"));
    assert_eq!(trip.dep_airport_horiz_m, Some(100));

    let size_after = fs::metadata(&mapping).unwrap().len();
    let count_after: i64 = {
        let c = Connection::open(&mapping).unwrap();
        c.query_row("SELECT COUNT(*) FROM mappings_current", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(size_before, size_after);
    assert_eq!(count_before, count_after);
}

#[test]
fn ingest_skips_legs_without_coordinates() {
    let dir = tempdir().unwrap();
    let mapping = dir.path().join("map.sqlite");
    write_mapping(&mapping);
    let conn = fleet::open_mapping_ro(&mapping).unwrap();
    let fleet = query_fleet(&conn).unwrap();
    let db = JournalDb::open(&dir.path().join("trips.sqlite")).unwrap();
    let flights = parse_flights(
        br#"[{
          "icao24":"abcdef",
          "firstSeen":1705276800,
          "estDepartureAirport":null,
          "estArrivalAirport":null
        }]"#,
    )
    .unwrap();
    let (up, skip) = ingest_opensky_flights(
        &db,
        &fleet.rows[0],
        &flights,
        &AirportIndex::empty(),
        &HashMap::new(),
        "t",
    )
    .unwrap();
    assert_eq!(up, 0);
    assert_eq!(skip, 1);
    assert_eq!(db.trip_count().unwrap(), 0);
}

#[test]
fn ingest_uses_track_when_airports_null() {
    let dir = tempdir().unwrap();
    let mapping = dir.path().join("map.sqlite");
    write_mapping(&mapping);
    let conn = fleet::open_mapping_ro(&mapping).unwrap();
    let fleet = query_fleet(&conn).unwrap();
    let db = JournalDb::open(&dir.path().join("trips.sqlite")).unwrap();
    let flights = parse_flights(
        br#"[{
          "icao24":"abcdef",
          "firstSeen":1705276800,
          "lastSeen":1705280400
        }]"#,
    )
    .unwrap();
    let mut tracks = HashMap::new();
    tracks.insert(
        1_705_276_800,
        TrackEnds {
            dep_lat: 39.57,
            dep_lon: -104.67,
            arr_lat: 40.64,
            arr_lon: -73.78,
            callsign: Some("N1".into()),
        },
    );
    let (up, skip) = ingest_opensky_flights(
        &db,
        &fleet.rows[0],
        &flights,
        &AirportIndex::empty(),
        &tracks,
        "t",
    )
    .unwrap();
    assert_eq!(up, 1);
    assert_eq!(skip, 0);
    let trip = db.get_trip(HEX, "2024-01-15T00:00:00Z").unwrap().unwrap();
    assert_eq!(trip.source, "opensky_track");
    assert!(trip.dep_airport.is_none());
    assert_eq!(trip.callsign.as_deref(), Some("N1"));
}

#[test]
fn cursor_404_advances_429_does_not() {
    let dir = tempdir().unwrap();
    let db = JournalDb::open(&dir.path().join("trips.sqlite")).unwrap();
    let d = NaiveDate::parse_from_str(DAY, "%Y-%m-%d").unwrap();

    // HTTP 404: no legs that day — still a successful fetch.
    db.advance_cursor_ok(HEX, d).unwrap();
    assert_eq!(db.last_ok_date(HEX).unwrap(), Some(d));
    assert_eq!(db.trip_count().unwrap(), 0);

    db.set_cursor_error(HEX, "HTTP 429 remaining=Some(0)")
        .unwrap();
    assert_eq!(
        db.last_ok_date(HEX).unwrap(),
        Some(d),
        "429 must not advance or rewind last_ok_date"
    );
    let c = db.get_cursor(HEX).unwrap().unwrap();
    assert!(c.last_error.unwrap().contains("429"));
}

#[test]
fn ingest_collapses_cat_xom_near_duplicates() {
    let dir = tempdir().unwrap();
    let mapping = dir.path().join("map.sqlite");
    write_mapping(&mapping);
    let conn = fleet::open_mapping_ro(&mapping).unwrap();
    let fleet = query_fleet(&conn).unwrap();
    let db = JournalDb::open(&dir.path().join("trips.sqlite")).unwrap();
    // CAT-style 16s gap + incomplete second object (same hex as mapping: abcdef).
    let flights = parse_flights(
        br#"[
          {"icao24":"ABCDEF","firstSeen":1705276800,"lastSeen":1705280400,"estDepartureAirport":"KAPA","estArrivalAirport":"KJFK"},
          {"icao24":"ABCDEF","firstSeen":1705276816,"lastSeen":1705280410,"estDepartureAirport":"KAPA","estArrivalAirport":null}
        ]"#,
    )
    .unwrap();
    let (up, skip) = ingest_opensky_flights(
        &db,
        &fleet.rows[0],
        &flights,
        &airports(),
        &HashMap::new(),
        "t",
    )
    .unwrap();
    assert_eq!(up, 1);
    assert_eq!(skip, 0);
    assert_eq!(db.trip_count().unwrap(), 1);
    let trip = db.get_trip(HEX, "2024-01-15T00:00:00Z").unwrap().unwrap();
    assert_eq!(trip.arr_airport.as_deref(), Some("KJFK"));
}

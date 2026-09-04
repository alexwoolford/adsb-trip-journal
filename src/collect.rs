//! Daily collect loop: fleet snapshot → fetch/cache traces → segment → snap → trips.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::{info, warn};

use crate::adsbx::{AdsBxClient, FetchOutcome};
use crate::airports::AirportIndex;
use crate::fleet::{self, FleetQuery, FleetRow};
use crate::live::{parse_live_aircraft, CompletedLiveTrip, LiveTracker};
use crate::opensky::{
    estimated_states_credits, filter_flights_to_fleet, flight_to_trip, states_request_count,
    utc_day_two_hour_slices, Flight, OpenskyClient, OpenskyOutcome, TrackEnds,
    FLIGHTS_ALL_LOOKBACK_DAYS, FLIGHTS_ALL_SLICES_PER_DAY, FLIGHTS_ALL_SLICE_CREDITS,
};
use crate::segment::{is_overnight_continuation, parse_trace_json, segment_legs, Leg};
use crate::store::{utc_iso, JournalDb, TripRow, TripSource};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CollectSource {
    #[default]
    AdsBx,
    Opensky,
}

pub struct CollectOptions {
    pub mapping_sqlite: PathBuf,
    pub journal_sqlite: PathBuf,
    pub cache_dir: PathBuf,
    pub airports_csv: Option<PathBuf>,
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    pub live: bool,
    pub poll_interval: Duration,
    pub max_polls: Option<u32>,
    /// When set, HTTP is used for missing cache days. When None, cache-only.
    pub client: Option<AdsBxClient>,
    /// Prefer recent traces over hist when hist is unavailable.
    pub allow_hist: bool,
    pub allow_recent: bool,
    pub source: CollectSource,
    pub opensky: Option<Arc<OpenskyClient>>,
    pub max_flights_credits: u32,
    pub tracks_fallback: bool,
    /// When non-empty, only these hexes are fetched (must be in the fleet slice).
    pub hex_filter: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CollectReport {
    pub fleet_hexes: usize,
    pub skipped_empty_icao24: u64,
    pub days_ok: u64,
    pub days_not_found: u64,
    pub days_from_cache: u64,
    pub trips_upserted: u64,
    pub hexes_stopped_on_error: u64,
    pub live_polls: u64,
    pub live_trips: u64,
    pub flights_calls: u64,
    pub tracks_calls: u64,
    pub skipped_no_coords: u64,
    pub estimated_flights_credits: u32,
    pub dates_skipped_no_watch: u64,
}

pub fn default_today_utc() -> NaiveDate {
    Utc::now().date_naive()
}

pub fn flights_all_slice_cache_path(cache_dir: &Path, date: NaiveDate, slice_idx: u32) -> PathBuf {
    cache_dir
        .join("flights_all")
        .join(date.to_string())
        .join(format!("{slice_idx:02}.json.gz"))
}

pub fn trace_cache_path(cache_dir: &Path, date: NaiveDate, icao24: &str) -> PathBuf {
    cache_dir
        .join("traces")
        .join(date.to_string())
        .join(format!("{icao24}.json.gz"))
}

pub fn write_trace_cache(path: &Path, json: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(path)?;
    let mut enc = GzEncoder::new(file, Compression::default());
    enc.write_all(json)?;
    enc.finish()?;
    Ok(())
}

pub fn read_trace_cache(path: &Path) -> Result<Vec<u8>> {
    let file = fs::File::open(path)?;
    let mut dec = GzDecoder::new(file);
    let mut buf = Vec::new();
    match dec.read_to_end(&mut buf) {
        Ok(_) => Ok(buf),
        Err(_) => fs::read(path).with_context(|| format!("read {}", path.display())),
    }
}

pub async fn collect(opts: CollectOptions) -> Result<CollectReport> {
    let mapping = fleet::open_mapping_ro(&opts.mapping_sqlite)?;
    let fleet = fleet::query_fleet(&mapping)?;
    info!(
        hexes = fleet.rows.len(),
        skipped_empty = fleet.skipped_empty_icao24,
        "fleet query"
    );

    let mut db = JournalDb::open(&opts.journal_sqlite)?;
    db.replace_fleet_snapshot(&fleet.rows, &fleet.snapshot_as_of)?;

    let airports = match &opts.airports_csv {
        Some(p) if p.exists() => AirportIndex::load_csv(p)?,
        _ => {
            warn!("no airports csv; dep_airport/arr_airport will be null (lat,lon places)");
            AirportIndex::empty()
        }
    };

    if opts.source == CollectSource::Opensky {
        return collect_opensky(opts, fleet, db, airports).await;
    }
    if opts.live {
        return collect_live(opts, fleet, db, airports).await;
    }
    collect_traces(opts, fleet, db, airports).await
}

async fn collect_traces(
    opts: CollectOptions,
    fleet: FleetQuery,
    db: JournalDb,
    airports: AirportIndex,
) -> Result<CollectReport> {
    let mut report = CollectReport {
        fleet_hexes: fleet.rows.len(),
        skipped_empty_icao24: fleet.skipped_empty_icao24,
        ..CollectReport::default()
    };
    let today = opts.to.unwrap_or_else(default_today_utc);
    let fetched_at = utc_iso(Utc::now());

    for row in &fleet.rows {
        let start = match opts.from {
            Some(d) => d,
            None => match db.last_ok_date(&row.icao24)? {
                Some(last) => last.succ_opt().unwrap_or(last),
                None => today,
            },
        };
        if start > today {
            continue;
        }

        let mut date = start;
        while date <= today {
            match ingest_hex_day(&opts, &db, &airports, row, date, &fetched_at, &mut report).await?
            {
                DayResult::Ok | DayResult::NotFound => {
                    date = match date.succ_opt() {
                        Some(n) => n,
                        None => break,
                    };
                }
                DayResult::StopHex => {
                    report.hexes_stopped_on_error += 1;
                    break;
                }
            }
        }
    }
    Ok(report)
}

enum DayResult {
    Ok,
    NotFound,
    StopHex,
}

async fn ingest_hex_day(
    opts: &CollectOptions,
    db: &JournalDb,
    airports: &AirportIndex,
    row: &FleetRow,
    date: NaiveDate,
    fetched_at: &str,
    report: &mut CollectReport,
) -> Result<DayResult> {
    let cache_path = trace_cache_path(&opts.cache_dir, date, &row.icao24);

    let (json, source, from_cache) = if cache_path.exists() {
        report.days_from_cache += 1;
        (
            read_trace_cache(&cache_path)?,
            TripSource::AdsBxTraceHist,
            true,
        )
    } else if let Some(client) = &opts.client {
        match fetch_day(client, opts, &row.icao24, date).await? {
            FetchDay::Trace { body, source } => {
                write_trace_cache(&cache_path, &body)?;
                (body, source, false)
            }
            FetchDay::NotFound => {
                db.advance_cursor_ok(&row.icao24, date)?;
                report.days_not_found += 1;
                report.days_ok += 1;
                return Ok(DayResult::NotFound);
            }
            FetchDay::Fatal(msg) => {
                warn!(icao24 = %row.icao24, date = %date, %msg, "fetch failed; cursor not advanced");
                db.set_cursor_error(&row.icao24, &msg)?;
                return Ok(DayResult::StopHex);
            }
        }
    } else {
        let msg = format!("no cache for {date} and no API client");
        db.set_cursor_error(&row.icao24, &msg)?;
        return Ok(DayResult::StopHex);
    };

    let _ = from_cache;
    let file = parse_trace_json(&json)?;
    let mut legs = segment_legs(&file);
    merge_overnight(db, row, &mut legs, airports, source, fetched_at)?;

    for leg in legs {
        let trip = snap_trip(row, &leg, airports, source, fetched_at);
        db.upsert_trip(&trip)?;
        report.trips_upserted += 1;
    }
    db.advance_cursor_ok(&row.icao24, date)?;
    report.days_ok += 1;
    Ok(DayResult::Ok)
}

enum FetchDay {
    Trace { body: Vec<u8>, source: TripSource },
    NotFound,
    Fatal(String),
}

async fn fetch_day(
    client: &AdsBxClient,
    opts: &CollectOptions,
    icao24: &str,
    date: NaiveDate,
) -> Result<FetchDay> {
    if opts.allow_hist {
        match retry_fetch(|| client.fetch_hist(icao24, date)).await? {
            FetchOutcome::Ok { body } => {
                return Ok(FetchDay::Trace {
                    body,
                    source: TripSource::AdsBxTraceHist,
                })
            }
            FetchOutcome::NotFound => return Ok(FetchDay::NotFound),
            FetchOutcome::Denied { status, message } => {
                return Ok(FetchDay::Fatal(format!("hist HTTP {status}: {message}")))
            }
            FetchOutcome::RateLimited { retry_after } => {
                return Ok(FetchDay::Fatal(format!(
                    "hist HTTP 429 retry-after={retry_after:?}"
                )))
            }
            FetchOutcome::Other { status, message } => {
                // Fall through to recent if hist is simply unimplemented on this key.
                if status == 404 {
                    return Ok(FetchDay::NotFound);
                }
                if !opts.allow_recent {
                    return Ok(FetchDay::Fatal(format!("hist HTTP {status}: {message}")));
                }
            }
        }
    }

    if opts.allow_recent {
        match retry_fetch(|| client.fetch_recent(icao24)).await? {
            FetchOutcome::Ok { body } => {
                return Ok(FetchDay::Trace {
                    body,
                    source: TripSource::AdsBxTraceRecent,
                })
            }
            FetchOutcome::NotFound => return Ok(FetchDay::NotFound),
            other => {
                if let Some(msg) = other.error_message() {
                    return Ok(FetchDay::Fatal(format!("recent {msg}")));
                }
            }
        }
    }

    Ok(FetchDay::Fatal(
        "no hist/recent access and no cached trace".into(),
    ))
}

async fn retry_fetch<F, Fut>(mut f: F) -> Result<FetchOutcome>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<FetchOutcome>>,
{
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        match f().await? {
            FetchOutcome::RateLimited { retry_after } if attempts < 4 => {
                let wait = retry_after.unwrap_or(30).min(120);
                warn!(wait, attempts, "429; sleeping");
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }
            other => return Ok(other),
        }
    }
}

fn merge_overnight(
    db: &JournalDb,
    row: &FleetRow,
    legs: &mut Vec<Leg>,
    airports: &AirportIndex,
    _source: TripSource,
    fetched_at: &str,
) -> Result<()> {
    let Some(open) = db.open_trip(&row.icao24)? else {
        return Ok(());
    };
    if legs.is_empty() {
        return Ok(());
    }
    if !is_overnight_continuation(&legs[0]) {
        return Ok(());
    }
    let first = legs.remove(0);
    let arr_snap = airports.snap(first.arr_lat, first.arr_lon);
    let arr_ts = first.arr_ts.map(utc_iso);
    db.close_open_trip(
        &open.icao24,
        &open.dep_ts,
        arr_ts.as_deref(),
        Some(first.arr_lat),
        Some(first.arr_lon),
        arr_snap.ident.as_deref(),
        Some(arr_snap.place.as_str()),
        fetched_at,
    )?;
    Ok(())
}

fn snap_trip(
    row: &FleetRow,
    leg: &Leg,
    airports: &AirportIndex,
    source: TripSource,
    fetched_at: &str,
) -> TripRow {
    let dep = airports.snap(leg.dep_lat, leg.dep_lon);
    let arr = airports.snap(leg.arr_lat, leg.arr_lon);
    TripRow {
        icao24: row.icao24.clone(),
        dep_ts: utc_iso(leg.dep_ts),
        arr_ts: leg.arr_ts.map(utc_iso),
        n_number: row.n_number.clone(),
        ticker: row.ticker.clone(),
        cik: row.cik.clone(),
        dep_lat: Some(leg.dep_lat),
        dep_lon: Some(leg.dep_lon),
        arr_lat: Some(leg.arr_lat),
        arr_lon: Some(leg.arr_lon),
        dep_airport: dep.ident,
        arr_airport: arr.ident,
        dep_place: Some(dep.place),
        arr_place: Some(arr.place),
        source: source.as_str().to_string(),
        fetched_at: fetched_at.to_string(),
    }
}

fn live_trip_row(t: &CompletedLiveTrip, airports: &AirportIndex, fetched_at: &str) -> TripRow {
    let dep = airports.snap(t.dep_lat, t.dep_lon);
    let arr = airports.snap(t.arr_lat, t.arr_lon);
    TripRow {
        icao24: t.icao24.clone(),
        dep_ts: utc_iso(t.dep_ts),
        arr_ts: Some(utc_iso(t.arr_ts)),
        n_number: t.n_number.clone(),
        ticker: t.ticker.clone(),
        cik: t.cik.clone(),
        dep_lat: Some(t.dep_lat),
        dep_lon: Some(t.dep_lon),
        arr_lat: Some(t.arr_lat),
        arr_lon: Some(t.arr_lon),
        dep_airport: dep.ident,
        arr_airport: arr.ident,
        dep_place: Some(dep.place),
        arr_place: Some(arr.place),
        source: t.source.as_str().to_string(),
        fetched_at: fetched_at.to_string(),
    }
}

async fn collect_live(
    opts: CollectOptions,
    fleet: FleetQuery,
    db: JournalDb,
    airports: AirportIndex,
) -> Result<CollectReport> {
    let mut report = CollectReport {
        fleet_hexes: fleet.rows.len(),
        skipped_empty_icao24: fleet.skipped_empty_icao24,
        ..CollectReport::default()
    };
    let Some(client) = opts.client else {
        anyhow::bail!("collect --live requires ADSBX_API_KEY or RAPIDAPI_KEY");
    };
    let hexes: Vec<String> = fleet.rows.iter().map(|r| r.icao24.clone()).collect();
    let mut tracker = LiveTracker::new(&fleet.rows);
    let mut polls = 0u32;
    loop {
        polls += 1;
        report.live_polls += 1;
        let fetched_at = utc_iso(Utc::now());
        match client.fetch_live_batch(&hexes).await? {
            FetchOutcome::Ok { body } => {
                let samples = parse_live_aircraft(&body)?;
                let trips = tracker.ingest(&samples, Utc::now());
                for t in trips {
                    db.upsert_trip(&live_trip_row(&t, &airports, &fetched_at))?;
                    report.live_trips += 1;
                    report.trips_upserted += 1;
                }
            }
            other => {
                if let Some(msg) = other.error_message() {
                    warn!(%msg, "live poll failed");
                    if !other.advances_cursor() {
                        for h in &hexes {
                            db.set_cursor_error(h, &msg)?;
                        }
                        break;
                    }
                }
            }
        }
        if opts.max_polls.map(|m| polls >= m).unwrap_or(false) {
            break;
        }
        tokio::time::sleep(opts.poll_interval).await;
    }
    let today = default_today_utc();
    for row in &fleet.rows {
        db.advance_cursor_ok(&row.icao24, today)?;
        report.days_ok += 1;
    }
    Ok(report)
}

/// Used by tests: process already-cached traces for a date range (no HTTP).
pub async fn collect_from_cache(
    mapping_sqlite: &Path,
    journal_sqlite: &Path,
    cache_dir: &Path,
    airports_csv: Option<&Path>,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<CollectReport> {
    collect(CollectOptions {
        mapping_sqlite: mapping_sqlite.to_path_buf(),
        journal_sqlite: journal_sqlite.to_path_buf(),
        cache_dir: cache_dir.to_path_buf(),
        airports_csv: airports_csv.map(|p| p.to_path_buf()),
        from: Some(from),
        to: Some(to),
        live: false,
        poll_interval: Duration::from_secs(1),
        max_polls: None,
        client: None,
        allow_hist: false,
        allow_recent: false,
        source: CollectSource::AdsBx,
        opensky: None,
        max_flights_credits: 500,
        tracks_fallback: true,
        hex_filter: Vec::new(),
    })
    .await
}

pub async fn watch_opensky(
    mapping_sqlite: &Path,
    journal_sqlite: &Path,
    client: Arc<OpenskyClient>,
    interval: Duration,
    max_polls: Option<u32>,
) -> Result<CollectReport> {
    let mut db = JournalDb::open(journal_sqlite)?;
    let mut report = CollectReport::default();
    let mut polls = 0u32;
    let mut prev_remaining: Option<u32> = None;
    loop {
        polls += 1;
        report.live_polls += 1;
        let mapping = fleet::open_mapping_ro(mapping_sqlite)?;
        let fleet = fleet::query_fleet(&mapping)?;
        drop(mapping);
        db.replace_fleet_snapshot(&fleet.rows, &fleet.snapshot_as_of)?;
        report.fleet_hexes = fleet.rows.len();
        report.skipped_empty_icao24 = fleet.skipped_empty_icao24;
        let hexes: Vec<String> = fleet.rows.iter().map(|r| r.icao24.clone()).collect();
        let date = default_today_utc();
        match client.states_fleet(&hexes).await? {
            OpenskyOutcome::Ok { data, credit } => {
                let chunks = states_request_count(hexes.len());
                let estimated = estimated_states_credits(hexes.len());
                let spent = match (prev_remaining, credit.remaining) {
                    (Some(before), Some(after)) if before >= after => before - after,
                    _ => estimated,
                };
                info!(
                    n = data.len(),
                    remaining = ?credit.remaining,
                    spent,
                    chunks,
                    fleet = hexes.len(),
                    "opensky states"
                );
                prev_remaining = credit.remaining;
                for hex in &data {
                    db.mark_seen_airborne(hex, date)?;
                }
                // Collect is not gated on seen_airborne; this table is a live diagnostic.
            }
            OpenskyOutcome::RateLimited { credit } => {
                let wait = credit.retry_after.unwrap_or(60).max(1);
                warn!(wait, remaining = ?credit.remaining, "opensky states 429");
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }
            other => {
                if let Some(msg) = other.error_message() {
                    warn!(%msg, "opensky states failed");
                    if !other.advances_cursor() {
                        anyhow::bail!(
                            "opensky states failed (HTTP does not advance cursor): {msg}"
                        );
                    }
                }
            }
        }
        if max_polls.map(|m| polls >= m).unwrap_or(false) {
            break;
        }
        tokio::time::sleep(interval).await;
    }
    Ok(report)
}

/// Cluster window for OpenSky near-duplicate FlightObjects (gold-set CAT 16s, XOM 145s).
pub const OPENSKY_NEAR_DUP_SECS: i64 = 180;

/// Keep the most complete FlightObject when several share a hex and firstSeen within 180s.
pub fn collapse_near_duplicate_flights(flights: &[Flight]) -> Vec<Flight> {
    if flights.len() <= 1 {
        return flights.to_vec();
    }
    let mut sorted = flights.to_vec();
    sorted.sort_by_key(|f| (f.icao24.clone(), f.first_seen));
    let mut clusters: Vec<Vec<Flight>> = Vec::new();
    for f in sorted {
        match clusters.last_mut() {
            Some(c)
                if c[0].icao24 == f.icao24
                    && f.first_seen.saturating_sub(c[0].first_seen) <= OPENSKY_NEAR_DUP_SECS =>
            {
                c.push(f);
            }
            _ => clusters.push(vec![f]),
        }
    }
    clusters
        .into_iter()
        .map(|mut c| {
            c.sort_by_key(|f| std::cmp::Reverse(flight_completeness(f)));
            c.into_iter().next().unwrap()
        })
        .collect()
}

fn flight_completeness(f: &Flight) -> (u8, u8, i64) {
    let dep = u8::from(f.est_departure_airport.is_some());
    let arr = u8::from(f.est_arrival_airport.is_some());
    (dep + arr, arr, -f.first_seen)
}

/// Map already-fetched OpenSky flights into `trips` (no HTTP). Used by collect and tests.
pub fn ingest_opensky_flights(
    db: &JournalDb,
    row: &FleetRow,
    flights: &[Flight],
    airports: &AirportIndex,
    tracks: &HashMap<i64, TrackEnds>,
    fetched_at: &str,
) -> Result<(u64, u64)> {
    let flights = collapse_near_duplicate_flights(flights);
    let mut upserted = 0u64;
    let mut skipped = 0u64;
    for f in &flights {
        let has_airports = f.est_departure_airport.is_some() || f.est_arrival_airport.is_some();
        let track = tracks.get(&f.first_seen).copied();
        let source = if has_airports && track.is_none() {
            TripSource::OpenskyFlights
        } else if track.is_some() && !has_airports {
            TripSource::OpenskyTrack
        } else {
            TripSource::OpenskyFlights
        };
        match flight_to_trip(f, row, airports, track, fetched_at, source) {
            Some(trip) => {
                db.upsert_trip(&trip)?;
                upserted += 1;
            }
            None => skipped += 1,
        }
    }
    Ok((upserted, skipped))
}

async fn collect_opensky(
    opts: CollectOptions,
    fleet: FleetQuery,
    db: JournalDb,
    airports: AirportIndex,
) -> Result<CollectReport> {
    let Some(client) = opts.opensky.clone() else {
        anyhow::bail!("collect --source opensky requires OPENSKY_CLIENT_ID/SECRET or OPENSKY_CREDENTIALS_JSON");
    };
    let mut report = CollectReport {
        fleet_hexes: fleet.rows.len(),
        skipped_empty_icao24: fleet.skipped_empty_icao24,
        ..CollectReport::default()
    };
    let yesterday = default_today_utc()
        .pred_opt()
        .unwrap_or_else(default_today_utc);
    let end = opts.to.unwrap_or(yesterday).min(yesterday);
    let by_hex: HashMap<String, FleetRow> = fleet
        .rows
        .iter()
        .map(|r| (r.icao24.clone(), r.clone()))
        .collect();
    let hex_filter: Vec<String> = opts
        .hex_filter
        .iter()
        .map(|h| h.trim().to_ascii_lowercase())
        .filter(|h| !h.is_empty())
        .collect();
    if !hex_filter.is_empty() {
        for h in hex_filter.iter().filter(|h| !by_hex.contains_key(*h)) {
            warn!(hex = %h, "not in default fleet slice; skip");
        }
        if hex_filter.iter().all(|h| !by_hex.contains_key(h)) {
            anyhow::bail!("none of --hex values are in the mapped fleet slice");
        }
    }
    let fleet_hexes: HashSet<String> = if hex_filter.is_empty() {
        by_hex.keys().cloned().collect()
    } else {
        hex_filter
            .iter()
            .filter(|h| by_hex.contains_key(*h))
            .cloned()
            .collect()
    };
    let fetched_at = utc_iso(Utc::now());

    let mut date = match opts.from {
        Some(d) => d,
        None => db.default_opensky_collect_from(
            yesterday,
            FLIGHTS_ALL_SLICES_PER_DAY,
            FLIGHTS_ALL_LOOKBACK_DAYS,
        )?,
    };
    if date > end {
        info!(%date, %end, "opensky collect: nothing to do (default window is yesterday UTC)");
        return Ok(report);
    }

    info!(
        %date,
        %end,
        slices = FLIGHTS_ALL_SLICES_PER_DAY,
        fleet = fleet_hexes.len(),
        "opensky collect via /flights/all (not gated on seen_airborne)"
    );

    while date <= end {
        if db.flights_all_day_complete(date, FLIGHTS_ALL_SLICES_PER_DAY)? {
            info!(%date, "flights/all day already complete");
            date = match date.succ_opt() {
                Some(n) => n,
                None => break,
            };
            continue;
        }
        let slices = utc_day_two_hour_slices(date);
        let mut halted = false;
        for (slice_idx, (begin, end_ts)) in slices.into_iter().enumerate() {
            let slice_idx = slice_idx as u32;
            if db.flights_all_slice_complete(date, slice_idx)? {
                continue;
            }
            let cache_path = flights_all_slice_cache_path(&opts.cache_dir, date, slice_idx);
            let (mut flights, from_cache) = if cache_path.exists() {
                let raw = read_trace_cache(&cache_path)?;
                (crate::opensky::parse_flights(&raw)?, true)
            } else {
                if report
                    .estimated_flights_credits
                    .saturating_add(FLIGHTS_ALL_SLICE_CREDITS)
                    > opts.max_flights_credits
                {
                    warn!(
                        used = report.estimated_flights_credits,
                        cap = opts.max_flights_credits,
                        "max-flights-credits reached; stopping"
                    );
                    db.set_flights_all_error(date, "max-flights-credits cap")?;
                    halted = true;
                    break;
                }
                report.flights_calls += 1;
                report.estimated_flights_credits += FLIGHTS_ALL_SLICE_CREDITS;
                match client.flights_all(begin, end_ts).await? {
                    (OpenskyOutcome::Ok { data, credit }, bytes, elapsed_ms) => {
                        info!(
                            %date,
                            slice = slice_idx,
                            n = data.len(),
                            bytes,
                            elapsed_ms,
                            remaining = ?credit.remaining,
                            "opensky flights/all"
                        );
                        let filtered = filter_flights_to_fleet(&data, &fleet_hexes);
                        let json = serde_json::to_vec(&filtered)?;
                        write_trace_cache(&cache_path, &json)?;
                        (filtered, false)
                    }
                    (OpenskyOutcome::NotFound { credit }, ..) => {
                        info!(
                            %date,
                            slice = slice_idx,
                            remaining = ?credit.remaining,
                            "opensky flights/all 404"
                        );
                        write_trace_cache(&cache_path, b"[]")?;
                        report.days_not_found += 1;
                        (Vec::new(), false)
                    }
                    (OpenskyOutcome::RateLimited { credit }, ..) => {
                        let msg = format!("flights/all HTTP 429 remaining={:?}", credit.remaining);
                        warn!(%date, slice = slice_idx, %msg);
                        db.set_flights_all_error(date, &msg)?;
                        report.hexes_stopped_on_error += 1;
                        return Ok(report);
                    }
                    (other, ..) => {
                        let msg = other
                            .error_message()
                            .unwrap_or_else(|| "opensky flights/all failed".into());
                        warn!(%date, slice = slice_idx, %msg);
                        db.set_flights_all_error(date, &msg)?;
                        report.hexes_stopped_on_error += 1;
                        return Ok(report);
                    }
                }
            };
            flights = filter_flights_to_fleet(&flights, &fleet_hexes);
            if from_cache {
                report.days_from_cache += 1;
                info!(%date, slice = slice_idx, n = flights.len(), "flights/all slice from cache");
            }
            match ingest_filtered_opensky_slice(
                &client,
                &db,
                &by_hex,
                &flights,
                &airports,
                opts.tracks_fallback,
                &fetched_at,
                &mut report,
            )
            .await?
            {
                SliceHalt::Stop => {
                    db.set_flights_all_error(date, "tracks halted slice")?;
                    return Ok(report);
                }
                SliceHalt::Continue => {}
            }
            db.mark_flights_all_slice_ok(date, slice_idx)?;
        }
        if halted {
            return Ok(report);
        }
        if db.flights_all_day_complete(date, FLIGHTS_ALL_SLICES_PER_DAY)? {
            for hex in &fleet_hexes {
                db.advance_cursor_ok(hex, date)?;
            }
            report.days_ok += 1;
            info!(%date, "flights/all day complete");
        }
        date = match date.succ_opt() {
            Some(n) => n,
            None => break,
        };
    }
    Ok(report)
}

enum SliceHalt {
    Continue,
    Stop,
}

async fn ingest_filtered_opensky_slice(
    client: &OpenskyClient,
    db: &JournalDb,
    by_hex: &HashMap<String, FleetRow>,
    flights: &[Flight],
    airports: &AirportIndex,
    tracks_fallback: bool,
    fetched_at: &str,
    report: &mut CollectReport,
) -> Result<SliceHalt> {
    let mut by_icao: HashMap<String, Vec<Flight>> = HashMap::new();
    for f in flights {
        by_icao.entry(f.icao24.clone()).or_default().push(f.clone());
    }
    for (hex, legs) in by_icao {
        let Some(row) = by_hex.get(&hex) else {
            continue;
        };
        let data = collapse_near_duplicate_flights(&legs);
        let mut tracks = HashMap::new();
        for f in &data {
            let can_place = f
                .est_departure_airport
                .as_deref()
                .and_then(|id| airports.by_ident(id))
                .is_some()
                || f.est_arrival_airport
                    .as_deref()
                    .and_then(|id| airports.by_ident(id))
                    .is_some();
            if !can_place && tracks_fallback {
                report.tracks_calls += 1;
                match client.tracks(&hex, f.first_seen).await? {
                    OpenskyOutcome::Ok {
                        data: Some(ends), ..
                    } => {
                        tracks.insert(f.first_seen, ends);
                    }
                    OpenskyOutcome::RateLimited { credit } => {
                        let msg = format!("tracks HTTP 429 remaining={:?}", credit.remaining);
                        warn!(%hex, %msg);
                        db.set_cursor_error(&hex, &msg)?;
                        report.hexes_stopped_on_error += 1;
                        return Ok(SliceHalt::Stop);
                    }
                    OpenskyOutcome::Denied { status, message } => {
                        db.set_cursor_error(&hex, &format!("tracks HTTP {status}: {message}"))?;
                        report.hexes_stopped_on_error += 1;
                        return Ok(SliceHalt::Stop);
                    }
                    _ => {}
                }
            }
        }
        let (up, skip) = ingest_opensky_flights(db, row, &data, airports, &tracks, fetched_at)?;
        report.trips_upserted += up;
        report.skipped_no_coords += skip;
    }
    Ok(SliceHalt::Continue)
}

pub fn print_status(status: &crate::store::JournalStatus, fleet: Option<&FleetQuery>) {
    println!("journal: {}", status.path);
    println!(
        "fleet_snapshot: {} hexes (as of {}, recorded_at {})",
        status.fleet_hexes,
        status.snapshot_as_of.as_deref().unwrap_or("?"),
        status.recorded_at.as_deref().unwrap_or("?")
    );
    if let Some(f) = fleet {
        println!(
            "skipped_empty_icao24 (current map slice): {}",
            f.skipped_empty_icao24
        );
    }
    println!("trips: {}", status.trips);
    println!(
        "cursors: {} (errors {}) last_ok {} .. {}",
        status.cursors,
        status.cursors_with_error,
        status.last_ok_min.as_deref().unwrap_or("—"),
        status.last_ok_max.as_deref().unwrap_or("—"),
    );
    for (hex, err) in &status.errors {
        println!("  error {hex}: {err}");
    }
    if let Some(d) = &status.flights_all_complete_max {
        println!("flights_all last complete UTC day: {d}");
    } else {
        println!("flights_all last complete UTC day: —");
    }
    for (d, n) in &status.flights_all_incomplete {
        println!("  flights_all in progress {d}: {n}/12 slices");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_path_layout() {
        let p = trace_cache_path(
            Path::new("cache"),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            "abcdef",
        );
        assert_eq!(p, PathBuf::from("cache/traces/2024-01-15/abcdef.json.gz"));
        let s = flights_all_slice_cache_path(
            Path::new("cache"),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            7,
        );
        assert_eq!(s, PathBuf::from("cache/flights_all/2024-01-15/07.json.gz"));
    }

    #[test]
    fn collapse_keeps_complete_pair_in_near_dup_cluster() {
        let flights = crate::opensky::parse_flights(
            br#"[
              {"icao24":"a12c04","firstSeen":1756675233,"lastSeen":1756678148,"estDepartureAirport":"KOAK","estArrivalAirport":"KBUR"},
              {"icao24":"a12c04","firstSeen":1756675249,"lastSeen":1756678174,"estDepartureAirport":"KOAK","estArrivalAirport":null},
              {"icao24":"a004b4","firstSeen":1756637256,"lastSeen":1756657402,"estDepartureAirport":"LEBL","estArrivalAirport":"OTBD"},
              {"icao24":"a004b4","firstSeen":1756637401,"lastSeen":1756657409,"estDepartureAirport":"LEBL","estArrivalAirport":null},
              {"icao24":"a12c04","firstSeen":1756680000,"lastSeen":1756683600,"estDepartureAirport":"KBUR","estArrivalAirport":"KAPA"}
            ]"#,
        )
        .unwrap();
        let kept = collapse_near_duplicate_flights(&flights);
        assert_eq!(kept.len(), 3, "two hops for CAT plus one XOM");
        let cat_oak = kept
            .iter()
            .find(|f| f.icao24 == "a12c04" && f.est_arrival_airport.as_deref() == Some("KBUR"))
            .unwrap();
        assert_eq!(cat_oak.first_seen, 1_756_675_233);
        assert!(kept
            .iter()
            .any(|f| f.icao24 == "a004b4" && f.est_arrival_airport.as_deref() == Some("OTBD")));
        assert!(kept
            .iter()
            .any(|f| f.icao24 == "a12c04" && f.est_arrival_airport.as_deref() == Some("KAPA")));
    }

    #[test]
    fn collapse_across_adjacent_slices_is_idempotent() {
        let a = crate::opensky::parse_flights(
            br#"[{"icao24":"abcdef","firstSeen":100,"lastSeen":200,"estDepartureAirport":"KAPA","estArrivalAirport":"KJFK"}]"#,
        )
        .unwrap();
        let b = crate::opensky::parse_flights(
            br#"[{"icao24":"abcdef","firstSeen":100,"lastSeen":200,"estDepartureAirport":"KAPA","estArrivalAirport":"KJFK"}]"#,
        )
        .unwrap();
        let mut both = a;
        both.extend(b);
        let kept = collapse_near_duplicate_flights(&both);
        assert_eq!(kept.len(), 1);
    }
}

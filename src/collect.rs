//! Daily collect loop: fleet snapshot → OpenSky /flights/all → snap → trips.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::future::Future;
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

use crate::airports::AirportIndex;
use crate::fleet::{self, FleetQuery, FleetRow};
use crate::opensky::{
    estimated_states_credits, filter_flights_to_fleet, flight_to_trip, states_request_count,
    utc_day_two_hour_slices, Flight, OpenskyClient, OpenskyOutcome, TrackEnds,
    FLIGHTS_ALL_LOOKBACK_DAYS, FLIGHTS_ALL_SLICES_PER_DAY, FLIGHTS_ALL_SLICE_CREDITS,
};
use crate::store::{utc_iso, JournalDb, TripSource};

pub struct CollectOptions {
    pub mapping_sqlite: PathBuf,
    pub journal_sqlite: PathBuf,
    pub cache_dir: PathBuf,
    pub airports_csv: Option<PathBuf>,
    pub from: Option<NaiveDate>,
    pub to: Option<NaiveDate>,
    pub opensky: Option<Arc<OpenskyClient>>,
    pub max_flights_credits: u32,
    pub tracks_fallback: bool,
    /// When non-empty, ingest only these mapped hexes. Slice cache is still the
    /// full fleet; a `--hex` run does not mark the UTC day complete.
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

pub fn flights_all_tracks_cache_path(cache_dir: &Path, date: NaiveDate, slice_idx: u32) -> PathBuf {
    cache_dir
        .join("flights_all")
        .join(date.to_string())
        .join(format!("{slice_idx:02}.tracks.json.gz"))
}

fn track_attempt_key(icao24: &str, first_seen: i64) -> String {
    format!("{icao24}:{first_seen}")
}

fn read_tracks_cache(path: &Path) -> Result<HashMap<String, Option<TrackEnds>>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let raw = read_gzip_cache(path)?;
    Ok(serde_json::from_slice(&raw).unwrap_or_default())
}

fn write_tracks_cache(path: &Path, map: &HashMap<String, Option<TrackEnds>>) -> Result<()> {
    let json = serde_json::to_vec(map)?;
    write_gzip_cache(path, &json)
}

/// Cache set is the mapped fleet. `--hex` only restricts ingest.
pub fn opensky_hex_sets(
    fleet_keys: &HashSet<String>,
    hex_filter: &[String],
) -> Result<(HashSet<String>, HashSet<String>)> {
    let cache_hexes = fleet_keys.clone();
    if hex_filter.is_empty() {
        return Ok((cache_hexes.clone(), cache_hexes));
    }
    for h in hex_filter.iter().filter(|h| !fleet_keys.contains(*h)) {
        warn!(hex = %h, "not in default fleet slice; skip");
    }
    let ingest_hexes: HashSet<String> = hex_filter
        .iter()
        .filter(|h| fleet_keys.contains(*h))
        .cloned()
        .collect();
    if ingest_hexes.is_empty() {
        anyhow::bail!("none of --hex values are in the mapped fleet slice");
    }
    Ok((cache_hexes, ingest_hexes))
}

pub(crate) trait OpenskyCollectApi: Send + Sync {
    fn get_flights_all(
        &self,
        begin: i64,
        end: i64,
    ) -> impl Future<Output = Result<(OpenskyOutcome<Vec<Flight>>, usize, u128)>> + Send;

    fn get_tracks(
        &self,
        icao24: &str,
        time: i64,
    ) -> impl Future<Output = Result<OpenskyOutcome<Option<TrackEnds>>>> + Send;
}

impl OpenskyCollectApi for OpenskyClient {
    fn get_flights_all(
        &self,
        begin: i64,
        end: i64,
    ) -> impl Future<Output = Result<(OpenskyOutcome<Vec<Flight>>, usize, u128)>> + Send {
        self.flights_all(begin, end)
    }

    fn get_tracks(
        &self,
        icao24: &str,
        time: i64,
    ) -> impl Future<Output = Result<OpenskyOutcome<Option<TrackEnds>>>> + Send {
        self.tracks(icao24, time)
    }
}

pub fn write_gzip_cache(path: &Path, json: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(path)?;
    let mut enc = GzEncoder::new(file, Compression::default());
    enc.write_all(json)?;
    enc.finish()?;
    Ok(())
}

pub fn read_gzip_cache(path: &Path) -> Result<Vec<u8>> {
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

    collect_opensky(opts, fleet, db, airports).await
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

fn flight_completeness(f: &Flight) -> (u8, u8, u8, i64) {
    let dep = u8::from(f.est_departure_airport.is_some());
    let arr = u8::from(f.est_arrival_airport.is_some());
    let cs = u8::from(f.callsign.is_some());
    (dep + arr, arr, cs, -f.first_seen)
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
        let track = tracks.get(&f.first_seen).cloned();
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
        anyhow::bail!("collect requires OPENSKY_CLIENT_ID/SECRET or OPENSKY_CREDENTIALS_JSON");
    };
    collect_opensky_with(&*client, &opts, fleet, db, airports).await
}

async fn collect_opensky_with<C: OpenskyCollectApi>(
    client: &C,
    opts: &CollectOptions,
    fleet: FleetQuery,
    db: JournalDb,
    airports: AirportIndex,
) -> Result<CollectReport> {
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
    let fleet_keys: HashSet<String> = by_hex.keys().cloned().collect();
    let (cache_hexes, ingest_hexes) = opensky_hex_sets(&fleet_keys, &hex_filter)?;
    let mark_complete = ingest_hexes == cache_hexes;
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
        cache_fleet = cache_hexes.len(),
        ingest = ingest_hexes.len(),
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
            let tracks_path = flights_all_tracks_cache_path(&opts.cache_dir, date, slice_idx);
            let (mut flights, from_cache) = if cache_path.exists() {
                let raw = read_gzip_cache(&cache_path)?;
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
                match client.get_flights_all(begin, end_ts).await? {
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
                        let filtered = filter_flights_to_fleet(&data, &cache_hexes);
                        let json = serde_json::to_vec(&filtered)?;
                        write_gzip_cache(&cache_path, &json)?;
                        (filtered, false)
                    }
                    (OpenskyOutcome::NotFound { credit }, ..) => {
                        info!(
                            %date,
                            slice = slice_idx,
                            remaining = ?credit.remaining,
                            "opensky flights/all 404"
                        );
                        write_gzip_cache(&cache_path, b"[]")?;
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
            flights = filter_flights_to_fleet(&flights, &ingest_hexes);
            if from_cache {
                report.days_from_cache += 1;
                info!(%date, slice = slice_idx, n = flights.len(), "flights/all slice from cache");
            }
            match ingest_filtered_opensky_slice(
                client,
                &db,
                &by_hex,
                &flights,
                &airports,
                opts.tracks_fallback,
                &fetched_at,
                &tracks_path,
                &mut report,
            )
            .await?
            {
                SliceHalt::Stop(msg) => {
                    db.set_flights_all_error(date, &msg)?;
                    return Ok(report);
                }
                SliceHalt::Continue => {}
            }
            if mark_complete {
                db.mark_flights_all_slice_ok(date, slice_idx)?;
            }
        }
        if halted {
            return Ok(report);
        }
        if mark_complete && db.flights_all_day_complete(date, FLIGHTS_ALL_SLICES_PER_DAY)? {
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
    Stop(String),
}

#[allow(clippy::too_many_arguments)]
async fn ingest_filtered_opensky_slice<C: OpenskyCollectApi>(
    client: &C,
    db: &JournalDb,
    by_hex: &HashMap<String, FleetRow>,
    flights: &[Flight],
    airports: &AirportIndex,
    tracks_fallback: bool,
    fetched_at: &str,
    tracks_cache_path: &Path,
    report: &mut CollectReport,
) -> Result<SliceHalt> {
    let mut by_icao: HashMap<String, Vec<Flight>> = HashMap::new();
    for f in flights {
        by_icao.entry(f.icao24.clone()).or_default().push(f.clone());
    }
    let mut tracks_cache = read_tracks_cache(tracks_cache_path)?;
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
                let key = track_attempt_key(&hex, f.first_seen);
                if let Some(cached) = tracks_cache.get(&key) {
                    if let Some(ends) = cached {
                        tracks.insert(f.first_seen, ends.clone());
                    }
                    continue;
                }
                report.tracks_calls += 1;
                match client.get_tracks(&hex, f.first_seen).await? {
                    OpenskyOutcome::Ok {
                        data: Some(ends), ..
                    } => {
                        tracks.insert(f.first_seen, ends.clone());
                        tracks_cache.insert(key, Some(ends));
                        write_tracks_cache(tracks_cache_path, &tracks_cache)?;
                    }
                    OpenskyOutcome::Ok { data: None, .. } | OpenskyOutcome::NotFound { .. } => {
                        tracks_cache.insert(key, None);
                        write_tracks_cache(tracks_cache_path, &tracks_cache)?;
                    }
                    OpenskyOutcome::RateLimited { credit } => {
                        let msg = format!("tracks HTTP 429 remaining={:?}", credit.remaining);
                        warn!(%hex, %msg);
                        write_tracks_cache(tracks_cache_path, &tracks_cache)?;
                        report.hexes_stopped_on_error += 1;
                        return Ok(SliceHalt::Stop(msg));
                    }
                    OpenskyOutcome::Denied { status, message }
                    | OpenskyOutcome::Other { status, message } => {
                        let msg = format!("tracks HTTP {status}: {message}");
                        warn!(%hex, %msg);
                        write_tracks_cache(tracks_cache_path, &tracks_cache)?;
                        report.hexes_stopped_on_error += 1;
                        return Ok(SliceHalt::Stop(msg));
                    }
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
    if let Some(d) = &status.flights_all_complete_max {
        println!("flights_all last complete UTC day: {d} (12/12)");
    } else {
        println!("flights_all last complete UTC day: —");
    }
    for (d, n) in &status.flights_all_incomplete {
        println!("  flights_all in progress {d}: {n}/12 slices");
    }
    for (d, err) in &status.flights_all_errors {
        println!("  flights_all error {d}: {err}");
    }
    println!("trips: {}", status.trips);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opensky::DEFAULT_MAX_FLIGHTS_CREDITS;

    #[test]
    fn cache_path_layout() {
        let s = flights_all_slice_cache_path(
            Path::new("cache"),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            7,
        );
        assert_eq!(s, PathBuf::from("cache/flights_all/2024-01-15/07.json.gz"));
        let t = flights_all_tracks_cache_path(
            Path::new("cache"),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            7,
        );
        assert_eq!(
            t,
            PathBuf::from("cache/flights_all/2024-01-15/07.tracks.json.gz")
        );
    }

    #[test]
    fn opensky_hex_sets_cache_is_full_fleet() {
        let fleet = ["abcdef".into(), "ffffff".into()].into_iter().collect();
        let (cache, ingest) = opensky_hex_sets(&fleet, &[]).unwrap();
        assert_eq!(cache.len(), 2);
        assert_eq!(ingest.len(), 2);
        let (cache, ingest) = opensky_hex_sets(&fleet, &["abcdef".into()]).unwrap();
        assert_eq!(cache.len(), 2);
        assert_eq!(ingest.len(), 1);
        assert!(ingest.contains("abcdef"));
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

    #[test]
    fn collapse_prefers_callsign_when_airports_tie() {
        let flights = crate::opensky::parse_flights(
            br#"[
              {"icao24":"abcdef","firstSeen":100,"lastSeen":200,"estDepartureAirport":"KAPA","estArrivalAirport":"KJFK"},
              {"icao24":"abcdef","firstSeen":110,"lastSeen":210,"estDepartureAirport":"KAPA","estArrivalAirport":"KJFK","callsign":"DCM1"}
            ]"#,
        )
        .unwrap();
        let kept = collapse_near_duplicate_flights(&flights);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].callsign.as_deref(), Some("DCM1"));
        assert_eq!(kept[0].first_seen, 110);
    }

    fn fleet_row(hex: &str) -> FleetRow {
        FleetRow {
            n_number: format!("N{hex}"),
            icao24: hex.to_string(),
            ticker: "AAA".into(),
            cik: None,
            company_name: None,
            make: None,
            model: None,
            registrant_name: None,
            match_method: None,
            aviation_issuer: 0,
            fleet_size: 1,
            as_of_date: None,
        }
    }

    fn test_airports() -> AirportIndex {
        let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/airports.csv");
        AirportIndex::load_csv(&p).unwrap()
    }

    fn collect_opts(
        cache: &Path,
        journal: &Path,
        date: NaiveDate,
        hex_filter: Vec<String>,
        tracks_fallback: bool,
    ) -> CollectOptions {
        CollectOptions {
            mapping_sqlite: journal.to_path_buf(),
            journal_sqlite: journal.to_path_buf(),
            cache_dir: cache.to_path_buf(),
            airports_csv: None,
            from: Some(date),
            to: Some(date),
            opensky: None,
            max_flights_credits: DEFAULT_MAX_FLIGHTS_CREDITS,
            tracks_fallback,
            hex_filter,
        }
    }

    struct FakeOpensky {
        flights_payload: Vec<Flight>,
        flights_http: std::sync::atomic::AtomicU32,
        fail_on_flights_http: Option<u32>,
        tracks: std::sync::Mutex<std::collections::VecDeque<OpenskyOutcome<Option<TrackEnds>>>>,
        tracks_http: std::sync::atomic::AtomicU32,
    }

    fn flights_credit(remaining: u32) -> crate::opensky::CreditInfo {
        crate::opensky::CreditInfo {
            remaining: Some(remaining),
            retry_after: None,
            bucket: crate::opensky::CreditBucket::Flights,
        }
    }

    fn tracks_credit() -> crate::opensky::CreditInfo {
        crate::opensky::CreditInfo {
            remaining: Some(1000),
            retry_after: Some(1),
            bucket: crate::opensky::CreditBucket::Tracks,
        }
    }

    impl OpenskyCollectApi for FakeOpensky {
        fn get_flights_all(
            &self,
            _begin: i64,
            _end: i64,
        ) -> impl Future<Output = Result<(OpenskyOutcome<Vec<Flight>>, usize, u128)>> + Send
        {
            let n = self
                .flights_http
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let fail = self.fail_on_flights_http;
            let data = if n == 1 {
                self.flights_payload.clone()
            } else {
                Vec::new()
            };
            async move {
                if fail == Some(n) {
                    return Ok((
                        OpenskyOutcome::RateLimited {
                            credit: flights_credit(0),
                        },
                        0,
                        1,
                    ));
                }
                let bytes = data.len();
                Ok((
                    OpenskyOutcome::Ok {
                        data,
                        credit: flights_credit(1000),
                    },
                    bytes,
                    1,
                ))
            }
        }

        fn get_tracks(
            &self,
            _icao24: &str,
            _time: i64,
        ) -> impl Future<Output = Result<OpenskyOutcome<Option<TrackEnds>>>> + Send {
            self.tracks_http
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let next = self.tracks.lock().unwrap().pop_front();
            async move {
                Ok(next.unwrap_or(OpenskyOutcome::Ok {
                    data: None,
                    credit: tracks_credit(),
                }))
            }
        }
    }

    #[tokio::test]
    async fn collect_opensky_429_leaves_remaining_slices_unmarked() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let journal = dir.path().join("trips.sqlite");
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let mut db = JournalDb::open(&journal).unwrap();
        let fleet = crate::fleet::FleetQuery {
            rows: vec![fleet_row("abcdef")],
            skipped_empty_icao24: 0,
            snapshot_as_of: "2026-08-31".into(),
        };
        db.replace_fleet_snapshot(&fleet.rows, &fleet.snapshot_as_of)
            .unwrap();
        let fake = FakeOpensky {
            flights_payload: Vec::new(),
            flights_http: std::sync::atomic::AtomicU32::new(0),
            fail_on_flights_http: Some(3),
            tracks: std::sync::Mutex::new(std::collections::VecDeque::new()),
            tracks_http: std::sync::atomic::AtomicU32::new(0),
        };
        let opts = collect_opts(&cache, &journal, date, Vec::new(), false);
        let r = collect_opensky_with(&fake, &opts, fleet.clone(), db, test_airports())
            .await
            .unwrap();
        assert_eq!(r.flights_calls, 3);
        assert_eq!(r.estimated_flights_credits, 90);
        let db = JournalDb::open(&journal).unwrap();
        assert_eq!(db.flights_all_slices_done(date).unwrap(), 2);
        assert!(!db.flights_all_day_complete(date, 12).unwrap());

        let r2 = collect_opensky_with(
            &fake,
            &opts,
            fleet,
            JournalDb::open(&journal).unwrap(),
            test_airports(),
        )
        .await
        .unwrap();
        assert_eq!(
            r2.flights_calls, 10,
            "two complete slices skipped; rest HTTP"
        );
        assert_eq!(
            r2.estimated_flights_credits, 300,
            "cached complete slices must not add flights credits"
        );
        let db = JournalDb::open(&journal).unwrap();
        assert!(db.flights_all_day_complete(date, 12).unwrap());
        assert_eq!(
            fake.flights_http.load(std::sync::atomic::Ordering::SeqCst),
            13
        );
    }

    #[tokio::test]
    async fn collect_opensky_hex_filter_does_not_shrink_slice_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let journal = dir.path().join("trips.sqlite");
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let flights = crate::opensky::parse_flights(
            br#"[
              {"icao24":"abcdef","firstSeen":1705276800,"lastSeen":1705280400,"estDepartureAirport":"KAPA","estArrivalAirport":"KJFK"},
              {"icao24":"ffffff","firstSeen":1705276900,"lastSeen":1705280500,"estDepartureAirport":"KAPA","estArrivalAirport":"KJFK"}
            ]"#,
        )
        .unwrap();
        let mut db = JournalDb::open(&journal).unwrap();
        let fleet = crate::fleet::FleetQuery {
            rows: vec![fleet_row("abcdef"), fleet_row("ffffff")],
            skipped_empty_icao24: 0,
            snapshot_as_of: "2026-08-31".into(),
        };
        db.replace_fleet_snapshot(&fleet.rows, &fleet.snapshot_as_of)
            .unwrap();
        let fake = FakeOpensky {
            flights_payload: flights,
            flights_http: std::sync::atomic::AtomicU32::new(0),
            fail_on_flights_http: None,
            tracks: std::sync::Mutex::new(std::collections::VecDeque::new()),
            tracks_http: std::sync::atomic::AtomicU32::new(0),
        };
        let filtered = collect_opts(&cache, &journal, date, vec!["abcdef".into()], false);
        let r = collect_opensky_with(&fake, &filtered, fleet.clone(), db, test_airports())
            .await
            .unwrap();
        assert_eq!(r.flights_calls, 12);
        assert_eq!(r.trips_upserted, 1);
        let raw = read_gzip_cache(&flights_all_slice_cache_path(&cache, date, 0)).unwrap();
        let cached = crate::opensky::parse_flights(&raw).unwrap();
        assert_eq!(cached.len(), 2, "--hex must not shrink the slice cache");
        let db = JournalDb::open(&journal).unwrap();
        assert!(
            !db.flights_all_day_complete(date, 12).unwrap(),
            "--hex run must not mark the UTC day complete"
        );
        assert_eq!(db.trip_count().unwrap(), 1);

        let full = collect_opts(&cache, &journal, date, Vec::new(), false);
        let r2 = collect_opensky_with(
            &fake,
            &full,
            fleet,
            JournalDb::open(&journal).unwrap(),
            test_airports(),
        )
        .await
        .unwrap();
        assert_eq!(
            r2.flights_calls, 0,
            "cache hit must not add flights credits"
        );
        assert_eq!(r2.estimated_flights_credits, 0);
        assert_eq!(r2.days_from_cache, 12);
        assert_eq!(r2.trips_upserted, 2);
        let db = JournalDb::open(&journal).unwrap();
        assert!(db.flights_all_day_complete(date, 12).unwrap());
        assert_eq!(db.trip_count().unwrap(), 2);
        assert_eq!(
            fake.flights_http.load(std::sync::atomic::Ordering::SeqCst),
            12
        );
    }

    #[tokio::test]
    async fn collect_opensky_tracks_cache_skips_http_on_resume() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let journal = dir.path().join("trips.sqlite");
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let flights = crate::opensky::parse_flights(
            br#"[
              {"icao24":"abcdef","firstSeen":1705276800,"lastSeen":1705280400},
              {"icao24":"abcdef","firstSeen":1705290000,"lastSeen":1705293600}
            ]"#,
        )
        .unwrap();
        let ends = TrackEnds {
            dep_lat: 39.57,
            dep_lon: -104.67,
            arr_lat: 40.64,
            arr_lon: -73.78,
            callsign: None,
        };
        let mut script = std::collections::VecDeque::new();
        script.push_back(OpenskyOutcome::Ok {
            data: Some(ends.clone()),
            credit: tracks_credit(),
        });
        script.push_back(OpenskyOutcome::RateLimited {
            credit: tracks_credit(),
        });
        script.push_back(OpenskyOutcome::Ok {
            data: Some(ends),
            credit: tracks_credit(),
        });
        let mut db = JournalDb::open(&journal).unwrap();
        let fleet = crate::fleet::FleetQuery {
            rows: vec![fleet_row("abcdef")],
            skipped_empty_icao24: 0,
            snapshot_as_of: "2026-08-31".into(),
        };
        db.replace_fleet_snapshot(&fleet.rows, &fleet.snapshot_as_of)
            .unwrap();
        let fake = FakeOpensky {
            flights_payload: flights,
            flights_http: std::sync::atomic::AtomicU32::new(0),
            fail_on_flights_http: None,
            tracks: std::sync::Mutex::new(script),
            tracks_http: std::sync::atomic::AtomicU32::new(0),
        };
        let opts = collect_opts(&cache, &journal, date, Vec::new(), true);
        let r = collect_opensky_with(&fake, &opts, fleet.clone(), db, AirportIndex::empty())
            .await
            .unwrap();
        assert_eq!(r.tracks_calls, 2);
        assert_eq!(r.flights_calls, 1);
        assert!(flights_all_tracks_cache_path(&cache, date, 0).exists());
        let db = JournalDb::open(&journal).unwrap();
        assert!(!db.flights_all_slice_complete(date, 0).unwrap());

        let r2 = collect_opensky_with(
            &fake,
            &opts,
            fleet,
            JournalDb::open(&journal).unwrap(),
            AirportIndex::empty(),
        )
        .await
        .unwrap();
        assert_eq!(
            r2.flights_calls, 11,
            "slice 0 from cache; remaining slices HTTP"
        );
        assert_eq!(r2.days_from_cache, 1);
        assert_eq!(
            r2.tracks_calls, 1,
            "cached firstSeen must not re-call /tracks"
        );
        assert_eq!(r2.trips_upserted, 2);
        assert_eq!(
            fake.tracks_http.load(std::sync::atomic::Ordering::SeqCst),
            3
        );
        assert_eq!(
            fake.flights_http.load(std::sync::atomic::Ordering::SeqCst),
            12
        );
    }

    #[tokio::test]
    async fn collect_opensky_tracks_other_does_not_mark_slice_complete() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let journal = dir.path().join("trips.sqlite");
        let date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let flights = crate::opensky::parse_flights(
            br#"[{"icao24":"abcdef","firstSeen":1705276800,"lastSeen":1705280400}]"#,
        )
        .unwrap();
        let mut script = std::collections::VecDeque::new();
        script.push_back(OpenskyOutcome::Other {
            status: 0,
            message: "timeout".into(),
        });
        let mut db = JournalDb::open(&journal).unwrap();
        let fleet = crate::fleet::FleetQuery {
            rows: vec![fleet_row("abcdef")],
            skipped_empty_icao24: 0,
            snapshot_as_of: "2026-08-31".into(),
        };
        db.replace_fleet_snapshot(&fleet.rows, &fleet.snapshot_as_of)
            .unwrap();
        let fake = FakeOpensky {
            flights_payload: flights,
            flights_http: std::sync::atomic::AtomicU32::new(0),
            fail_on_flights_http: None,
            tracks: std::sync::Mutex::new(script),
            tracks_http: std::sync::atomic::AtomicU32::new(0),
        };
        let opts = collect_opts(&cache, &journal, date, Vec::new(), true);
        let r = collect_opensky_with(&fake, &opts, fleet, db, AirportIndex::empty())
            .await
            .unwrap();
        assert_eq!(r.tracks_calls, 1);
        assert_eq!(r.flights_calls, 1);
        assert_eq!(r.hexes_stopped_on_error, 1);
        let db = JournalDb::open(&journal).unwrap();
        assert!(
            !db.flights_all_slice_complete(date, 0).unwrap(),
            "tracks Other must not mark the slice complete"
        );
        assert!(!db.flights_all_day_complete(date, 12).unwrap());
        assert!(
            db.get_cursor("abcdef").unwrap().is_none(),
            "tracks errors must not write leftover fetch_cursor"
        );
        let st = db.status(&journal).unwrap();
        assert_eq!(st.flights_all_errors.len(), 1);
        assert!(
            st.flights_all_errors[0]
                .1
                .contains("tracks HTTP 0: timeout"),
            "{:?}",
            st.flights_all_errors
        );
    }
}

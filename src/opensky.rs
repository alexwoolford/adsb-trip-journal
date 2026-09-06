//! OpenSky Network REST API: OAuth2 client, states / flights / tracks.

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::airports::AirportIndex;
use crate::fleet::FleetRow;
use crate::store::{utc_iso, TripRow, TripSource};

const TOKEN_URL: &str =
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token";
const API_ROOT: &str = "https://opensky-network.org/api";
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(30);
/// Observed `/flights/aircraft` cost for a historical UTC-day (or 6h) query.
/// Live lookback may still be 4; yesterday’s batch has billed **30** in this account.
pub const FLIGHTS_CALL_CREDITS: u32 = 30;
pub const TRACKS_CALL_CREDITS: u32 = 4;
/// Observed `/states/all?icao24=…` cost (hex filter, not serial-only).
/// Serial-only `/states/all` is 1; icao24/bbox billed **4** on this account
/// (2026-09-02 watch: 5 chunks of 80 → remaining dropped 20/poll).
pub const STATES_CALL_CREDITS: u32 = 4;
/// Laptop/dev collect cap: two UTC days of `/flights/all` at 30/slice is 720.
/// Production host env is **3600** (repair budget; ~10 days/run, ~400 slack).
/// `install.sh` does not overwrite an existing env file.
pub const DEFAULT_MAX_FLIGHTS_CREDITS: u32 = 800;
pub const HEX_CHUNK: usize = 80;
/// `/flights/all` max window. Twelve adjacent slices cover one UTC day.
pub const FLIGHTS_ALL_SLICES_PER_DAY: u32 = 12;
pub const FLIGHTS_ALL_SLICE_SECS: i64 = 7_200;
/// Measured `/flights/all` 2h historical slice: **30** flights-credits
/// (2026-09-03 12:00–14:00 UTC; HTTP 200, remaining consistent with 30 after
/// that day's `/flights/aircraft` collect). Docs' "Live / < 24 h → 4" does not apply.
pub const FLIGHTS_ALL_SLICE_CREDITS: u32 = 30;
const STATES_HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const FLIGHTS_ALL_HTTP_TIMEOUT: Duration = Duration::from_secs(180);
/// Rolling repair window: never-started or incomplete `/flights/all` days
/// inside this many UTC days before yesterday. Watch `seen_airborne` is not
/// used. Incomplete slice rows older than the window still resume.
pub const FLIGHTS_ALL_LOOKBACK_DAYS: u64 = 90;

/// How many `/states/all` requests a fleet of `n_hexes` needs at [`HEX_CHUNK`].
pub fn states_request_count(n_hexes: usize) -> u32 {
    if n_hexes == 0 {
        0
    } else {
        n_hexes.div_ceil(HEX_CHUNK) as u32
    }
}

/// Estimated states-bucket spend for one watch poll of `n_hexes`.
pub fn estimated_states_credits(n_hexes: usize) -> u32 {
    states_request_count(n_hexes).saturating_mul(STATES_CALL_CREDITS)
}

#[derive(Debug, Clone)]
pub struct OpenskyConfig {
    pub client_id: String,
    pub client_secret: String,
}

impl OpenskyConfig {
    pub fn from_env() -> Result<Option<Self>> {
        if let (Some(id), Some(secret)) = (
            empty_to_none(std::env::var("OPENSKY_CLIENT_ID").ok()),
            empty_to_none(std::env::var("OPENSKY_CLIENT_SECRET").ok()),
        ) {
            return Ok(Some(Self {
                client_id: id,
                client_secret: secret,
            }));
        }
        if let Some(path) = empty_to_none(std::env::var("OPENSKY_CREDENTIALS_JSON").ok()) {
            return Ok(Some(Self::from_json_file(Path::new(&path))?));
        }
        Ok(None)
    }

    pub fn from_json_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read OpenSky credentials {}", path.display()))?;
        Self::from_json_str(&raw)
    }

    pub fn from_json_str(s: &str) -> Result<Self> {
        let v: Value = serde_json::from_str(s).context("parse OpenSky credentials JSON")?;
        let id = v
            .get("clientId")
            .or_else(|| v.get("client_id"))
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow::anyhow!("credentials JSON missing clientId"))?;
        let secret = v
            .get("clientSecret")
            .or_else(|| v.get("client_secret"))
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow::anyhow!("credentials JSON missing clientSecret"))?;
        Ok(Self {
            client_id: id.to_string(),
            client_secret: secret.to_string(),
        })
    }
}

fn empty_to_none(s: Option<String>) -> Option<String> {
    s.and_then(|x| {
        let t = x.trim().to_string();
        if t.is_empty() {
            None
        } else {
            Some(t)
        }
    })
}

struct CachedToken {
    access: String,
    expires_at: Instant,
}

pub struct OpenskyClient {
    http: reqwest::Client,
    http_long: reqwest::Client,
    cfg: OpenskyConfig,
    token: Mutex<Option<CachedToken>>,
}

#[derive(Debug, Clone, Copy)]
pub enum CreditBucket {
    States,
    Flights,
    Tracks,
}

#[derive(Debug, Clone)]
pub struct CreditInfo {
    pub remaining: Option<u32>,
    pub retry_after: Option<u64>,
    pub bucket: CreditBucket,
}

#[derive(Debug, Clone)]
pub enum OpenskyOutcome<T> {
    Ok { data: T, credit: CreditInfo },
    NotFound { credit: CreditInfo },
    RateLimited { credit: CreditInfo },
    Denied { status: u16, message: String },
    Other { status: u16, message: String },
}

impl<T> OpenskyOutcome<T> {
    pub fn advances_cursor(&self) -> bool {
        matches!(self, Self::Ok { .. } | Self::NotFound { .. })
    }

    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::Denied { status, message } => Some(format!("HTTP {status}: {message}")),
            Self::RateLimited { credit } => Some(format!(
                "HTTP 429 retry-after={}s remaining={:?}",
                credit.retry_after.unwrap_or(0),
                credit.remaining
            )),
            Self::Other { status, message } => Some(format!("HTTP {status}: {message}")),
            Self::Ok { .. } | Self::NotFound { .. } => None,
        }
    }

    pub fn remaining(&self) -> Option<u32> {
        match self {
            Self::Ok { credit, .. } | Self::NotFound { credit } | Self::RateLimited { credit } => {
                credit.remaining
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
pub struct Flight {
    pub icao24: String,
    #[serde(rename = "firstSeen")]
    pub first_seen: i64,
    #[serde(rename = "lastSeen", default)]
    pub last_seen: Option<i64>,
    #[serde(rename = "estDepartureAirport", default)]
    pub est_departure_airport: Option<String>,
    #[serde(rename = "estArrivalAirport", default)]
    pub est_arrival_airport: Option<String>,
    /// Transponder label (often N-number, sometimes DCM/FFL). Not identity.
    #[serde(default)]
    pub callsign: Option<String>,
    #[serde(rename = "estDepartureAirportHorizDistance", default)]
    pub est_departure_airport_horiz_distance: Option<i64>,
    #[serde(rename = "estDepartureAirportVertDistance", default)]
    pub est_departure_airport_vert_distance: Option<i64>,
    #[serde(rename = "estArrivalAirportHorizDistance", default)]
    pub est_arrival_airport_horiz_distance: Option<i64>,
    #[serde(rename = "estArrivalAirportVertDistance", default)]
    pub est_arrival_airport_vert_distance: Option<i64>,
    #[serde(rename = "departureAirportCandidatesCount", default)]
    pub departure_airport_candidates_count: Option<i64>,
    #[serde(rename = "arrivalAirportCandidatesCount", default)]
    pub arrival_airport_candidates_count: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TrackEnds {
    pub dep_lat: f64,
    pub dep_lon: f64,
    pub arr_lat: f64,
    pub arr_lon: f64,
    /// OpenSky track `callsign` (docs sometimes spell `calllsign`). Fill-in only.
    #[serde(default)]
    pub callsign: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenskyFlightsAllProbeReport {
    pub date: String,
    pub start_hour: u32,
    pub token_ok: bool,
    pub flights_status: Option<u16>,
    pub flights_remaining: Option<u32>,
    pub flights_remaining_before: Option<u32>,
    pub flights_credits_spent: Option<u32>,
    pub flights_begin: Option<i64>,
    pub flights_end: Option<i64>,
    pub flights_window: String,
    pub body_bytes: usize,
    pub elapsed_ms: u128,
    pub raw_count: usize,
    pub fleet_count: usize,
    pub fleet_hexes: usize,
    pub journal_trips_in_window: usize,
    pub matched_keys: usize,
    pub extra_vs_journal: usize,
    pub missing_from_journal: usize,
    pub abort: bool,
    pub note: String,
}

#[derive(Debug, Clone, Default)]
pub struct FlightsAllOverlap {
    pub fleet_count: usize,
    pub fleet_hexes: usize,
    pub journal_trips_in_window: usize,
    pub matched_keys: usize,
    pub extra_vs_journal: usize,
    pub missing_from_journal: usize,
}

impl OpenskyClient {
    pub fn new(cfg: OpenskyConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("adsb-trip-journal/0.1 (OpenSky REST)")
            .timeout(STATES_HTTP_TIMEOUT)
            .build()
            .context("build OpenSky HTTP client")?;
        let http_long = reqwest::Client::builder()
            .user_agent("adsb-trip-journal/0.1 (OpenSky REST)")
            .timeout(FLIGHTS_ALL_HTTP_TIMEOUT)
            .build()
            .context("build OpenSky long-timeout HTTP client")?;
        Ok(Self {
            http,
            http_long,
            cfg,
            token: Mutex::new(None),
        })
    }

    async fn bearer(&self) -> Result<String> {
        {
            let guard = self.token.lock().await;
            if let Some(t) = guard.as_ref() {
                if Instant::now() + TOKEN_REFRESH_MARGIN < t.expires_at {
                    return Ok(t.access.clone());
                }
            }
        }
        let resp = self
            .http
            .post(TOKEN_URL)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.cfg.client_id.as_str()),
                ("client_secret", self.cfg.client_secret.as_str()),
            ])
            .send()
            .await
            .context("OpenSky token request")?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("OpenSky token HTTP {status}: {}", truncate(&body, 200));
        }
        let tok: TokenResponse = serde_json::from_str(&body).context("parse OpenSky token JSON")?;
        let expires = Duration::from_secs(tok.expires_in.unwrap_or(1800));
        let access = tok.access_token;
        {
            let mut guard = self.token.lock().await;
            *guard = Some(CachedToken {
                access: access.clone(),
                expires_at: Instant::now() + expires,
            });
        }
        Ok(access)
    }

    async fn authed_get(
        &self,
        url: reqwest::Url,
        bucket: CreditBucket,
    ) -> Result<(StatusCode, HeaderMap, Vec<u8>)> {
        self.authed_get_on(&self.http, url, bucket).await
    }

    async fn authed_get_long(
        &self,
        url: reqwest::Url,
        bucket: CreditBucket,
    ) -> Result<(StatusCode, HeaderMap, Vec<u8>)> {
        self.authed_get_on(&self.http_long, url, bucket).await
    }

    async fn authed_get_on(
        &self,
        http: &reqwest::Client,
        url: reqwest::Url,
        _bucket: CreditBucket,
    ) -> Result<(StatusCode, HeaderMap, Vec<u8>)> {
        let token = self.bearer().await?;
        let resp = http
            .get(url)
            .header(
                reqwest::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}"))?,
            )
            .send()
            .await?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.bytes().await.unwrap_or_default().to_vec();
        Ok((status, headers, bytes))
    }

    pub async fn states_fleet(&self, hexes: &[String]) -> Result<OpenskyOutcome<Vec<String>>> {
        if hexes.is_empty() {
            return Ok(OpenskyOutcome::Ok {
                data: Vec::new(),
                credit: CreditInfo {
                    remaining: None,
                    retry_after: None,
                    bucket: CreditBucket::States,
                },
            });
        }
        let mut all = Vec::new();
        let mut last_credit = CreditInfo {
            remaining: None,
            retry_after: None,
            bucket: CreditBucket::States,
        };
        for chunk in hexes.chunks(HEX_CHUNK) {
            let mut url = reqwest::Url::parse(&format!("{API_ROOT}/states/all"))?;
            {
                let mut q = url.query_pairs_mut();
                for h in chunk {
                    q.append_pair("icao24", &h.to_ascii_lowercase());
                }
            }
            let (status, headers, body) = self.authed_get(url, CreditBucket::States).await?;
            last_credit = credit_from_headers(&headers, CreditBucket::States);
            match classify(status, body, last_credit.clone())? {
                OpenskyOutcome::Ok { data, credit } => {
                    last_credit = credit;
                    all.extend(parse_states_hexes(&data)?);
                }
                OpenskyOutcome::NotFound { credit } => last_credit = credit,
                OpenskyOutcome::RateLimited { credit } => {
                    return Ok(OpenskyOutcome::RateLimited { credit });
                }
                OpenskyOutcome::Denied { status, message } => {
                    return Ok(OpenskyOutcome::Denied { status, message });
                }
                OpenskyOutcome::Other { status, message } => {
                    return Ok(OpenskyOutcome::Other { status, message });
                }
            }
        }
        all.sort();
        all.dedup();
        Ok(OpenskyOutcome::Ok {
            data: all,
            credit: last_credit,
        })
    }

    pub async fn flights_all(
        &self,
        begin: i64,
        end: i64,
    ) -> Result<(OpenskyOutcome<Vec<Flight>>, usize, u128)> {
        anyhow::ensure!(end > begin, "flights/all end must be greater than begin");
        anyhow::ensure!(
            end - begin <= FLIGHTS_ALL_SLICE_SECS,
            "flights/all window must be ≤ 2 hours"
        );
        let mut url = reqwest::Url::parse(&format!("{API_ROOT}/flights/all"))?;
        url.query_pairs_mut()
            .append_pair("begin", &begin.to_string())
            .append_pair("end", &end.to_string());
        let started = Instant::now();
        let (status, headers, body) = match self.authed_get_long(url, CreditBucket::Flights).await {
            Ok(t) => t,
            Err(e) => {
                let elapsed_ms = started.elapsed().as_millis();
                let timeout = e.to_string().to_ascii_lowercase().contains("timed out")
                    || e.to_string().to_ascii_lowercase().contains("timeout");
                let message = if timeout {
                    format!("timeout after {elapsed_ms}ms: {e}")
                } else {
                    format!("transport: {e}")
                };
                return Ok((OpenskyOutcome::Other { status: 0, message }, 0, elapsed_ms));
            }
        };
        let elapsed_ms = started.elapsed().as_millis();
        let body_bytes = body.len();
        let credit = credit_from_headers(&headers, CreditBucket::Flights);
        let outcome = match classify(status, body, credit)? {
            OpenskyOutcome::Ok { data, credit } => match parse_flights(&data) {
                Ok(flights) => OpenskyOutcome::Ok {
                    data: flights,
                    credit,
                },
                Err(e) => OpenskyOutcome::Other {
                    status: status.as_u16(),
                    message: format!("unparseable flights/all body ({body_bytes} bytes): {e}"),
                },
            },
            OpenskyOutcome::NotFound { credit } => OpenskyOutcome::NotFound { credit },
            OpenskyOutcome::RateLimited { credit } => OpenskyOutcome::RateLimited { credit },
            OpenskyOutcome::Denied { status, message } => {
                OpenskyOutcome::Denied { status, message }
            }
            OpenskyOutcome::Other { status, message } => OpenskyOutcome::Other { status, message },
        };
        Ok((outcome, body_bytes, elapsed_ms))
    }

    pub async fn probe_flights_all(
        &self,
        date: NaiveDate,
        start_hour: u32,
        flights_remaining_before: Option<u32>,
        fleet: &HashSet<String>,
        journal_keys: &HashSet<(String, i64)>,
    ) -> Result<OpenskyFlightsAllProbeReport> {
        let (begin, end) = utc_hours_window(date, start_hour, 2);
        let window = format!("2h from {start_hour:02}:00 UTC");
        if let Err(e) = self.bearer().await {
            return Ok(OpenskyFlightsAllProbeReport {
                date: date.to_string(),
                start_hour,
                token_ok: false,
                flights_status: None,
                flights_remaining: None,
                flights_remaining_before,
                flights_credits_spent: None,
                flights_begin: Some(begin),
                flights_end: Some(end),
                flights_window: window,
                body_bytes: 0,
                elapsed_ms: 0,
                raw_count: 0,
                fleet_count: 0,
                fleet_hexes: 0,
                journal_trips_in_window: journal_keys.len(),
                matched_keys: 0,
                extra_vs_journal: 0,
                missing_from_journal: journal_keys.len(),
                abort: true,
                note: format!("token failed: {e}"),
            });
        }
        let (outcome, body_bytes, elapsed_ms) = self.flights_all(begin, end).await?;
        let mut abort = false;
        let mut note = String::new();
        let (flights_status, flights_remaining, flights) = match outcome {
            OpenskyOutcome::Ok { data, credit } => (Some(200u16), credit.remaining, data),
            OpenskyOutcome::NotFound { credit } => {
                note.push_str("flights/all 404 (empty interval); ");
                (Some(404), credit.remaining, Vec::new())
            }
            OpenskyOutcome::RateLimited { credit } => {
                abort = true;
                note.push_str("flights/all 429; ");
                (Some(429), credit.remaining, Vec::new())
            }
            OpenskyOutcome::Denied { status, message } => {
                abort = true;
                note.push_str(&format!("flights/all {status} {message}; "));
                (Some(status), None, Vec::new())
            }
            OpenskyOutcome::Other { status, message } => {
                abort = true;
                note.push_str(&format!("flights/all {status} {message}; "));
                (Some(status), None, Vec::new())
            }
        };
        let overlap = overlap_fleet_journal(&flights, fleet, journal_keys);
        let flights_credits_spent = match (flights_remaining_before, flights_remaining) {
            (Some(before), Some(after)) if before >= after => {
                let delta = before - after;
                // Ignore leftover remaining from a different probe/day.
                if delta > 60 {
                    None
                } else {
                    Some(delta)
                }
            }
            _ => None,
        };
        if elapsed_ms >= FLIGHTS_ALL_HTTP_TIMEOUT.as_millis().saturating_sub(1_000)
            && flights_status == Some(0)
        {
            abort = true;
        }
        if flights_credits_spent.is_none()
            && flights_remaining_before.is_some()
            && flights_remaining.is_some()
        {
            note.push_str("remaining_before ignored (stale or implausible delta); ");
        }
        if note.is_empty() {
            note = if abort { "abort".into() } else { "ok".into() };
        }
        Ok(OpenskyFlightsAllProbeReport {
            date: date.to_string(),
            start_hour,
            token_ok: true,
            flights_status,
            flights_remaining,
            flights_remaining_before,
            flights_credits_spent,
            flights_begin: Some(begin),
            flights_end: Some(end),
            flights_window: window,
            body_bytes,
            elapsed_ms,
            raw_count: flights.len(),
            fleet_count: overlap.fleet_count,
            fleet_hexes: overlap.fleet_hexes,
            journal_trips_in_window: overlap.journal_trips_in_window,
            matched_keys: overlap.matched_keys,
            extra_vs_journal: overlap.extra_vs_journal,
            missing_from_journal: overlap.missing_from_journal,
            abort,
            note,
        })
    }

    pub async fn tracks(
        &self,
        icao24: &str,
        time: i64,
    ) -> Result<OpenskyOutcome<Option<TrackEnds>>> {
        let mut url = reqwest::Url::parse(&format!("{API_ROOT}/tracks/all"))?;
        url.query_pairs_mut()
            .append_pair("icao24", &icao24.to_ascii_lowercase())
            .append_pair("time", &time.to_string());
        let (status, headers, body) = self.authed_get(url, CreditBucket::Tracks).await?;
        let credit = credit_from_headers(&headers, CreditBucket::Tracks);
        match classify(status, body, credit)? {
            OpenskyOutcome::Ok { data, credit } => Ok(OpenskyOutcome::Ok {
                data: parse_track_endpoints(&data),
                credit,
            }),
            OpenskyOutcome::NotFound { credit } => Ok(OpenskyOutcome::NotFound { credit }),
            OpenskyOutcome::RateLimited { credit } => Ok(OpenskyOutcome::RateLimited { credit }),
            OpenskyOutcome::Denied { status, message } => {
                Ok(OpenskyOutcome::Denied { status, message })
            }
            OpenskyOutcome::Other { status, message } => {
                Ok(OpenskyOutcome::Other { status, message })
            }
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

fn classify(
    status: StatusCode,
    body: Vec<u8>,
    credit: CreditInfo,
) -> Result<OpenskyOutcome<Vec<u8>>> {
    let message = String::from_utf8_lossy(&body)
        .chars()
        .take(200)
        .collect::<String>();
    Ok(match status.as_u16() {
        200..=299 => OpenskyOutcome::Ok { data: body, credit },
        404 => OpenskyOutcome::NotFound { credit },
        401..=403 => OpenskyOutcome::Denied {
            status: status.as_u16(),
            message,
        },
        429 => OpenskyOutcome::RateLimited { credit },
        other => OpenskyOutcome::Other {
            status: other,
            message,
        },
    })
}

fn credit_from_headers(headers: &HeaderMap, bucket: CreditBucket) -> CreditInfo {
    let remaining = headers
        .get("x-rate-limit-remaining")
        .or_else(|| headers.get("X-Rate-Limit-Remaining"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());
    let retry_after = headers
        .get("x-rate-limit-retry-after-seconds")
        .or_else(|| headers.get("X-Rate-Limit-Retry-After-Seconds"))
        .or_else(|| headers.get(reqwest::header::RETRY_AFTER))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());
    CreditInfo {
        remaining,
        retry_after,
        bucket,
    }
}

/// UTC `[00:00:00, 23:59:59]` (86399s, same calendar day).
pub fn utc_day_window(date: NaiveDate) -> (i64, i64) {
    let start = date
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .and_utc()
        .timestamp();
    (start, start + 86_399)
}

/// `hours` starting at `start_hour` UTC on `date` (inclusive last second).
pub fn utc_hours_window(date: NaiveDate, start_hour: u32, hours: u32) -> (i64, i64) {
    let start = date
        .and_time(NaiveTime::from_hms_opt(start_hour, 0, 0).unwrap())
        .and_utc()
        .timestamp();
    (start, start + i64::from(hours) * 3_600 - 1)
}

/// Twelve adjacent 2-hour windows covering `[00:00:00, 23:59:59]` UTC.
pub fn utc_day_two_hour_slices(date: NaiveDate) -> Vec<(i64, i64)> {
    let (day_begin, day_end) = utc_day_window(date);
    (0..FLIGHTS_ALL_SLICES_PER_DAY)
        .map(|i| {
            let begin = day_begin + i64::from(i) * FLIGHTS_ALL_SLICE_SECS;
            let end = (begin + FLIGHTS_ALL_SLICE_SECS - 1).min(day_end);
            (begin, end)
        })
        .collect()
}

pub fn filter_flights_to_fleet(flights: &[Flight], fleet: &HashSet<String>) -> Vec<Flight> {
    flights
        .iter()
        .filter(|f| fleet.contains(&f.icao24))
        .cloned()
        .collect()
}

pub fn overlap_fleet_journal(
    flights: &[Flight],
    fleet: &HashSet<String>,
    journal_keys: &HashSet<(String, i64)>,
) -> FlightsAllOverlap {
    let fleet_flights = filter_flights_to_fleet(flights, fleet);
    let api_keys: HashSet<(String, i64)> = fleet_flights
        .iter()
        .map(|f| (f.icao24.clone(), f.first_seen))
        .collect();
    let matched = api_keys.intersection(journal_keys).count();
    let fleet_hexes = fleet_flights
        .iter()
        .map(|f| f.icao24.as_str())
        .collect::<HashSet<_>>()
        .len();
    FlightsAllOverlap {
        fleet_count: fleet_flights.len(),
        fleet_hexes,
        journal_trips_in_window: journal_keys.len(),
        matched_keys: matched,
        extra_vs_journal: api_keys.difference(journal_keys).count(),
        missing_from_journal: journal_keys.difference(&api_keys).count(),
    }
}

fn normalize_ident(value: &mut Option<String>) {
    if let Some(ref mut a) = value {
        *a = a.trim().to_ascii_uppercase();
        if a.is_empty() {
            *value = None;
        }
    }
}

fn normalize_callsign(raw: Option<&str>) -> Option<String> {
    let t = raw?.trim().to_ascii_uppercase();
    if t.is_empty() {
        None
    } else {
        Some(t)
    }
}

pub fn parse_flights(bytes: &[u8]) -> Result<Vec<Flight>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let raw: Vec<Flight> = serde_json::from_slice(bytes).context("parse OpenSky flights JSON")?;
    Ok(raw
        .into_iter()
        .map(|mut f| {
            f.icao24 = f.icao24.to_ascii_lowercase();
            normalize_ident(&mut f.est_departure_airport);
            normalize_ident(&mut f.est_arrival_airport);
            f.callsign = normalize_callsign(f.callsign.as_deref());
            f
        })
        .collect())
}

pub fn parse_states_hexes(bytes: &[u8]) -> Result<Vec<String>> {
    let v: Value = serde_json::from_slice(bytes).context("parse OpenSky states JSON")?;
    let Some(arr) = v.get("states").and_then(|s| s.as_array()) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for row in arr {
        let Some(cells) = row.as_array() else {
            continue;
        };
        let Some(hex) = cells.first().and_then(|x| x.as_str()) else {
            continue;
        };
        out.push(hex.to_ascii_lowercase());
    }
    Ok(out)
}

pub fn parse_track_endpoints(bytes: &[u8]) -> Option<TrackEnds> {
    let v: Value = serde_json::from_slice(bytes).ok()?;
    let path = v.get("path")?.as_array()?;
    let mut coords: Vec<(f64, f64)> = Vec::new();
    for pt in path {
        let Some(cells) = pt.as_array() else {
            continue;
        };
        if cells.len() < 3 {
            continue;
        }
        let Some(lat) = cells[1].as_f64() else {
            continue;
        };
        let Some(lon) = cells[2].as_f64() else {
            continue;
        };
        if lat.abs() <= 90.0 && lon.abs() <= 180.0 {
            coords.push((lat, lon));
        }
    }
    let (dep_lat, dep_lon) = *coords.first()?;
    let (arr_lat, arr_lon) = *coords.last()?;
    let callsign = v
        .get("callsign")
        .or_else(|| v.get("calllsign"))
        .and_then(|x| x.as_str())
        .and_then(|s| normalize_callsign(Some(s)));
    Some(TrackEnds {
        dep_lat,
        dep_lon,
        arr_lat,
        arr_lon,
        callsign,
    })
}

pub fn flight_to_trip(
    flight: &Flight,
    row: &FleetRow,
    airports: &AirportIndex,
    track: Option<TrackEnds>,
    fetched_at: &str,
    source: TripSource,
) -> Option<TripRow> {
    let dep_ident = flight
        .est_departure_airport
        .as_deref()
        .filter(|s| !s.is_empty());
    let arr_ident = flight
        .est_arrival_airport
        .as_deref()
        .filter(|s| !s.is_empty());
    let dep_ap = dep_ident.and_then(|id| airports.by_ident(id));
    let arr_ap = arr_ident.and_then(|id| airports.by_ident(id));

    let mut dep_lat = dep_ap.map(|a| a.lat);
    let mut dep_lon = dep_ap.map(|a| a.lon);
    let mut arr_lat = arr_ap.map(|a| a.lat);
    let mut arr_lon = arr_ap.map(|a| a.lon);

    if let Some(t) = track.as_ref() {
        if dep_lat.is_none() {
            dep_lat = Some(t.dep_lat);
            dep_lon = Some(t.dep_lon);
        }
        if arr_lat.is_none() {
            arr_lat = Some(t.arr_lat);
            arr_lon = Some(t.arr_lon);
        }
    }

    let (dep_lat, dep_lon) = (dep_lat?, dep_lon?);

    let dep_ts = Utc
        .timestamp_opt(flight.first_seen, 0)
        .single()
        .map(utc_iso)?;
    let arr_ts = flight
        .last_seen
        .and_then(|t| Utc.timestamp_opt(t, 0).single())
        .map(utc_iso);

    let dep_place = dep_ident
        .map(|s| s.to_string())
        .unwrap_or_else(|| crate::airports::format_latlon(dep_lat, dep_lon));
    let arr_place = arr_ident.map(|s| s.to_string()).or_else(|| {
        arr_lat
            .zip(arr_lon)
            .map(|(la, lo)| crate::airports::format_latlon(la, lo))
    });

    let callsign = flight
        .callsign
        .clone()
        .or_else(|| track.as_ref().and_then(|t| t.callsign.clone()));

    Some(TripRow {
        icao24: row.icao24.clone(),
        dep_ts,
        arr_ts,
        n_number: row.n_number.clone(),
        ticker: row.ticker.clone(),
        cik: row.cik.clone(),
        dep_lat: Some(dep_lat),
        dep_lon: Some(dep_lon),
        arr_lat,
        arr_lon,
        dep_airport: dep_ident
            .filter(|_| dep_ap.is_some())
            .map(|s| s.to_string()),
        arr_airport: arr_ident
            .filter(|_| arr_ap.is_some())
            .map(|s| s.to_string()),
        dep_place: Some(dep_place),
        arr_place,
        source: source.as_str().to_string(),
        fetched_at: fetched_at.to_string(),
        callsign,
        dep_airport_horiz_m: flight.est_departure_airport_horiz_distance,
        dep_airport_vert_m: flight.est_departure_airport_vert_distance,
        arr_airport_horiz_m: flight.est_arrival_airport_horiz_distance,
        arr_airport_vert_m: flight.est_arrival_airport_vert_distance,
        dep_airport_candidates: flight.departure_airport_candidates_count,
        arr_airport_candidates: flight.arrival_airport_candidates_count,
    })
}

fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn credentials_accept_camel_and_snake() {
        let a =
            OpenskyConfig::from_json_str(r#"{"clientId":"id-a","clientSecret":"sec-a"}"#).unwrap();
        assert_eq!(a.client_id, "id-a");
        let b = OpenskyConfig::from_json_str(r#"{"client_id":"id-b","client_secret":"sec-b"}"#)
            .unwrap();
        assert_eq!(b.client_id, "id-b");
    }

    #[test]
    fn day_window_is_under_24h_same_calendar_day() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let (b, e) = utc_day_window(d);
        assert_eq!(e - b, 86_399);
        assert_eq!(e - b + 1, 86_400);
        let start = Utc.timestamp_opt(b, 0).single().unwrap();
        let end = Utc.timestamp_opt(e, 0).single().unwrap();
        assert_eq!(start.date_naive(), d);
        assert_eq!(end.date_naive(), d);
    }

    #[test]
    fn six_hour_window_stays_on_same_utc_day() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let (b, e) = utc_hours_window(d, 12, 6);
        assert_eq!(e - b, 6 * 3_600 - 1);
        let start = Utc.timestamp_opt(b, 0).single().unwrap();
        let end = Utc.timestamp_opt(e, 0).single().unwrap();
        assert_eq!(start.hour(), 12);
        assert_eq!(end.hour(), 17);
        assert_eq!(end.minute(), 59);
        assert_eq!(start.date_naive(), d);
        assert_eq!(end.date_naive(), d);
    }

    #[test]
    fn twelve_slices_cover_utc_day_without_gaps() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let (day_b, day_e) = utc_day_window(d);
        let slices = utc_day_two_hour_slices(d);
        assert_eq!(slices.len(), 12);
        assert_eq!(slices[0].0, day_b);
        assert_eq!(slices[11].1, day_e);
        for i in 1..12 {
            assert_eq!(slices[i].0, slices[i - 1].1 + 1);
        }
        for (b, e) in &slices {
            assert_eq!(*e - *b, FLIGHTS_ALL_SLICE_SECS - 1);
        }
    }

    #[test]
    fn filter_and_overlap_keep_fleet_keys() {
        let flights = parse_flights(
            br#"[
              {"icao24":"abcdef","firstSeen":100,"estDepartureAirport":"KAPA"},
              {"icao24":"ffffff","firstSeen":100,"estDepartureAirport":"KJFK"},
              {"icao24":"abcdef","firstSeen":200}
            ]"#,
        )
        .unwrap();
        let fleet = ["abcdef".to_string()].into_iter().collect();
        let kept = filter_flights_to_fleet(&flights, &fleet);
        assert_eq!(kept.len(), 2);
        let journal = [("abcdef".to_string(), 100)].into_iter().collect();
        let o = overlap_fleet_journal(&flights, &fleet, &journal);
        assert_eq!(o.fleet_count, 2);
        assert_eq!(o.fleet_hexes, 1);
        assert_eq!(o.matched_keys, 1);
        assert_eq!(o.extra_vs_journal, 1);
        assert_eq!(o.missing_from_journal, 0);
    }

    #[test]
    fn historical_flights_call_is_thirty_credits() {
        assert_eq!(FLIGHTS_CALL_CREDITS, 30);
        assert_eq!(FLIGHTS_ALL_SLICE_CREDITS, 30);
        assert_eq!(DEFAULT_MAX_FLIGHTS_CREDITS, 800);
        assert_eq!(FLIGHTS_ALL_LOOKBACK_DAYS, 90);
    }

    #[test]
    fn icao24_filter_states_call_is_four_credits() {
        assert_eq!(STATES_CALL_CREDITS, 4);
        assert_eq!(estimated_states_credits(0), 0);
        assert_eq!(estimated_states_credits(1), 4);
        assert_eq!(estimated_states_credits(80), 4);
        assert_eq!(estimated_states_credits(81), 8);
        assert_eq!(estimated_states_credits(343), 20);
        assert_eq!(states_request_count(343), 5);
    }

    #[test]
    fn parse_empty_and_array_are_no_legs() {
        assert!(parse_flights(b"").unwrap().is_empty());
        assert!(parse_flights(b"[]").unwrap().is_empty());
    }

    #[test]
    fn outcome_404_advances_cursor_429_does_not() {
        let credit = CreditInfo {
            remaining: Some(10),
            retry_after: Some(30),
            bucket: CreditBucket::Flights,
        };
        assert!(OpenskyOutcome::<()>::NotFound {
            credit: credit.clone()
        }
        .advances_cursor());
        assert!(!OpenskyOutcome::<()>::RateLimited { credit }.advances_cursor());
    }

    #[test]
    fn parse_flight_object() {
        let json = br#"[{
          "icao24":"ABCDEF",
          "firstSeen":1705276800,
          "lastSeen":1705280400,
          "estDepartureAirport":"KAPA",
          "estArrivalAirport":"KJFK",
          "callsign":" dcm1  ",
          "estDepartureAirportHorizDistance": 1250,
          "estDepartureAirportVertDistance": 304,
          "estArrivalAirportHorizDistance": 800,
          "estArrivalAirportVertDistance": 50,
          "departureAirportCandidatesCount": 1,
          "arrivalAirportCandidatesCount": 2
        }]"#;
        let f = parse_flights(json).unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].icao24, "abcdef");
        assert_eq!(f[0].est_departure_airport.as_deref(), Some("KAPA"));
        assert_eq!(f[0].callsign.as_deref(), Some("DCM1"));
        assert_eq!(f[0].est_departure_airport_horiz_distance, Some(1250));
        assert_eq!(f[0].arrival_airport_candidates_count, Some(2));
    }

    #[test]
    fn parse_empty_states() {
        let json = br#"{"time":1,"states":null}"#;
        assert!(parse_states_hexes(json).unwrap().is_empty());
    }

    #[test]
    fn parse_states_hex_list() {
        let json = br#"{"time":1,"states":[["aBcDeF","N1",null,1,1,1.0,2.0,1000,false]]}"#;
        assert_eq!(
            parse_states_hexes(json).unwrap(),
            vec!["abcdef".to_string()]
        );
    }

    #[test]
    fn parse_track_first_last() {
        let json = br#"{
          "calllsign":" n175ct ",
          "path":[
            [1,39.57,-104.67,100,90,false],
            [2,40.64,-73.78,200,90,true]
          ]
        }"#;
        let t = parse_track_endpoints(json).unwrap();
        assert!((t.dep_lat - 39.57).abs() < 1e-9);
        assert!((t.arr_lat - 40.64).abs() < 1e-9);
        assert_eq!(t.callsign.as_deref(), Some("N175CT"));
    }

    #[test]
    fn flight_to_trip_uses_airport_idents() {
        let csv = "ident,type,latitude_deg,longitude_deg\nKAPA,large_airport,39.5701,-104.6737\nKJFK,large_airport,40.6399,-73.7787\n";
        let idx = AirportIndex::from_reader(csv.as_bytes()).unwrap();
        let row = FleetRow {
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
            as_of_date: None,
        };
        let flight = Flight {
            icao24: "abcdef".into(),
            first_seen: 1_705_276_800,
            last_seen: Some(1_705_280_400),
            est_departure_airport: Some("KAPA".into()),
            est_arrival_airport: Some("KJFK".into()),
            ..Default::default()
        };
        let trip = flight_to_trip(
            &flight,
            &row,
            &idx,
            None,
            "2026-09-01T00:00:00Z",
            TripSource::OpenskyFlights,
        )
        .unwrap();
        assert_eq!(trip.dep_airport.as_deref(), Some("KAPA"));
        assert_eq!(trip.arr_airport.as_deref(), Some("KJFK"));
        assert_eq!(trip.source, "opensky_flights");
        assert!(trip.dep_lat.unwrap() > 39.0);
        assert!(trip.arr_lat.is_some());
    }

    #[test]
    fn flight_to_trip_does_not_copy_dep_as_arrival() {
        let csv = "ident,type,latitude_deg,longitude_deg\nKAPA,large_airport,39.5701,-104.6737\n";
        let idx = AirportIndex::from_reader(csv.as_bytes()).unwrap();
        let row = FleetRow {
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
            as_of_date: None,
        };
        let flight = Flight {
            icao24: "abcdef".into(),
            first_seen: 1_705_276_800,
            last_seen: Some(1_705_280_400),
            est_departure_airport: Some("KAPA".into()),
            est_arrival_airport: None,
            ..Default::default()
        };
        let trip =
            flight_to_trip(&flight, &row, &idx, None, "t", TripSource::OpenskyFlights).unwrap();
        assert_eq!(trip.dep_airport.as_deref(), Some("KAPA"));
        assert!(trip.arr_airport.is_none());
        assert!(trip.arr_lat.is_none());
        assert!(trip.arr_lon.is_none());
        assert!(trip.arr_place.is_none());
    }

    #[test]
    fn flight_without_coords_is_skipped() {
        let idx = AirportIndex::empty();
        let row = FleetRow {
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
            as_of_date: None,
        };
        let flight = Flight {
            icao24: "abcdef".into(),
            first_seen: 1_705_276_800,
            last_seen: None,
            est_departure_airport: None,
            est_arrival_airport: None,
            ..Default::default()
        };
        assert!(
            flight_to_trip(&flight, &row, &idx, None, "t", TripSource::OpenskyFlights).is_none()
        );
    }

    #[test]
    fn flight_fills_from_track_when_airports_null() {
        let idx = AirportIndex::empty();
        let row = FleetRow {
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
            as_of_date: None,
        };
        let flight = Flight {
            icao24: "abcdef".into(),
            first_seen: 1_705_276_800,
            last_seen: Some(1_705_280_400),
            est_departure_airport: None,
            est_arrival_airport: None,
            ..Default::default()
        };
        let trip = flight_to_trip(
            &flight,
            &row,
            &idx,
            Some(TrackEnds {
                dep_lat: 39.57,
                dep_lon: -104.67,
                arr_lat: 40.64,
                arr_lon: -73.78,
                callsign: Some("N1".into()),
            }),
            "t",
            TripSource::OpenskyTrack,
        )
        .unwrap();
        assert!(trip.dep_airport.is_none());
        assert!((trip.dep_lat.unwrap() - 39.57).abs() < 1e-9);
        assert!((trip.arr_lat.unwrap() - 40.64).abs() < 1e-9);
        assert_eq!(trip.source, "opensky_track");
        assert_eq!(trip.callsign.as_deref(), Some("N1"));
    }

    #[test]
    fn flight_callsign_wins_over_track() {
        let csv = "ident,type,latitude_deg,longitude_deg\nKAPA,large_airport,39.5701,-104.6737\nKJFK,large_airport,40.6399,-73.7787\n";
        let idx = AirportIndex::from_reader(csv.as_bytes()).unwrap();
        let row = FleetRow {
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
            as_of_date: None,
        };
        let flight = Flight {
            icao24: "abcdef".into(),
            first_seen: 1_705_276_800,
            last_seen: Some(1_705_280_400),
            est_departure_airport: Some("KAPA".into()),
            est_arrival_airport: Some("KJFK".into()),
            callsign: Some("DCM123".into()),
            est_departure_airport_horiz_distance: Some(100),
            est_departure_airport_vert_distance: Some(20),
            est_arrival_airport_horiz_distance: Some(200),
            est_arrival_airport_vert_distance: Some(30),
            departure_airport_candidates_count: Some(1),
            arrival_airport_candidates_count: Some(2),
        };
        let trip = flight_to_trip(
            &flight,
            &row,
            &idx,
            Some(TrackEnds {
                dep_lat: 39.57,
                dep_lon: -104.67,
                arr_lat: 40.64,
                arr_lon: -73.78,
                callsign: Some("TRACK".into()),
            }),
            "t",
            TripSource::OpenskyFlights,
        )
        .unwrap();
        assert_eq!(trip.callsign.as_deref(), Some("DCM123"));
        assert_eq!(trip.dep_airport_horiz_m, Some(100));
        assert_eq!(trip.arr_airport_candidates, Some(2));
    }
}

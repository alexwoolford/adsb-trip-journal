//! ADS-B Exchange daily trace → completed legs.

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::Value;

pub const NEW_LEG_FLAG: u32 = 2;
pub const STALE_FLAG: u32 = 1;
pub const TAXI_GS_KT: f64 = 40.0;
pub const GROUND_ALT_FT: f64 = 100.0;
pub const GROUND_DWELL_SECS: f64 = 15.0 * 60.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Altitude {
    Feet(f64),
    Ground,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TracePoint {
    pub seconds: f64,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub altitude: Altitude,
    pub gs: Option<f64>,
    pub flags: u32,
}

#[derive(Debug, Clone)]
pub struct TraceFile {
    pub icao: Option<String>,
    pub timestamp: f64,
    pub trace: Vec<TracePoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Leg {
    pub dep_ts: DateTime<Utc>,
    pub arr_ts: Option<DateTime<Utc>>,
    pub dep_lat: f64,
    pub dep_lon: f64,
    pub arr_lat: f64,
    pub arr_lon: f64,
    /// True when this is the last leg of the file and the aircraft is still airborne.
    pub still_airborne: bool,
    /// First non-stale point of the leg had `flags & 2` (or followed a long ground dwell).
    pub new_leg_boundary: bool,
}

#[derive(Debug, Deserialize)]
struct TraceFileRaw {
    icao: Option<String>,
    timestamp: f64,
    #[serde(default)]
    trace: Vec<Value>,
}

pub fn parse_trace_json(bytes: &[u8]) -> anyhow::Result<TraceFile> {
    let raw: TraceFileRaw = serde_json::from_slice(bytes)?;
    let mut trace = Vec::with_capacity(raw.trace.len());
    for row in raw.trace {
        if let Some(p) = parse_point(&row) {
            trace.push(p);
        }
    }
    Ok(TraceFile {
        icao: raw.icao.map(|s| s.to_ascii_lowercase()),
        timestamp: raw.timestamp,
        trace,
    })
}

fn parse_point(v: &Value) -> Option<TracePoint> {
    let arr = v.as_array()?;
    if arr.len() < 4 {
        return None;
    }
    let seconds = arr[0].as_f64()?;
    let lat = json_f64(&arr[1]);
    let lon = json_f64(&arr[2]);
    let altitude = parse_alt(&arr[3]);
    let gs = arr.get(4).and_then(json_f64);
    let flags = arr
        .get(6)
        .and_then(|x| x.as_u64().or_else(|| x.as_f64().map(|f| f as u64)))
        .unwrap_or(0) as u32;
    Some(TracePoint {
        seconds,
        lat,
        lon,
        altitude,
        gs,
        flags,
    })
}

fn json_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::Null => None,
        _ => None,
    }
}

fn parse_alt(v: &Value) -> Altitude {
    match v {
        Value::String(s) if s.eq_ignore_ascii_case("ground") => Altitude::Ground,
        Value::Number(n) => n.as_f64().map(Altitude::Feet).unwrap_or(Altitude::Unknown),
        _ => Altitude::Unknown,
    }
}

pub fn is_ground(p: &TracePoint) -> bool {
    match p.altitude {
        Altitude::Ground => true,
        Altitude::Feet(ft) if ft <= GROUND_ALT_FT => p.gs.map(|g| g < TAXI_GS_KT).unwrap_or(true),
        Altitude::Unknown => p.gs.map(|g| g < TAXI_GS_KT).unwrap_or(false),
        Altitude::Feet(_) => false,
    }
}

pub fn is_stale(p: &TracePoint) -> bool {
    p.flags & STALE_FLAG != 0
}

pub fn has_coords(p: &TracePoint) -> bool {
    matches!(
        (p.lat, p.lon),
        (Some(lat), Some(lon)) if lat.abs() <= 90.0 && lon.abs() <= 180.0
    )
}

pub fn is_airborne(p: &TracePoint) -> bool {
    !is_ground(p)
}

/// Split a daily trace into completed (or still-open) legs.
pub fn segment_legs(file: &TraceFile) -> Vec<Leg> {
    if file.trace.is_empty() {
        return Vec::new();
    }

    let groups = split_groups(&file.trace);
    let n = groups.len();
    let mut out = Vec::new();
    for (i, g) in groups.into_iter().enumerate() {
        let last_of_day = i + 1 == n;
        if let Some(leg) = group_to_leg(file.timestamp, &g.points, g.new_leg_boundary, last_of_day)
        {
            out.push(leg);
        }
    }
    out
}

struct Group {
    points: Vec<TracePoint>,
    new_leg_boundary: bool,
}

fn split_groups(points: &[TracePoint]) -> Vec<Group> {
    let mut groups = Vec::new();
    let mut current: Vec<TracePoint> = Vec::new();
    let mut current_boundary = false;
    let mut ground_since: Option<f64> = None;

    for p in points {
        let new_leg = p.flags & NEW_LEG_FLAG != 0;
        let ground = is_ground(p);
        let dwell_split = matches!(
            ground_since,
            Some(t0) if !ground && (p.seconds - t0) >= GROUND_DWELL_SECS && !current.is_empty()
        );

        if (new_leg || dwell_split) && !current.is_empty() {
            groups.push(Group {
                points: std::mem::take(&mut current),
                new_leg_boundary: current_boundary,
            });
            current_boundary = new_leg || dwell_split;
            current.push(p.clone());
        } else {
            if current.is_empty() {
                current_boundary = new_leg;
            }
            current.push(p.clone());
        }

        if ground {
            if ground_since.is_none() {
                ground_since = Some(p.seconds);
            }
        } else {
            ground_since = None;
        }
    }

    if !current.is_empty() {
        groups.push(Group {
            points: current,
            new_leg_boundary: current_boundary,
        });
    }
    groups
}

fn group_to_leg(
    day_ts: f64,
    points: &[TracePoint],
    new_leg_boundary: bool,
    last_of_day: bool,
) -> Option<Leg> {
    let airborne_any = points.iter().any(is_airborne);
    if !airborne_any && !new_leg_boundary {
        return None;
    }

    let dep = pick_dep(points)?;
    let arr = pick_arr(points).unwrap_or(dep);
    let still_airborne = last_of_day && is_airborne(arr);

    Some(Leg {
        dep_ts: unix_to_utc(day_ts + dep.seconds),
        arr_ts: if still_airborne {
            None
        } else {
            Some(unix_to_utc(day_ts + arr.seconds))
        },
        dep_lat: dep.lat?,
        dep_lon: dep.lon?,
        arr_lat: arr.lat?,
        arr_lon: arr.lon?,
        still_airborne,
        new_leg_boundary,
    })
}

fn pick_dep(points: &[TracePoint]) -> Option<&TracePoint> {
    // Prefer first non-stale airborne point with coords; else first non-stale with coords;
    // else first with coords (even if stale — last resort, still better than dropping the trip).
    points
        .iter()
        .find(|p| !is_stale(p) && is_airborne(p) && has_coords(p))
        .or_else(|| points.iter().find(|p| !is_stale(p) && has_coords(p)))
        .or_else(|| points.iter().find(|p| has_coords(p)))
}

fn pick_arr(points: &[TracePoint]) -> Option<&TracePoint> {
    points
        .iter()
        .rev()
        .find(|p| !is_stale(p) && has_coords(p))
        .or_else(|| points.iter().rev().find(|p| has_coords(p)))
}

pub fn unix_to_utc(secs: f64) -> DateTime<Utc> {
    let whole = secs.floor() as i64;
    let nsec = ((secs - secs.floor()) * 1_000_000_000.0).round() as u32;
    Utc.timestamp_opt(whole, nsec)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(whole, 0).single().unwrap_or(Utc::now()))
}

/// Whether the first leg of a new UTC day should close yesterday's still-open trip.
pub fn is_overnight_continuation(first: &Leg) -> bool {
    first.dep_ts.time() < chrono::NaiveTime::from_hms_opt(2, 0, 0).unwrap()
        && !first.new_leg_boundary
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts_day() -> f64 {
        // 2024-01-15 00:00:00 UTC
        1_705_276_800.0
    }

    fn pt(secs: f64, lat: f64, lon: f64, alt: Altitude, gs: f64, flags: u32) -> TracePoint {
        TracePoint {
            seconds: secs,
            lat: Some(lat),
            lon: Some(lon),
            altitude: alt,
            gs: Some(gs),
            flags,
        }
    }

    fn file(points: Vec<TracePoint>) -> TraceFile {
        TraceFile {
            icao: Some("abcdef".into()),
            timestamp: ts_day(),
            trace: points,
        }
    }

    #[test]
    fn flags_bit2_splits_legs() {
        let f = file(vec![
            pt(100.0, 39.57, -104.67, Altitude::Feet(500.0), 120.0, 0),
            pt(200.0, 39.58, -104.66, Altitude::Feet(800.0), 140.0, 0),
            pt(400.0, 39.57, -104.67, Altitude::Ground, 5.0, 0),
            pt(
                800.0,
                39.57,
                -104.67,
                Altitude::Feet(400.0),
                110.0,
                NEW_LEG_FLAG,
            ),
            pt(900.0, 39.59, -104.65, Altitude::Feet(900.0), 130.0, 0),
        ]);
        let legs = segment_legs(&f);
        assert_eq!(legs.len(), 2);
        assert!(legs[1].new_leg_boundary);
        assert_eq!(legs[0].dep_lat, 39.57);
        assert_eq!(legs[1].dep_lat, 39.57);
    }

    #[test]
    fn ground_dwell_splits_without_flags() {
        let f = file(vec![
            pt(0.0, 39.57, -104.67, Altitude::Feet(500.0), 120.0, 0),
            pt(60.0, 39.57, -104.67, Altitude::Ground, 8.0, 0),
            pt(
                60.0 + GROUND_DWELL_SECS + 10.0,
                39.57,
                -104.67,
                Altitude::Ground,
                5.0,
                0,
            ),
            pt(
                60.0 + GROUND_DWELL_SECS + 20.0,
                39.57,
                -104.67,
                Altitude::Feet(400.0),
                100.0,
                0,
            ),
        ]);
        let legs = segment_legs(&f);
        assert_eq!(legs.len(), 2);
        assert!(legs[1].new_leg_boundary);
    }

    #[test]
    fn stale_coords_ignored_for_dep() {
        let f = file(vec![
            pt(10.0, 1.0, 1.0, Altitude::Feet(1000.0), 200.0, STALE_FLAG),
            pt(20.0, 39.57, -104.67, Altitude::Feet(1000.0), 200.0, 0),
            pt(80.0, 39.58, -104.66, Altitude::Feet(1200.0), 200.0, 0),
        ]);
        let legs = segment_legs(&f);
        assert_eq!(legs.len(), 1);
        assert!((legs[0].dep_lat - 39.57).abs() < 1e-9);
    }

    #[test]
    fn still_airborne_leaves_arr_ts_null() {
        let f = file(vec![
            pt(100.0, 39.57, -104.67, Altitude::Feet(500.0), 120.0, 0),
            pt(80_000.0, 40.0, -100.0, Altitude::Feet(35000.0), 400.0, 0),
        ]);
        let legs = segment_legs(&f);
        assert_eq!(legs.len(), 1);
        assert!(legs[0].still_airborne);
        assert!(legs[0].arr_ts.is_none());
        assert!((legs[0].arr_lat - 40.0).abs() < 1e-9);
    }

    #[test]
    fn no_coords_emits_nothing() {
        let f = file(vec![TracePoint {
            seconds: 1.0,
            lat: None,
            lon: None,
            altitude: Altitude::Feet(1000.0),
            gs: Some(200.0),
            flags: 0,
        }]);
        assert!(segment_legs(&f).is_empty());
    }

    #[test]
    fn parse_ground_string_altitude() {
        let json = br#"{
          "icao": "ABCDEF",
          "timestamp": 1705276800,
          "trace": [
            [10, 39.57, -104.67, "ground", 8, 90, 0],
            [20, 39.57, -104.67, 500, 110, 90, 2]
          ]
        }"#;
        let t = parse_trace_json(json).unwrap();
        assert_eq!(t.icao.as_deref(), Some("abcdef"));
        assert_eq!(t.trace[0].altitude, Altitude::Ground);
        assert_eq!(t.trace[1].flags, 2);
        let legs = segment_legs(&t);
        assert_eq!(legs.len(), 1);
    }

    #[test]
    fn continuation_window() {
        let mut leg = Leg {
            dep_ts: unix_to_utc(ts_day() + 60.0),
            arr_ts: Some(unix_to_utc(ts_day() + 120.0)),
            dep_lat: 1.0,
            dep_lon: 2.0,
            arr_lat: 3.0,
            arr_lon: 4.0,
            still_airborne: false,
            new_leg_boundary: false,
        };
        assert!(is_overnight_continuation(&leg));
        leg.new_leg_boundary = true;
        assert!(!is_overnight_continuation(&leg));
    }
}

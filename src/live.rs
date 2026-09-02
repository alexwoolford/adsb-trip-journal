//! Live-position fallback: ground → airborne → ground as a trip.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::fleet::FleetRow;
use crate::store::TripSource;

#[derive(Debug, Clone)]
pub struct LiveAircraft {
    pub hex: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alt_baro: Option<LiveAlt>,
    pub gs: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum LiveAlt {
    Ground,
    Feet(f64),
}

#[derive(Debug, Clone)]
pub struct CompletedLiveTrip {
    pub icao24: String,
    pub n_number: String,
    pub ticker: String,
    pub cik: Option<String>,
    pub dep_ts: DateTime<Utc>,
    pub arr_ts: DateTime<Utc>,
    pub dep_lat: f64,
    pub dep_lon: f64,
    pub arr_lat: f64,
    pub arr_lon: f64,
    pub source: TripSource,
}

#[derive(Debug, Clone)]
enum Phase {
    Unknown,
    Ground,
    Airborne {
        dep_ts: DateTime<Utc>,
        dep_lat: f64,
        dep_lon: f64,
    },
}

pub struct LiveTracker {
    phases: HashMap<String, Phase>,
    fleet: HashMap<String, FleetRow>,
}

impl LiveTracker {
    pub fn new(fleet: &[FleetRow]) -> Self {
        let fleet = fleet
            .iter()
            .map(|r| (r.icao24.clone(), r.clone()))
            .collect();
        Self {
            phases: HashMap::new(),
            fleet,
        }
    }

    pub fn ingest(
        &mut self,
        samples: &[LiveAircraft],
        now: DateTime<Utc>,
    ) -> Vec<CompletedLiveTrip> {
        let mut done = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for s in samples {
            let hex = s.hex.to_ascii_lowercase();
            seen.insert(hex.clone());
            let Some(row) = self.fleet.get(&hex).cloned() else {
                continue;
            };
            let airborne = is_airborne(s);
            let coords = match (s.lat, s.lon) {
                (Some(lat), Some(lon)) => Some((lat, lon)),
                _ => None,
            };

            let prev = self.phases.get(&hex).cloned().unwrap_or(Phase::Unknown);
            let next = match (prev, airborne, coords) {
                (
                    Phase::Airborne {
                        dep_ts,
                        dep_lat,
                        dep_lon,
                    },
                    false,
                    Some((lat, lon)),
                ) => {
                    done.push(CompletedLiveTrip {
                        icao24: hex.clone(),
                        n_number: row.n_number,
                        ticker: row.ticker,
                        cik: row.cik,
                        dep_ts,
                        arr_ts: now,
                        dep_lat,
                        dep_lon,
                        arr_lat: lat,
                        arr_lon: lon,
                        source: TripSource::AdsBxLive,
                    });
                    Phase::Ground
                }
                (
                    Phase::Airborne {
                        dep_ts,
                        dep_lat,
                        dep_lon,
                    },
                    true,
                    Some(_),
                ) => Phase::Airborne {
                    dep_ts,
                    dep_lat,
                    dep_lon,
                },
                (Phase::Unknown | Phase::Ground, true, Some((lat, lon))) => Phase::Airborne {
                    dep_ts: now,
                    dep_lat: lat,
                    dep_lon: lon,
                },
                (Phase::Unknown, false, _) => Phase::Ground,
                (other, _, _) => other,
            };
            self.phases.insert(hex, next);
        }

        // Missing from the poll: treat as "not seen". Do not close airborne legs
        // on a single missed poll (coverage holes). Leave them open.
        let _ = seen;
        done
    }
}

fn is_airborne(s: &LiveAircraft) -> bool {
    match s.alt_baro {
        Some(LiveAlt::Ground) => false,
        Some(LiveAlt::Feet(ft)) if ft <= 100.0 => s.gs.map(|g| g >= 40.0).unwrap_or(false),
        Some(LiveAlt::Feet(_)) => true,
        None => s.gs.map(|g| g >= 40.0).unwrap_or(false),
    }
}

pub fn parse_live_aircraft(body: &[u8]) -> anyhow::Result<Vec<LiveAircraft>> {
    let v: serde_json::Value = serde_json::from_slice(body)?;
    let arr = v
        .get("ac")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for a in arr {
        let hex = a
            .get("hex")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if hex.is_empty() {
            continue;
        }
        let alt_baro = match a.get("alt_baro") {
            Some(x)
                if x.as_str()
                    .map(|s| s.eq_ignore_ascii_case("ground"))
                    .unwrap_or(false) =>
            {
                Some(LiveAlt::Ground)
            }
            Some(x) => x.as_f64().map(LiveAlt::Feet),
            None => None,
        };
        out.push(LiveAircraft {
            hex,
            lat: a.get("lat").and_then(|x| x.as_f64()),
            lon: a.get("lon").and_then(|x| x.as_f64()),
            alt_baro,
            gs: a.get("gs").and_then(|x| x.as_f64()),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::FleetRow;
    use chrono::TimeZone;

    fn fleet() -> FleetRow {
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
            as_of_date: None,
        }
    }

    fn ac(alt: LiveAlt, lat: f64, lon: f64, gs: f64) -> LiveAircraft {
        LiveAircraft {
            hex: "abcdef".into(),
            lat: Some(lat),
            lon: Some(lon),
            alt_baro: Some(alt),
            gs: Some(gs),
        }
    }

    #[test]
    fn ground_air_ground_emits_trip() {
        let mut t = LiveTracker::new(&[fleet()]);
        let t0 = Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();
        assert!(t
            .ingest(&[ac(LiveAlt::Ground, 39.57, -104.67, 5.0)], t0)
            .is_empty());
        let t1 = t0 + chrono::Duration::minutes(5);
        assert!(t
            .ingest(&[ac(LiveAlt::Feet(2000.0), 39.58, -104.66, 180.0)], t1)
            .is_empty());
        let t2 = t1 + chrono::Duration::minutes(40);
        let trips = t.ingest(&[ac(LiveAlt::Ground, 40.64, -73.78, 8.0)], t2);
        assert_eq!(trips.len(), 1);
        assert_eq!(trips[0].dep_lat, 39.58);
        assert_eq!(trips[0].arr_lat, 40.64);
        assert_eq!(trips[0].source, TripSource::AdsBxLive);
    }

    #[test]
    fn parse_live_json() {
        let body = br#"{"ac":[{"hex":"ABCDEF","lat":1.0,"lon":2.0,"alt_baro":"ground","gs":3}]}"#;
        let v = parse_live_aircraft(body).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].hex, "abcdef");
        assert!(matches!(v[0].alt_baro, Some(LiveAlt::Ground)));
    }
}

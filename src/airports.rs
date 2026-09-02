//! Snap dep/arr lat/lon to OurAirports `ident` within a small radius.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};

pub const SNAP_RADIUS_KM: f64 = 8.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AirportType {
    Large = 0,
    Medium = 1,
    Small = 2,
    Heliport = 3,
}

impl AirportType {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "large_airport" => Some(Self::Large),
            "medium_airport" => Some(Self::Medium),
            "small_airport" => Some(Self::Small),
            "heliport" => Some(Self::Heliport),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Airport {
    pub ident: String,
    pub lat: f64,
    pub lon: f64,
    pub kind: AirportType,
}

#[derive(Debug, Clone)]
pub struct AirportIndex {
    airports: Vec<Airport>,
    by_ident: HashMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Snap {
    pub ident: Option<String>,
    pub place: String,
}

impl AirportIndex {
    pub fn load_csv(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("open airports csv {}", path.display()))?;
        Self::from_reader(file)
    }

    pub fn from_reader<R: Read>(r: R) -> Result<Self> {
        let mut rdr = csv::Reader::from_reader(r);
        let mut airports: Vec<Airport> = Vec::new();
        let mut tmp: HashMap<String, usize> = HashMap::new();
        for rec in rdr.deserialize::<AirportCsv>() {
            let rec = rec?;
            let Some(kind) = AirportType::parse(&rec.type_field) else {
                continue;
            };
            let Some(lat) = rec.latitude_deg else {
                continue;
            };
            let Some(lon) = rec.longitude_deg else {
                continue;
            };
            if rec.ident.trim().is_empty() {
                continue;
            }
            let ident = rec.ident.trim().to_string();
            let key = ident.to_ascii_uppercase();
            let airport = Airport {
                ident,
                lat,
                lon,
                kind,
            };
            if let Some(&idx) = tmp.get(&key) {
                if kind < airports[idx].kind {
                    airports[idx] = airport;
                }
            } else {
                tmp.insert(key, airports.len());
                airports.push(airport);
            }
        }
        let by_ident = airports
            .iter()
            .enumerate()
            .map(|(i, a)| (a.ident.to_ascii_uppercase(), i))
            .collect();
        Ok(Self { airports, by_ident })
    }

    pub fn empty() -> Self {
        Self {
            airports: Vec::new(),
            by_ident: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.airports.len()
    }

    pub fn is_empty(&self) -> bool {
        self.airports.is_empty()
    }

    pub fn by_ident(&self, ident: &str) -> Option<&Airport> {
        let key = ident.trim().to_ascii_uppercase();
        self.by_ident.get(&key).map(|&i| &self.airports[i])
    }

    pub fn snap(&self, lat: f64, lon: f64) -> Snap {
        self.snap_radius(lat, lon, SNAP_RADIUS_KM)
    }

    pub fn snap_radius(&self, lat: f64, lon: f64, radius_km: f64) -> Snap {
        let mut best_non_heli: Option<(f64, AirportType, &Airport)> = None;
        let mut best_heli: Option<(f64, &Airport)> = None;

        for a in &self.airports {
            let d = haversine_km(lat, lon, a.lat, a.lon);
            if d > radius_km {
                continue;
            }
            match a.kind {
                AirportType::Heliport => {
                    if best_heli.map(|(bd, _)| d < bd).unwrap_or(true) {
                        best_heli = Some((d, a));
                    }
                }
                k => {
                    let better = match best_non_heli {
                        None => true,
                        Some((bd, bk, _)) => k < bk || (k == bk && d < bd),
                    };
                    if better {
                        best_non_heli = Some((d, k, a));
                    }
                }
            }
        }

        let hit = best_non_heli
            .map(|(_, _, a)| a)
            .or_else(|| best_heli.map(|(_, a)| a));
        match hit {
            Some(a) => Snap {
                ident: Some(a.ident.clone()),
                place: a.ident.clone(),
            },
            None => Snap {
                ident: None,
                place: format_latlon(lat, lon),
            },
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct AirportCsv {
    ident: String,
    #[serde(rename = "type")]
    type_field: String,
    latitude_deg: Option<f64>,
    longitude_deg: Option<f64>,
}

pub fn format_latlon(lat: f64, lon: f64) -> String {
    format!("{lat:.4},{lon:.4}")
}

pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let p1 = lat1.to_radians();
    let p2 = lat2.to_radians();
    let dp = (lat2 - lat1).to_radians();
    let dl = (lon2 - lon1).to_radians();
    let a = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"ident,type,latitude_deg,longitude_deg
KAPA,large_airport,39.5701,-104.6737
KJFK,large_airport,40.6399,-73.7787
CO99,small_airport,39.5750,-104.6700
HEL1,heliport,39.5710,-104.6740
FAR,small_airport,0.0,0.0
"#;

    fn idx() -> AirportIndex {
        AirportIndex::from_reader(FIXTURE.as_bytes()).unwrap()
    }

    #[test]
    fn by_ident_is_case_insensitive() {
        let idx = idx();
        let s = idx.by_ident("kapa").unwrap();
        assert_eq!(s.ident, "KAPA");
    }

    #[test]
    fn prefers_large_over_closer_small() {
        let s = idx().snap(39.572, -104.672);
        assert_eq!(s.ident.as_deref(), Some("KAPA"));
    }

    #[test]
    fn heliport_only_if_nothing_else() {
        let csv = "ident,type,latitude_deg,longitude_deg\nHEL1,heliport,39.5710,-104.6740\n";
        let i = AirportIndex::from_reader(csv.as_bytes()).unwrap();
        assert_eq!(i.snap(39.571, -104.674).ident.as_deref(), Some("HEL1"));
    }

    #[test]
    fn miss_uses_rounded_latlon() {
        let s = idx().snap(10.12344, 20.98761);
        assert!(s.ident.is_none());
        assert_eq!(s.place, "10.1234,20.9876");
    }

    #[test]
    fn skips_closed_and_balloonports() {
        let csv = "ident,type,latitude_deg,longitude_deg\nXX,closed_airport,39.57,-104.67\n";
        let i = AirportIndex::from_reader(csv.as_bytes()).unwrap();
        assert!(i.snap(39.57, -104.67).ident.is_none());
    }
}

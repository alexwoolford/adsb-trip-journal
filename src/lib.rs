//! ADS-B trip journal: consume a tail-to-ticker mapping feed and collect trips.

pub mod airports;
mod capture;
pub mod collect;
pub mod fleet;
pub mod opensky;
pub mod store;

pub use airports::{AirportIndex, SNAP_RADIUS_KM};
pub use collect::{collect, watch_opensky, CollectOptions, CollectReport};
pub use fleet::{query_fleet, FleetQuery, FleetRow};
pub use store::{JournalDb, TripRow, TripSource};

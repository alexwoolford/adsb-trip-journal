//! ADS-B trip journal: consume a tail-to-ticker mapping feed and collect trips.

pub mod adsbx;
pub mod airports;
pub mod collect;
pub mod fleet;
pub mod live;
pub mod opensky;
pub mod segment;
pub mod store;

pub use adsbx::{AdsBxClient, AdsBxConfig, ProbeReport};
pub use airports::{AirportIndex, SNAP_RADIUS_KM};
pub use collect::{collect, watch_opensky, CollectOptions, CollectReport, CollectSource};
pub use fleet::{query_fleet, FleetQuery, FleetRow};
pub use store::{JournalDb, TripRow, TripSource};

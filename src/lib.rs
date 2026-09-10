//! ADS-B trip journal: consume a tail-to-ticker mapping feed and collect trips.

pub mod airports;
pub mod collect;
pub mod fleet;
pub mod opensky;
pub mod store;

pub use airports::{estimate_horiz_ok, AirportIndex, SNAP_RADIUS_KM};
pub use collect::{
    collect, remove_flights_all_caches, watch_opensky, CollectOptions, CollectReport,
};
pub use fleet::{query_fleet, FleetQuery, FleetRow};
pub use store::{utc_dates_inclusive, GcReport, InvalidateReport, JournalDb, TripRow, TripSource};

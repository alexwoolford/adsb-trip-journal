//! Read-only fleet slice from tail-to-ticker `mappings_current`.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

/// One published mapping row that survived the default fleet filter *except*
/// empty `icao24` (those are counted, then dropped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetRow {
    pub n_number: String,
    pub icao24: String,
    pub ticker: String,
    pub cik: Option<String>,
    pub company_name: Option<String>,
    pub make: Option<String>,
    pub model: Option<String>,
    pub registrant_name: Option<String>,
    pub match_method: Option<String>,
    pub aviation_issuer: i64,
    pub fleet_size: i64,
    pub as_of_date: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FleetQuery {
    pub rows: Vec<FleetRow>,
    /// Slice rows whose `icao24` was null/empty after trim.
    pub skipped_empty_icao24: u64,
    /// `MAX(as_of_date)` on the slice (for `fleet_snapshot.snapshot_as_of`).
    pub snapshot_as_of: String,
}

const FLEET_SQL: &str = r#"
SELECT n_number, icao24, ticker, cik, company_name, make, model,
       registrant_name, match_method, aviation_issuer, fleet_size,
       as_of_date
FROM mappings_current
WHERE aviation_issuer = 0
  AND fleet_size BETWEEN 1 AND 6
"#;

pub fn open_mapping_ro(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open mapping sqlite read-only: {}", path.display()))
}

pub fn query_fleet(conn: &Connection) -> Result<FleetQuery> {
    let mut stmt = conn.prepare(FLEET_SQL)?;
    let iter = stmt.query_map([], |row| {
        Ok(RawRow {
            n_number: row.get(0)?,
            icao24: row.get::<_, Option<String>>(1)?,
            ticker: row.get(2)?,
            cik: row.get(3)?,
            company_name: row.get(4)?,
            make: row.get(5)?,
            model: row.get(6)?,
            registrant_name: row.get(7)?,
            match_method: row.get(8)?,
            aviation_issuer: row.get(9)?,
            fleet_size: row.get(10)?,
            as_of_date: row.get(11)?,
        })
    })?;

    let mut rows = Vec::new();
    let mut skipped_empty_icao24 = 0u64;
    let mut max_as_of: Option<String> = None;

    for item in iter {
        let raw = item?;
        if let Some(ref d) = raw.as_of_date {
            match &max_as_of {
                None => max_as_of = Some(d.clone()),
                Some(cur) if d.as_str() > cur.as_str() => max_as_of = Some(d.clone()),
                _ => {}
            }
        }
        let hex = raw
            .icao24
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_lowercase());
        match hex {
            None => skipped_empty_icao24 += 1,
            Some(icao24) => rows.push(FleetRow {
                n_number: raw.n_number,
                icao24,
                ticker: raw.ticker,
                cik: raw.cik,
                company_name: raw.company_name,
                make: raw.make,
                model: raw.model,
                registrant_name: raw.registrant_name,
                match_method: raw.match_method,
                aviation_issuer: raw.aviation_issuer,
                fleet_size: raw.fleet_size,
                as_of_date: raw.as_of_date,
            }),
        }
    }

    let snapshot_as_of = max_as_of.unwrap_or_else(|| chrono::Utc::now().date_naive().to_string());
    Ok(FleetQuery {
        rows,
        skipped_empty_icao24,
        snapshot_as_of,
    })
}

struct RawRow {
    n_number: String,
    icao24: Option<String>,
    ticker: String,
    cik: Option<String>,
    company_name: Option<String>,
    make: Option<String>,
    model: Option<String>,
    registrant_name: Option<String>,
    match_method: Option<String>,
    aviation_issuer: i64,
    fleet_size: i64,
    as_of_date: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
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
              aviation_issuer INTEGER NOT NULL
            );
            INSERT INTO mappings_current VALUES
              ('N1','ABCDEF','AAA',NULL,'A Co',NULL,NULL,NULL,NULL,'2026-08-31',1,0),
              ('N2','','AAA',NULL,'A Co',NULL,NULL,NULL,NULL,'2026-08-31',1,0),
              ('N3',NULL,'AAA',NULL,'A Co',NULL,NULL,NULL,NULL,'2026-08-31',2,0),
              ('N4','aabbcc','BIG',NULL,'Big Co',NULL,NULL,NULL,NULL,'2026-08-31',20,0),
              ('N5','ddeeff','OEM',NULL,'Oem Co',NULL,NULL,NULL,NULL,'2026-08-31',1,1);
            "#,
        )
        .unwrap();
        conn
    }

    #[test]
    fn default_slice_skips_empty_and_out_of_scope() {
        let q = query_fleet(&setup()).unwrap();
        assert_eq!(q.skipped_empty_icao24, 2);
        assert_eq!(q.rows.len(), 1);
        assert_eq!(q.rows[0].n_number, "N1");
        assert_eq!(q.rows[0].icao24, "abcdef");
        assert_eq!(q.snapshot_as_of, "2026-08-31");
    }
}

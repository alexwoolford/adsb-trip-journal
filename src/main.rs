//! CLI for the ADS-B trip journal.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use adsb_trip_journal::collect::{
    collect, default_today_utc, print_status, watch_opensky, CollectOptions, CollectReport,
};
use adsb_trip_journal::fleet::{self, query_fleet};
use adsb_trip_journal::opensky::{OpenskyClient, OpenskyConfig, DEFAULT_MAX_FLIGHTS_CREDITS};
use adsb_trip_journal::store::JournalDb;

const DEFAULT_MAPPING: &str = "../tail-to-ticker/data/current/tail_to_ticker.sqlite";
const OUR_AIRPORTS_URL: &str = "https://davidmegginson.github.io/ourairports-data/airports.csv";

#[derive(Parser)]
#[command(
    name = "adsb-trip-journal",
    about = "Restartable trip journal for mapped corporate tails"
)]
struct Cli {
    #[arg(long, global = true, default_value = "data", env = "TRIP_JOURNAL_DATA")]
    data_dir: PathBuf,
    #[arg(
        long,
        global = true,
        default_value = "cache",
        env = "TRIP_JOURNAL_CACHE"
    )]
    cache_dir: PathBuf,
    #[arg(
        long,
        global = true,
        default_value = DEFAULT_MAPPING,
        env = "TAIL_TO_TICKER_SQLITE"
    )]
    mapping_sqlite: PathBuf,
    #[arg(long, global = true, env = "TRIP_JOURNAL_SQLITE")]
    journal_sqlite: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Probe one OpenSky GET /flights/all 2h slice. Does not call /flights/aircraft.
    Probe {
        /// UTC date for the 2h window starting 12:00 (default: yesterday).
        #[arg(long)]
        date: Option<String>,
        /// Required: measure GET /flights/all (skips /states/all and /flights/aircraft).
        #[arg(long)]
        opensky: bool,
        /// Required with --opensky.
        #[arg(long)]
        flights_all: bool,
    },
    /// Download OurAirports airports.csv into cache/.
    AirportsFetch {
        #[arg(long, default_value = OUR_AIRPORTS_URL)]
        url: String,
    },
    /// Snapshot the fleet and collect OpenSky /flights/all slices.
    Collect {
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        /// Cap OpenSky `/flights/all` spend (12 slices/day; historical slices billed 30 on this account).
        #[arg(long, default_value_t = DEFAULT_MAX_FLIGHTS_CREDITS)]
        max_flights_credits: u32,
        /// Skip /tracks when both OpenSky airport estimates are missing.
        #[arg(long, default_value_t = false)]
        no_tracks_fallback: bool,
        /// Restrict OpenSky ingest to these hexes (repeatable). Slice cache is still the full mapped fleet; a `--hex` run does not mark the UTC day complete.
        #[arg(long = "hex")]
        hexes: Vec<String>,
    },
    /// Poll OpenSky /states/all and record hexes seen airborne (not a collect gate).
    Watch {
        #[arg(long, default_value_t = 600)]
        interval_secs: u64,
        #[arg(long)]
        max_polls: Option<u32>,
    },
    /// Print journal coverage.
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Laptop-only: load `.env` from cwd. Production uses systemd EnvironmentFile.
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let journal = cli
        .journal_sqlite
        .clone()
        .unwrap_or_else(|| cli.data_dir.join("trips.sqlite"));

    match &cli.command {
        Commands::Probe {
            date,
            opensky,
            flights_all,
        } => {
            if !*opensky || !*flights_all {
                anyhow::bail!(
                    "probe only supports --opensky --flights-all (do not call /flights/aircraft)"
                );
            }
            cmd_probe_flights_all(&cli, date.as_deref()).await?;
        }
        Commands::AirportsFetch { url } => {
            cmd_airports_fetch(&cli.cache_dir, url).await?;
        }
        Commands::Collect {
            from,
            to,
            max_flights_credits,
            no_tracks_fallback,
            hexes,
        } => {
            cmd_collect(
                &cli,
                &journal,
                from.as_deref(),
                to.as_deref(),
                *max_flights_credits,
                !*no_tracks_fallback,
                hexes.clone(),
            )
            .await?;
        }
        Commands::Watch {
            interval_secs,
            max_polls,
        } => {
            cmd_watch(&cli, &journal, *interval_secs, *max_polls).await?;
        }
        Commands::Status => {
            cmd_status(&cli.mapping_sqlite, &journal)?;
        }
    }
    Ok(())
}

fn parse_date_opt(s: Option<&str>) -> Result<Option<NaiveDate>> {
    match s {
        None => Ok(None),
        Some(s) => Ok(Some(
            NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(|| format!("date {s}"))?,
        )),
    }
}

async fn cmd_collect(
    cli: &Cli,
    journal: &Path,
    from: Option<&str>,
    to: Option<&str>,
    max_flights_credits: u32,
    tracks_fallback: bool,
    hexes: Vec<String>,
) -> Result<()> {
    let cfg = OpenskyConfig::from_env()?.ok_or_else(|| {
        anyhow::anyhow!(
            "collect needs OPENSKY_CLIENT_ID + OPENSKY_CLIENT_SECRET, or OPENSKY_CREDENTIALS_JSON"
        )
    })?;
    let opts = CollectOptions {
        mapping_sqlite: cli.mapping_sqlite.clone(),
        journal_sqlite: journal.to_path_buf(),
        cache_dir: cli.cache_dir.clone(),
        airports_csv: Some(cli.cache_dir.join("airports.csv")),
        from: parse_date_opt(from)?,
        to: parse_date_opt(to)?,
        opensky: Some(Arc::new(OpenskyClient::new(cfg)?)),
        max_flights_credits,
        tracks_fallback,
        hex_filter: hexes,
    };
    let report = collect(opts).await?;
    print_report(&report);
    Ok(())
}

async fn cmd_watch(
    cli: &Cli,
    journal: &Path,
    interval_secs: u64,
    max_polls: Option<u32>,
) -> Result<()> {
    let cfg = OpenskyConfig::from_env()?.ok_or_else(|| {
        anyhow::anyhow!(
            "watch needs OPENSKY_CLIENT_ID + OPENSKY_CLIENT_SECRET, or OPENSKY_CREDENTIALS_JSON"
        )
    })?;
    let client = Arc::new(OpenskyClient::new(cfg)?);
    let report = watch_opensky(
        &cli.mapping_sqlite,
        journal,
        client,
        Duration::from_secs(interval_secs),
        max_polls,
    )
    .await?;
    print_report(&report);
    Ok(())
}

async fn cmd_probe_flights_all(cli: &Cli, date: Option<&str>) -> Result<()> {
    let cfg = OpenskyConfig::from_env()?.ok_or_else(|| {
        anyhow::anyhow!(
            "probe --opensky --flights-all needs OPENSKY_CLIENT_ID + OPENSKY_CLIENT_SECRET, or OPENSKY_CREDENTIALS_JSON"
        )
    })?;
    let client = OpenskyClient::new(cfg)?;
    let date = match date {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")?,
        None => default_today_utc()
            .pred_opt()
            .unwrap_or_else(default_today_utc),
    };
    let (begin, end) = adsb_trip_journal::opensky::utc_hours_window(date, 12, 2);
    let fleet: HashSet<String> = if cli.mapping_sqlite.exists() {
        let conn = fleet::open_mapping_ro(&cli.mapping_sqlite)?;
        query_fleet(&conn)?
            .rows
            .into_iter()
            .map(|r| r.icao24)
            .collect()
    } else {
        HashSet::new()
    };
    let journal = cli
        .journal_sqlite
        .clone()
        .unwrap_or_else(|| cli.data_dir.join("trips.sqlite"));
    let journal_keys: HashSet<(String, i64)> = if journal.exists() {
        let db = JournalDb::open(&journal)?;
        db.trip_keys_between(begin, end)?.into_iter().collect()
    } else {
        HashSet::new()
    };
    let before = previous_flights_all_remaining(&cli.data_dir);
    let report = client
        .probe_flights_all(date, 12, before, &fleet, &journal_keys)
        .await?;
    println!(
        "opensky flights/all probe date={} window={} abort={} note={}",
        report.date, report.flights_window, report.abort, report.note
    );
    println!(
        "  status={} bytes={} elapsed_ms={} raw_count={}",
        report
            .flights_status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "—".into()),
        report.body_bytes,
        report.elapsed_ms,
        report.raw_count
    );
    println!(
        "  remaining_before={:?} remaining_after={:?} spent={:?}",
        report.flights_remaining_before, report.flights_remaining, report.flights_credits_spent
    );
    println!(
        "  fleet_flights={} fleet_hexes={} journal_in_window={} matched={} extra={} missing={}",
        report.fleet_count,
        report.fleet_hexes,
        report.journal_trips_in_window,
        report.matched_keys,
        report.extra_vs_journal,
        report.missing_from_journal
    );
    std::fs::create_dir_all(&cli.data_dir)?;
    let out = cli.data_dir.join("flights_all_probe.json");
    std::fs::write(&out, serde_json::to_vec_pretty(&report)?)?;
    println!("wrote {}", out.display());
    if report.abort {
        anyhow::bail!("flights/all probe aborted; do not switch collect");
    }
    Ok(())
}

fn previous_flights_all_remaining(data_dir: &Path) -> Option<u32> {
    remaining_from_probe_json(&data_dir.join("flights_all_probe.json"))
}

fn remaining_from_probe_json(path: &Path) -> Option<u32> {
    let raw = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    v.get("flights_remaining")
        .and_then(|x| x.as_u64())
        .map(|n| n as u32)
}

async fn cmd_airports_fetch(cache_dir: &Path, url: &str) -> Result<()> {
    std::fs::create_dir_all(cache_dir)?;
    let dest = cache_dir.join("airports.csv");
    let client = reqwest::Client::builder()
        .user_agent("adsb-trip-journal/0.1")
        .timeout(Duration::from_secs(120))
        .build()?;
    let bytes = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    std::fs::write(&dest, &bytes)?;
    println!("wrote {} ({} bytes)", dest.display(), bytes.len());
    Ok(())
}

fn cmd_status(mapping: &Path, journal: &Path) -> Result<()> {
    if !journal.exists() {
        println!("no journal at {}", journal.display());
        return Ok(());
    }
    let db = JournalDb::open(journal)?;
    let status = db.status(journal)?;
    let fleet = if mapping.exists() {
        let conn = fleet::open_mapping_ro(mapping)?;
        Some(query_fleet(&conn)?)
    } else {
        None
    };
    print_status(&status, fleet.as_ref());
    Ok(())
}

fn print_report(r: &CollectReport) {
    println!(
        "collect fleet_hexes={} skipped_empty_icao24={} days_ok={} days_404={} from_cache={} trips_upserted={} stopped_hexes={} live_polls={} live_trips={}",
        r.fleet_hexes,
        r.skipped_empty_icao24,
        r.days_ok,
        r.days_not_found,
        r.days_from_cache,
        r.trips_upserted,
        r.hexes_stopped_on_error,
        r.live_polls,
        r.live_trips
    );
    if r.flights_calls > 0
        || r.tracks_calls > 0
        || r.estimated_flights_credits > 0
        || r.skipped_no_coords > 0
    {
        println!(
            "  opensky flights_calls={} tracks_calls={} skipped_no_coords={} estimated_flights_credits={}",
            r.flights_calls,
            r.tracks_calls,
            r.skipped_no_coords,
            r.estimated_flights_credits
        );
    }
}

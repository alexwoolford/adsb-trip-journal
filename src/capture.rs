//! SQLite capture layer: `_outbox`, generated triggers, announce, post-commit nudge.
//!
//! Copy this file as `src/capture.rs` (or `crates/<lib>/src/capture.rs`) and
//! `mod capture;` in that crate. Do not path-depend a sibling package.
//!
//! Canonical copy: `ticker-air-journal/sqlite-capture.rs`.
//! Spec: `capturable-state-design-principles.md`.
//!
//! **Clocks:** `_outbox.ts` and `deleted_at` are INTEGER Unix seconds.
//! Fact columns stay TEXT (`YYYY-MM-DD` / `YYYY-MM-DDTHH:MM:SSZ`).

#![allow(dead_code)] // copied module: not every utility uses every mode or helper

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OptionalExtension};

pub const ENV_ANNOUNCE_DIR: &str = "STATE_CAPTURE_ANNOUNCE_DIR";
pub const DEFAULT_ANNOUNCE_DIR: &str = "/var/lib/state-capture/announce";
pub const ENV_SOCK: &str = "STATE_CAPTURE_SOCK";
pub const DEFAULT_SOCK: &str = "/run/state/collect.sock";
pub const OUTBOX_TABLE: &str = "_outbox";

/// Canonical DDL. Do not customize. Never DROP this table.
pub const OUTBOX_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS _outbox (
  seq    INTEGER PRIMARY KEY AUTOINCREMENT,
  tbl    TEXT    NOT NULL,
  op     TEXT    NOT NULL CHECK (op IN ('I','U','D')),
  ts     INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  key    TEXT    NOT NULL,
  before TEXT,
  after  TEXT
) STRICT;
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureMode {
    /// Insert: after. Update: before+after. Delete: before.
    Full,
    /// Insert/update: after only. Delete: key only.
    After,
    /// Identity only on every op.
    Key,
}

#[derive(Debug, Clone)]
pub struct TableSpec<'a> {
    pub name: &'a str,
    pub mode: CaptureMode,
    pub exclude: &'a [&'a str],
}

impl<'a> TableSpec<'a> {
    pub fn new(name: &'a str, mode: CaptureMode) -> Self {
        Self {
            name,
            mode,
            exclude: &[],
        }
    }

    pub const fn exclude(mut self, cols: &'a [&'a str]) -> Self {
        self.exclude = cols;
        self
    }
}

/// Logical database name plus the tables to capture.
pub struct CaptureConfig<'a> {
    pub db_name: &'a str,
    pub sqlite_path: &'a Path,
    pub tables: &'a [TableSpec<'a>],
    /// Override `STATE_CAPTURE_ANNOUNCE_DIR`. `None` uses env / default.
    pub announce_dir: Option<&'a Path>,
    /// Override `STATE_CAPTURE_SOCK`. `None` uses env / default.
    pub sock: Option<&'a Path>,
}

impl<'a> CaptureConfig<'a> {
    pub fn new(db_name: &'a str, sqlite_path: &'a Path, tables: &'a [TableSpec<'a>]) -> Self {
        Self {
            db_name,
            sqlite_path,
            tables,
            announce_dir: None,
            sock: None,
        }
    }
}

/// Fire-and-forget datagram after commit. Errors are ignored.
#[derive(Debug, Clone)]
pub struct Nudge {
    db_name: String,
    sock: PathBuf,
}

impl Nudge {
    pub fn new(db_name: impl Into<String>, sock: Option<&Path>) -> Self {
        let sock = sock
            .map(PathBuf::from)
            .or_else(|| std::env::var(ENV_SOCK).ok().map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCK));
        Self {
            db_name: db_name.into(),
            sock,
        }
    }

    /// Send `db_name` bytes. Never fails visibly — collector may be absent.
    pub fn send(&self) {
        #[cfg(unix)]
        {
            use std::os::unix::net::UnixDatagram;
            if let Ok(sock) = UnixDatagram::unbound() {
                let _ = sock.send_to(self.db_name.as_bytes(), &self.sock);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = &self.sock;
        }
    }
}

/// Install `_outbox`, regenerate capture triggers, assert column lists, announce.
///
/// Never drops `_outbox`. Returns a [`Nudge`] to call after `commit()`; send
/// failures are ignored (collector may be down).
pub fn install(conn: &Connection, cfg: &CaptureConfig<'_>) -> Result<Nudge> {
    validate_db_name(cfg.db_name)?;
    if cfg.tables.is_empty() {
        bail!("capture set is empty");
    }
    ensure_outbox(conn)?;
    install_triggers(conn, cfg.tables)?;
    assert_triggers(conn, cfg.tables)?;
    let _ = announce(cfg.db_name, cfg.sqlite_path, cfg.announce_dir);
    Ok(Nudge::new(cfg.db_name, cfg.sock))
}

/// WAL, synchronous=NORMAL, busy_timeout=5000, foreign_keys=ON.
pub fn apply_runtime_pragmas(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("journal_mode WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("synchronous NORMAL")?;
    conn.busy_timeout(open_timeout()).context("busy_timeout")?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .context("foreign_keys ON")?;
    Ok(())
}

/// Same pragmas as the design spec, plus the usual open timeout.
pub fn open_timeout() -> Duration {
    Duration::from_millis(5_000)
}

/// True when `sqlite_master.sql` for `table` contains `STRICT`.
pub fn table_is_strict(conn: &Connection, table: &str) -> Result<bool> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .optional()
        .with_context(|| format!("sqlite_master sql for {table}"))?;
    Ok(sql.is_some_and(|s| s.to_ascii_uppercase().contains("STRICT")))
}

pub fn ensure_outbox(conn: &Connection) -> Result<()> {
    conn.execute_batch(OUTBOX_DDL).context("create _outbox")?;
    Ok(())
}

pub fn announce_path(db_name: &str) -> PathBuf {
    let dir = std::env::var(ENV_ANNOUNCE_DIR).unwrap_or_else(|_| DEFAULT_ANNOUNCE_DIR.to_string());
    PathBuf::from(dir).join(format!("{db_name}.json"))
}

/// Write `{db_name, sqlite_path}`. Falls back to `{sqlite_dir}/.capturable.json`.
pub fn announce(db_name: &str, sqlite_path: &Path, announce_dir: Option<&Path>) -> Result<PathBuf> {
    let abs = std::fs::canonicalize(sqlite_path)
        .unwrap_or_else(|_| sqlite_path.to_path_buf())
        .display()
        .to_string();
    let body = format!(
        "{{\n  \"db_name\": {},\n  \"sqlite_path\": {}\n}}\n",
        json_string(db_name),
        json_string(&abs)
    );
    let primary = match announce_dir {
        Some(dir) => dir.join(format!("{db_name}.json")),
        None => announce_path(db_name),
    };
    if let Some(parent) = primary.parent() {
        if fs::create_dir_all(parent).is_ok() && fs::write(&primary, &body).is_ok() {
            return Ok(primary);
        }
    }
    let fallback = sqlite_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".capturable.json");
    if let Some(parent) = fallback.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::write(&fallback, body).with_context(|| format!("write {}", fallback.display()))?;
    Ok(fallback)
}

fn json_string(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn validate_db_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        bail!("db_name must be ASCII alphanumeric / hyphen / underscore, got {name:?}")
    }
}

#[derive(Debug)]
struct Col {
    name: String,
    pk: i64,
}

pub fn install_triggers(conn: &Connection, tables: &[TableSpec<'_>]) -> Result<()> {
    for spec in tables {
        if spec.name == OUTBOX_TABLE {
            bail!("refusing to capture {OUTBOX_TABLE}");
        }
        validate_ident(spec.name)?;
        for c in spec.exclude {
            validate_ident(c)?;
        }
        let cols = table_columns(conn, spec.name)?;
        if cols.is_empty() {
            bail!("table {} has no columns (missing?)", spec.name);
        }
        let pk = pk_columns(&cols);
        let without_rowid = is_without_rowid(conn, spec.name)?;
        if pk.is_empty() && without_rowid {
            bail!("{} is WITHOUT ROWID and has no PRIMARY KEY", spec.name);
        }
        drop_capture_triggers(conn, spec.name)?;
        conn.execute_batch(&trigger_sql(spec, &cols, &pk, without_rowid)?)
            .with_context(|| format!("install triggers for {}", spec.name))?;
    }
    Ok(())
}

pub fn assert_triggers(conn: &Connection, tables: &[TableSpec<'_>]) -> Result<()> {
    for spec in tables {
        let cols = table_columns(conn, spec.name)?;
        let payload = payload_columns(&cols, spec.exclude);
        let pk = pk_columns(&cols);
        let without_rowid = is_without_rowid(conn, spec.name)?;
        let key_names: Vec<String> = if pk.is_empty() {
            vec!["rowid".into()]
        } else {
            pk.clone()
        };

        for op in ["I", "U", "D"] {
            let name = trigger_name(op, spec.name);
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
                    [&name],
                    |row| row.get(0),
                )
                .with_context(|| format!("missing trigger {name}"))?;

            for k in &key_names {
                if !sql.contains(&format!("'{k}'")) {
                    bail!("trigger {name} missing key column {k}");
                }
            }

            let expect_after = matches!(
                (op, spec.mode),
                ("I" | "U", CaptureMode::Full | CaptureMode::After)
            );
            let expect_before = matches!((op, spec.mode), ("U" | "D", CaptureMode::Full));

            if expect_after {
                for c in &payload {
                    if !sql.contains(&format!("NEW.\"{c}\"")) && !sql.contains(&format!("NEW.{c}"))
                    {
                        bail!("trigger {name} missing NEW.{c}; regenerate after schema change");
                    }
                }
            }
            if expect_before {
                for c in &payload {
                    if !sql.contains(&format!("OLD.\"{c}\"")) && !sql.contains(&format!("OLD.{c}"))
                    {
                        bail!("trigger {name} missing OLD.{c}; regenerate after schema change");
                    }
                }
            }

            for ex in spec.exclude {
                if key_names.iter().any(|k| k == ex) {
                    continue;
                }
                if sql.contains(&format!("NEW.\"{ex}\"")) || sql.contains(&format!("NEW.{ex}")) {
                    bail!("trigger {name} still captures excluded column {ex}");
                }
            }

            if without_rowid && sql.contains("NEW.rowid") {
                bail!("trigger {name} uses NEW.rowid on WITHOUT ROWID table");
            }
        }
    }
    Ok(())
}

fn trigger_sql(
    spec: &TableSpec<'_>,
    cols: &[Col],
    pk: &[String],
    without_rowid: bool,
) -> Result<String> {
    let payload = payload_columns(cols, spec.exclude);
    let key_new = key_expr(pk, "NEW", without_rowid)?;
    let key_old = key_expr(pk, "OLD", without_rowid)?;
    let after_new = json_object_expr(&payload, "NEW");
    let before_old = json_object_expr(&payload, "OLD");

    let null = "NULL".to_string();
    let (ins_before, ins_after) = match spec.mode {
        CaptureMode::Full | CaptureMode::After => (null.clone(), after_new.clone()),
        CaptureMode::Key => (null.clone(), null.clone()),
    };
    let (upd_before, upd_after) = match spec.mode {
        CaptureMode::Full => (before_old.clone(), after_new),
        CaptureMode::After => (null.clone(), after_new),
        CaptureMode::Key => (null.clone(), null.clone()),
    };
    let (del_before, del_after) = match spec.mode {
        CaptureMode::Full => (before_old, null),
        CaptureMode::After | CaptureMode::Key => (null.clone(), null),
    };

    let t = spec.name;
    Ok(format!(
        r#"
CREATE TRIGGER {ti} AFTER INSERT ON "{t}"
BEGIN
  INSERT INTO {out}(tbl, op, key, before, after)
  VALUES ('{t}', 'I', {key_new}, {ins_before}, {ins_after});
END;
CREATE TRIGGER {tu} AFTER UPDATE ON "{t}"
BEGIN
  INSERT INTO {out}(tbl, op, key, before, after)
  VALUES ('{t}', 'U', {key_new}, {upd_before}, {upd_after});
END;
CREATE TRIGGER {td} AFTER DELETE ON "{t}"
BEGIN
  INSERT INTO {out}(tbl, op, key, before, after)
  VALUES ('{t}', 'D', {key_old}, {del_before}, {del_after});
END;
"#,
        ti = trigger_name("I", t),
        tu = trigger_name("U", t),
        td = trigger_name("D", t),
        out = OUTBOX_TABLE,
    ))
}

fn trigger_name(op: &str, table: &str) -> String {
    format!("_cap_{op}_{table}")
}

fn drop_capture_triggers(conn: &Connection, table: &str) -> Result<()> {
    for op in ["I", "U", "D"] {
        let name = trigger_name(op, table);
        conn.execute(&format!("DROP TRIGGER IF EXISTS \"{name}\""), [])
            .with_context(|| format!("drop {name}"))?;
    }
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<Col>> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
        .with_context(|| format!("table_info {table}"))?;
    let cols = stmt
        .query_map([], |row| {
            Ok(Col {
                name: row.get(1)?,
                pk: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(cols)
}

fn pk_columns(cols: &[Col]) -> Vec<String> {
    let mut pk: Vec<(i64, String)> = cols
        .iter()
        .filter(|c| c.pk > 0)
        .map(|c| (c.pk, c.name.clone()))
        .collect();
    pk.sort_by_key(|(n, _)| *n);
    pk.into_iter().map(|(_, n)| n).collect()
}

fn payload_columns(cols: &[Col], exclude: &[&str]) -> Vec<String> {
    cols.iter()
        .filter(|c| !exclude.iter().any(|e| *e == c.name))
        .map(|c| c.name.clone())
        .collect()
}

fn key_expr(pk: &[String], prefix: &str, without_rowid: bool) -> Result<String> {
    if pk.is_empty() {
        if without_rowid {
            bail!("cannot use rowid on WITHOUT ROWID table");
        }
        return Ok(format!("json_object('rowid', {prefix}.rowid)"));
    }
    Ok(json_object_expr(pk, prefix))
}

fn json_object_expr(cols: &[String], prefix: &str) -> String {
    if cols.is_empty() {
        return "NULL".into();
    }
    let parts: Vec<String> = cols
        .iter()
        .map(|c| format!("'{c}', {prefix}.\"{c}\""))
        .collect();
    format!("json_object({})", parts.join(", "))
}

fn is_without_rowid(conn: &Connection, table: &str) -> Result<bool> {
    let sql: Option<String> = match conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get::<_, String>(0),
    ) {
        Ok(s) => Some(s),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e).context("without-rowid check"),
    };
    Ok(sql.is_some_and(|s| {
        s.to_ascii_uppercase()
            .replace(['\n', '\t'], " ")
            .contains("WITHOUT ROWID")
    }))
}

fn validate_ident(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if ok {
        Ok(())
    } else {
        bail!("invalid SQL identifier {name:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TMP_N: AtomicU64 = AtomicU64::new(0);

    fn tmp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sqlite-capture-{}-{}",
            std::process::id(),
            TMP_N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cap_cfg<'a>(
        db_name: &'a str,
        sqlite_path: &'a Path,
        tables: &'a [TableSpec<'a>],
        dir: &'a Path,
    ) -> CaptureConfig<'a> {
        CaptureConfig {
            db_name,
            sqlite_path,
            tables,
            announce_dir: Some(dir),
            sock: Some(dir),
        }
    }

    fn open_mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        apply_runtime_pragmas(&conn).unwrap();
        conn
    }

    #[test]
    fn outbox_has_autoincrement() {
        let conn = open_mem();
        ensure_outbox(&conn).unwrap();
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name = '_outbox'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let u = sql.to_ascii_uppercase();
        assert!(u.contains("AUTOINCREMENT"));
        assert!(u.contains("STRICT"));
    }

    #[test]
    fn insert_update_delete_one_row_each() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE jobs (
               id INTEGER PRIMARY KEY,
               state TEXT NOT NULL
             ) STRICT;",
        )
        .unwrap();
        let tables = [TableSpec::new("jobs", CaptureMode::Full)];
        let db = dir.join("t.sqlite");
        let cfg = cap_cfg("test-jobs", &db, &tables, &dir);
        install(&conn, &cfg).unwrap();

        conn.execute("INSERT INTO jobs(id, state) VALUES (1, 'queued')", [])
            .unwrap();
        conn.execute("UPDATE jobs SET state = 'done' WHERE id = 1", [])
            .unwrap();
        conn.execute("DELETE FROM jobs WHERE id = 1", []).unwrap();

        let rows: Vec<(String, String, String)> = {
            let mut stmt = conn
                .prepare("SELECT tbl, op, key FROM _outbox ORDER BY seq")
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "jobs");
        assert_eq!(rows[0].1, "I");
        assert_eq!(rows[1].1, "U");
        assert_eq!(rows[2].1, "D");
        for (_, _, key) in &rows {
            assert!(key.contains("\"id\""));
            assert!(!key.is_empty());
        }

        let (before, after): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT before, after FROM _outbox WHERE op = 'U'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(before.unwrap().contains("queued"));
        assert!(after.unwrap().contains("done"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rollback_produces_zero_outbox_rows() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.join("t.sqlite");
        install(&conn, &cap_cfg("rollback-test", &db, &tables, &dir)).unwrap();
        let mut conn = conn;
        {
            let tx = conn.transaction().unwrap();
            tx.execute("INSERT INTO t(id, v) VALUES (1, 'x')", [])
                .unwrap();
            tx.rollback().unwrap();
        }
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn reinstall_does_not_reset_outbox_seq() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.join("t.sqlite");
        let cfg = cap_cfg("seq-test", &db, &tables, &dir);
        install(&conn, &cfg).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (2, 'b')", [])
            .unwrap();
        let hi: i64 = conn
            .query_row("SELECT MAX(seq) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(hi, 2);
        install(&conn, &cfg).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (3, 'c')", [])
            .unwrap();
        let next: i64 = conn
            .query_row("SELECT MAX(seq) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(next, 3);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn exclude_drops_column_from_payload_keeps_key() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE t (
               id INTEGER PRIMARY KEY,
               v TEXT,
               blob TEXT
             ) STRICT;",
        )
        .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full).exclude(&["blob"])];
        let db = dir.join("t.sqlite");
        install(&conn, &cap_cfg("exclude-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v, blob) VALUES (1, 'ok', 'secret')", [])
            .unwrap();
        let after: String = conn
            .query_row("SELECT after FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(after.contains("ok"));
        assert!(!after.contains("secret"));
        assert!(!after.contains("blob"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn after_mode_omits_before_on_update() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::After)];
        let db = dir.join("t.sqlite");
        install(&conn, &cap_cfg("after-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute("UPDATE t SET v = 'b' WHERE id = 1", [])
            .unwrap();
        let before: Option<String> = conn
            .query_row("SELECT before FROM _outbox WHERE op = 'U'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(before.is_none());
        let after: String = conn
            .query_row("SELECT after FROM _outbox WHERE op = 'U'", [], |r| r.get(0))
            .unwrap();
        assert!(after.contains("b"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn key_mode_payloads_null() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Key)];
        let db = dir.join("t.sqlite");
        install(&conn, &cap_cfg("key-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        let (before, after): (Option<String>, Option<String>) = conn
            .query_row("SELECT before, after FROM _outbox", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert!(before.is_none());
        assert!(after.is_none());
        let key: String = conn
            .query_row("SELECT key FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(key.contains("\"id\""));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn composite_key() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch(
            "CREATE TABLE trips (
               icao24 TEXT NOT NULL,
               dep_ts TEXT NOT NULL,
               ticker TEXT,
               PRIMARY KEY (icao24, dep_ts)
             ) STRICT;",
        )
        .unwrap();
        let tables = [TableSpec::new("trips", CaptureMode::Full)];
        let db = dir.join("t.sqlite");
        install(&conn, &cap_cfg("trips-test", &db, &tables, &dir)).unwrap();
        conn.execute(
            "INSERT INTO trips(icao24, dep_ts, ticker) VALUES ('abc','2026-09-01T00:00:00Z','X')",
            [],
        )
        .unwrap();
        let key: String = conn
            .query_row("SELECT key FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert!(key.contains("icao24"));
        assert!(key.contains("dep_ts"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn upsert_emits_insert_then_update() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.join("t.sqlite");
        install(&conn, &cap_cfg("upsert-test", &db, &tables, &dir)).unwrap();
        conn.execute(
            "INSERT INTO t(id, v) VALUES (1, 'a')
             ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO t(id, v) VALUES (1, 'b')
             ON CONFLICT(id) DO UPDATE SET v = excluded.v",
            [],
        )
        .unwrap();
        let ops: Vec<String> = {
            let mut stmt = conn.prepare("SELECT op FROM _outbox ORDER BY seq").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(ops, vec!["I", "U"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn replace_is_two_inserts_no_delete() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Full)];
        let db = dir.join("t.sqlite");
        install(&conn, &cap_cfg("replace-test", &db, &tables, &dir)).unwrap();
        conn.execute("INSERT INTO t(id, v) VALUES (1, 'a')", [])
            .unwrap();
        conn.execute("INSERT OR REPLACE INTO t(id, v) VALUES (1, 'b')", [])
            .unwrap();
        let ops: Vec<String> = {
            let mut stmt = conn.prepare("SELECT op FROM _outbox ORDER BY seq").unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(ops, vec!["I", "I"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn table_is_strict_detects() {
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE a (id INTEGER PRIMARY KEY) STRICT;")
            .unwrap();
        conn.execute_batch("CREATE TABLE b (id INTEGER PRIMARY KEY);")
            .unwrap();
        assert!(table_is_strict(&conn, "a").unwrap());
        assert!(!table_is_strict(&conn, "b").unwrap());
        assert!(!table_is_strict(&conn, "missing").unwrap());
    }

    #[test]
    fn announce_file_written() {
        let dir = tmp_dir();
        let conn = open_mem();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT;")
            .unwrap();
        let tables = [TableSpec::new("t", CaptureMode::Key)];
        let db = dir.join("feed.sqlite");
        std::fs::write(&db, b"").unwrap();
        install(&conn, &cap_cfg("announce-test", &db, &tables, &dir)).unwrap();
        let p = dir.join("announce-test.json");
        let body = std::fs::read_to_string(p).unwrap();
        assert!(body.contains("announce-test"));
        assert!(body.contains("feed.sqlite"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn validate_db_name_rejects_slash() {
        assert!(validate_db_name("adsb-trip-journal").is_ok());
        assert!(validate_db_name("no/slash").is_err());
    }

    #[test]
    fn pragmas_wal() {
        let dir = tmp_dir();
        let path = dir.join("p.sqlite");
        let conn = Connection::open(&path).unwrap();
        apply_runtime_pragmas(&conn).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_ascii_lowercase(), "wal");
        drop(conn);
        let _ = std::fs::remove_dir_all(dir);
    }
}

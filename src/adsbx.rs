//! ADS-B Exchange HTTP client: probe + serialized hex-day fetches.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

const DEFAULT_LIVE_BASE: &str = "https://gateway.adsbexchange.com/api/aircraft/v2";
const DEFAULT_HIST_BASE: &str = "https://gateway.adsbexchange.com/api/aircraft/v2";
const DEFAULT_RAPIDAPI_HOST: &str = "adsbexchange-com1.p.rapidapi.com";

#[derive(Debug, Clone)]
pub struct AdsBxConfig {
    pub api_key: Option<String>,
    pub rapidapi_key: Option<String>,
    pub rapidapi_host: String,
    pub live_base: String,
    pub hist_base: String,
    pub s3_bucket: Option<String>,
    pub timeout: Duration,
}

impl AdsBxConfig {
    pub fn from_env() -> Self {
        Self {
            api_key: empty_to_none(std::env::var("ADSBX_API_KEY").ok()),
            rapidapi_key: empty_to_none(std::env::var("RAPIDAPI_KEY").ok()),
            rapidapi_host: std::env::var("RAPIDAPI_HOST")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| DEFAULT_RAPIDAPI_HOST.to_string()),
            live_base: std::env::var("ADSBX_LIVE_BASE")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| DEFAULT_LIVE_BASE.to_string()),
            hist_base: std::env::var("ADSBX_HIST_BASE")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| DEFAULT_HIST_BASE.to_string()),
            s3_bucket: empty_to_none(std::env::var("ADSBX_S3_BUCKET").ok()),
            timeout: Duration::from_secs(60),
        }
    }

    pub fn has_credentials(&self) -> bool {
        self.api_key.is_some() || self.rapidapi_key.is_some()
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

#[derive(Debug, Clone)]
pub struct AdsBxClient {
    http: reqwest::Client,
    cfg: AdsBxConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchOutcome {
    Ok {
        body: Vec<u8>,
    },
    NotFound,
    /// 401 / 402 / 403 — do not advance cursor.
    Denied {
        status: u16,
        message: String,
    },
    /// 429 — do not advance cursor.
    RateLimited {
        retry_after: Option<u64>,
    },
    Other {
        status: u16,
        message: String,
    },
}

impl FetchOutcome {
    pub fn advances_cursor(&self) -> bool {
        matches!(self, Self::Ok { .. } | Self::NotFound)
    }

    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::Denied { status, message } => Some(format!("HTTP {status}: {message}")),
            Self::RateLimited { retry_after } => Some(format!(
                "HTTP 429{}",
                retry_after
                    .map(|s| format!(" retry-after={s}s"))
                    .unwrap_or_default()
            )),
            Self::Other { status, message } => Some(format!("HTTP {status}: {message}")),
            Self::Ok { .. } | Self::NotFound => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointProbe {
    pub name: String,
    pub url: String,
    pub status: Option<u16>,
    pub ok: bool,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeReport {
    pub hex: String,
    pub date: String,
    pub credentials_present: bool,
    pub live: EndpointProbe,
    pub recent: Vec<EndpointProbe>,
    pub hist: EndpointProbe,
    pub s3: EndpointProbe,
    pub hist_available: bool,
    pub recent_available: bool,
    pub live_available: bool,
}

impl AdsBxClient {
    pub fn new(cfg: AdsBxConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );
        if let Some(ref k) = cfg.api_key {
            if let Ok(v) = HeaderValue::from_str(k) {
                headers.insert("api-auth", v);
            }
        }
        if let Some(ref k) = cfg.rapidapi_key {
            if let Ok(v) = HeaderValue::from_str(k) {
                headers.insert("X-RapidAPI-Key", v);
            }
            if let Ok(v) = HeaderValue::from_str(&cfg.rapidapi_host) {
                headers.insert("X-RapidAPI-Host", v);
            }
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .gzip(true)
            .timeout(cfg.timeout)
            .user_agent("adsb-trip-journal/0.1")
            .build()
            .context("build HTTP client")?;
        Ok(Self { http, cfg })
    }

    pub fn config(&self) -> &AdsBxConfig {
        &self.cfg
    }

    pub fn last_two(icao24: &str) -> &str {
        let s = icao24.trim();
        if s.len() >= 2 {
            &s[s.len() - 2..]
        } else {
            s
        }
    }

    pub fn hist_url(&self, icao24: &str, date: NaiveDate) -> String {
        let hex = icao24.to_ascii_lowercase();
        let last2 = Self::last_two(&hex);
        let y = date.format("%Y");
        let m = date.format("%m");
        let d = date.format("%d");
        format!(
            "{}/traces-hist/{y}/{m}/{d}/traces/{last2}/trace_full_{hex}.json",
            self.cfg.hist_base.trim_end_matches('/')
        )
    }

    pub fn live_url(&self, icao24: &str) -> String {
        format!("{}/icao/{icao24}", self.cfg.live_base.trim_end_matches('/'))
    }

    pub fn live_batch_url(&self, hexes: &[String]) -> String {
        let joined = hexes.join(",");
        format!("{}/icao/{joined}", self.cfg.live_base.trim_end_matches('/'))
    }

    pub fn recent_urls(&self, icao24: &str) -> Vec<(String, String)> {
        let hex = icao24.to_ascii_lowercase();
        let last2 = Self::last_two(&hex).to_string();
        let base = self.cfg.live_base.trim_end_matches('/');
        vec![
            (
                "recent_trace".into(),
                format!("{base}/traces/{last2}/trace_recent_{hex}.json"),
            ),
            (
                "recent_full".into(),
                format!("{base}/traces/{last2}/trace_full_{hex}.json"),
            ),
            ("api_trace".into(), format!("{base}/trace/{hex}.json")),
        ]
    }

    pub async fn probe(&self, icao24: &str, date: NaiveDate) -> Result<ProbeReport> {
        let hex = icao24.to_ascii_lowercase();
        let live = self.probe_url("live_icao", &self.live_url(&hex)).await;
        let mut recent = Vec::new();
        for (name, url) in self.recent_urls(&hex) {
            recent.push(self.probe_url(&name, &url).await);
        }
        let hist = self
            .probe_url("hist_trace", &self.hist_url(&hex, date))
            .await;
        let s3 = match &self.cfg.s3_bucket {
            Some(b) => EndpointProbe {
                name: "s3".into(),
                url: format!("s3://{b}"),
                status: None,
                ok: false,
                note: "bucket configured; v1 does not list or download S3 objects".into(),
            },
            None => EndpointProbe {
                name: "s3".into(),
                url: "".into(),
                status: None,
                ok: false,
                note: "ADSBX_S3_BUCKET not set".into(),
            },
        };
        Ok(ProbeReport {
            hex: hex.clone(),
            date: date.to_string(),
            credentials_present: self.cfg.has_credentials(),
            live_available: live.ok,
            recent_available: recent.iter().any(|e| e.ok),
            hist_available: hist.ok,
            live,
            recent,
            hist,
            s3,
        })
    }

    async fn probe_url(&self, name: &str, url: &str) -> EndpointProbe {
        match self.http.get(url).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let ok = resp.status().is_success();
                let note = if ok {
                    "200-class".into()
                } else {
                    format!("{}", resp.status())
                };
                EndpointProbe {
                    name: name.into(),
                    url: url.into(),
                    status: Some(status),
                    ok,
                    note,
                }
            }
            Err(e) => EndpointProbe {
                name: name.into(),
                url: url.into(),
                status: None,
                ok: false,
                note: format!("transport: {e}"),
            },
        }
    }

    pub async fn fetch_hist(&self, icao24: &str, date: NaiveDate) -> Result<FetchOutcome> {
        self.fetch_url(&self.hist_url(icao24, date)).await
    }

    pub async fn fetch_recent(&self, icao24: &str) -> Result<FetchOutcome> {
        let mut last = FetchOutcome::Other {
            status: 0,
            message: "no recent URLs".into(),
        };
        for (_name, url) in self.recent_urls(icao24) {
            last = self.fetch_url(&url).await?;
            if matches!(last, FetchOutcome::Ok { .. } | FetchOutcome::Denied { .. }) {
                return Ok(last);
            }
        }
        Ok(last)
    }

    pub async fn fetch_live_batch(&self, hexes: &[String]) -> Result<FetchOutcome> {
        if hexes.is_empty() {
            return Ok(FetchOutcome::Ok {
                body: b"{\"ac\":[]}".to_vec(),
            });
        }
        // URL length: batch in chunks of 40 hexes if needed.
        if hexes.len() <= 40 {
            return self.fetch_url(&self.live_batch_url(hexes)).await;
        }
        let mut all: Vec<serde_json::Value> = Vec::new();
        for chunk in hexes.chunks(40) {
            match self.fetch_url(&self.live_batch_url(chunk)).await? {
                FetchOutcome::Ok { body } => {
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
                        if let Some(arr) = v.get("ac").and_then(|a| a.as_array()) {
                            all.extend(arr.iter().cloned());
                        }
                    }
                }
                other => return Ok(other),
            }
        }
        let wrapped = serde_json::json!({ "ac": all });
        Ok(FetchOutcome::Ok {
            body: serde_json::to_vec(&wrapped)?,
        })
    }

    pub async fn fetch_url(&self, url: &str) -> Result<FetchOutcome> {
        let resp = match self.http.get(url).send().await {
            Ok(r) => r,
            Err(e) => {
                return Ok(FetchOutcome::Other {
                    status: 0,
                    message: format!("transport: {e}"),
                })
            }
        };
        let status = resp.status();
        let retry_after = parse_retry_after(resp.headers().get(RETRY_AFTER));
        let bytes = resp.bytes().await.unwrap_or_default();
        let message = String::from_utf8_lossy(&bytes)
            .chars()
            .take(200)
            .collect::<String>();
        Ok(classify(status, bytes.to_vec(), message, retry_after))
    }
}

fn classify(
    status: StatusCode,
    body: Vec<u8>,
    message: String,
    retry_after: Option<u64>,
) -> FetchOutcome {
    match status.as_u16() {
        200..=299 => FetchOutcome::Ok { body },
        404 => FetchOutcome::NotFound,
        401..=403 => FetchOutcome::Denied {
            status: status.as_u16(),
            message,
        },
        429 => FetchOutcome::RateLimited { retry_after },
        other => FetchOutcome::Other {
            status: other,
            message,
        },
    }
}

fn parse_retry_after(h: Option<&HeaderValue>) -> Option<u64> {
    h.and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_two_hex() {
        assert_eq!(AdsBxClient::last_two("abcdef"), "ef");
        assert_eq!(AdsBxClient::last_two("ab"), "ab");
    }

    #[test]
    fn hist_url_shape() {
        let cfg = AdsBxConfig {
            api_key: None,
            rapidapi_key: None,
            rapidapi_host: DEFAULT_RAPIDAPI_HOST.into(),
            live_base: DEFAULT_LIVE_BASE.into(),
            hist_base: DEFAULT_HIST_BASE.into(),
            s3_bucket: None,
            timeout: Duration::from_secs(5),
        };
        let c = AdsBxClient::new(cfg).unwrap();
        let d = NaiveDate::from_ymd_opt(2023, 1, 22).unwrap();
        let u = c.hist_url("A1B2C3", d);
        assert!(u.ends_with("/traces-hist/2023/01/22/traces/c3/trace_full_a1b2c3.json"));
    }

    #[test]
    fn classify_cursor_rules() {
        assert!(FetchOutcome::NotFound.advances_cursor());
        assert!(FetchOutcome::Ok { body: vec![] }.advances_cursor());
        assert!(!FetchOutcome::Denied {
            status: 403,
            message: "no".into()
        }
        .advances_cursor());
        assert!(!FetchOutcome::RateLimited {
            retry_after: Some(30)
        }
        .advances_cursor());
    }
}

use axum::{
    extract::{Path, Query},
    response::{IntoResponse, Json, Response},
    http::{StatusCode, header},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use chrono::Utc;
use rand::prelude::Rng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use reqwest::Client;
use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;  // added for async read/write lock
use once_cell::sync::Lazy;
use axum::body::Body;
use std::collections::{HashMap, HashSet};
use std::path::Path as StdPath;
use anyhow::Result;
use tokio::fs;
use axum::http::HeaderMap;
use tokio::process::Command as TokioCommand;
// Configuration loaded from `config.json` via `crate::CONFIG` in main.rs
// Load balancer / instance manager

struct InstanceManager {
    mirrors: RwLock<Vec<String>>,
    primary_requests: AtomicU64,
    primary_index: AtomicU64,
    window_start: AtomicU64,
    last_refresh: AtomicU64,
}

impl InstanceManager {
    fn new() -> Self {
        Self {
            mirrors: RwLock::new(Vec::new()),
            primary_requests: AtomicU64::new(0),
            primary_index: AtomicU64::new(0),
            window_start: AtomicU64::new(0),
            last_refresh: AtomicU64::new(0),
        }
    }

    async fn pick(&self, client: &Client) -> String {
        self.maybe_refresh(client).await;

        let now = Utc::now().timestamp() as u64;

        let window = self.window_start.load(Ordering::Relaxed);
        if now.saturating_sub(window) >= 60 {
            self.window_start.store(now, Ordering::Relaxed);
            self.primary_requests.store(0, Ordering::Relaxed);
        }

        let primary_instances = if crate::CONFIG.primary_instances.is_empty() {
            vec![crate::CONFIG.primary_instance.clone()]
        } else {
            crate::CONFIG.primary_instances.clone()
        };
        let primary_count = primary_instances.len() as u64;

        let rpm = self.primary_requests.fetch_add(1, Ordering::Relaxed) + 1;
        if rpm <= crate::CONFIG.primary_rpm_limit.saturating_mul(primary_count) {
            let idx = self.primary_index.fetch_add(1, Ordering::Relaxed) as usize;
            let chosen_primary = primary_instances[idx % primary_instances.len()].clone();
            return chosen_primary;
        }

        let mirrors = self.mirrors.read().await;
        if mirrors.is_empty() {
            let idx = self.primary_index.fetch_add(1, Ordering::Relaxed) as usize;
            return primary_instances[idx % primary_instances.len()].clone();
        }
        let mut rng = rand::thread_rng();
        let chosen = mirrors.choose(&mut rng).cloned().unwrap_or_else(|| {
            let idx = self.primary_index.fetch_add(1, Ordering::Relaxed) as usize;
            primary_instances[idx % primary_instances.len()].clone()
        });
        info!("Using mirror instance: {}", chosen);
        chosen
    }

    async fn all_instances(&self, client: &Client) -> Vec<String> {
        self.maybe_refresh(client).await;
        let mirrors = self.mirrors.read().await;
        let mut instances = Vec::new();
        let primary_instances = if crate::CONFIG.primary_instances.is_empty() {
            vec![crate::CONFIG.primary_instance.clone()]
        } else {
            crate::CONFIG.primary_instances.clone()
        };
        for primary in primary_instances.iter() {
            if !primary.is_empty() && !instances.contains(primary) {
                instances.push(primary.clone());
            }
        }
        for mirror in mirrors.iter() {
            if !instances.contains(mirror) {
                instances.push(mirror.clone());
            }
        }
        instances
    }

    async fn maybe_refresh(&self, client: &Client) {
        let now = Utc::now().timestamp() as u64;
        let last = self.last_refresh.load(Ordering::Relaxed);
        if now.saturating_sub(last) < crate::CONFIG.instances_refresh_secs {
            return;
        }

        match client.get(&crate::CONFIG.instances_list_url).timeout(std::time::Duration::from_secs(5)).send().await {
            Ok(resp) => {
                match resp.json::<serde_json::Value>().await {
                    Ok(json) => {
                        if let Some(arr) = json.get("invidious").and_then(|v| v.as_array()) {
                            let new_mirrors: Vec<String> = arr
                                .iter()
                                .filter_map(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                                .map(|s| s.trim_end_matches('/').to_string())
                                .collect();
                            info!("Refreshed instance list: {} mirrors", new_mirrors.len());
                            let mut w = self.mirrors.write().await;
                            *w = new_mirrors;
                            self.last_refresh.store(now, Ordering::Relaxed);
                        }
                    }
                    Err(e) => warn!("Failed to parse instances list: {}", e),
                }
            }
            Err(e) => warn!("Failed to fetch instances list: {}", e),
        }
    }
}

static INSTANCE_MANAGER: Lazy<Arc<InstanceManager>> =
    Lazy::new(|| Arc::new(InstanceManager::new()));

static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .pool_max_idle_per_host(20)
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(4))
        .tcp_keepalive(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client")
});

const STATS_FILE: &str = "stats.json";

// Simple in-memory analytics collector
#[derive(Default)]
struct Analytics {
    events: Vec<AnalyticsEvent>,
    registered_devices: HashSet<String>,
}

static PERSISTED_STATS: Lazy<Arc<RwLock<Option<StatsResponse>>>> = Lazy::new(|| Arc::new(RwLock::new(None)));

#[derive(Clone)]
struct AnalyticsEvent {
    path: String,
    user_agent: String,
    country: String,
    ts: i64,
}

static ANALYTICS: Lazy<Arc<RwLock<Analytics>>> = Lazy::new(|| Arc::new(RwLock::new(Analytics::default())));

pub async fn record_request(req: axum::http::Request<Body>, next: axum::middleware::Next) -> Response {
    let path = req.uri().path().to_string();
    let headers = req.headers();
    let ua = headers.get(axum::http::header::USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("Unknown").to_string();
    let country = headers.get("cf-ipcountry").or_else(|| headers.get("CF-IPCountry")).and_then(|v| v.to_str().ok()).unwrap_or("unknown").to_string();
    let ts = Utc::now().timestamp();

    // record event
    let event = AnalyticsEvent { path: path.clone(), user_agent: ua.clone(), country: country.clone(), ts };
    {
        let mut a = ANALYTICS.write().await;
        a.events.push(event);
    }

    next.run(req).await
}

#[derive(Clone, Serialize, Deserialize)]
struct TopItem {
    key: String,
    count: usize,
}

#[derive(Clone, Serialize, Deserialize)]
struct WindowStats {
    total_events: usize,
    registered_devices: usize,
    top_endpoints: Vec<TopItem>,
    top_user_agents: Vec<TopItem>,
    top_countries: Vec<TopItem>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StatsResponse {
    overall: WindowStats,
    last_7_days: WindowStats,
    last_30_days: WindowStats,
}

fn top_n_from_counts(counts: HashMap<String, usize>, n: usize) -> Vec<TopItem> {
    let mut v: Vec<(String, usize)> = counts.into_iter().collect();
    v.sort_by(|a,b| b.1.cmp(&a.1));
    v.into_iter().take(n).map(|(k,c)| TopItem{ key: k, count: c }).collect()
}

struct WindowCounts {
    total_events: usize,
    endpoints: HashMap<String, usize>,
    user_agents: HashMap<String, usize>,
    countries: HashMap<String, usize>,
}

fn compute_window_counts(events: &[AnalyticsEvent], since_ts: Option<i64>) -> WindowCounts {
    let mut counts = WindowCounts {
        total_events: 0,
        endpoints: HashMap::new(),
        user_agents: HashMap::new(),
        countries: HashMap::new(),
    };

    for e in events.iter() {
        if let Some(s) = since_ts {
            if e.ts < s { continue; }
        }
        counts.total_events += 1;
        *counts.endpoints.entry(e.path.clone()).or_default() += 1;
        *counts.user_agents.entry(e.user_agent.clone()).or_default() += 1;
        *counts.countries.entry(e.country.clone()).or_default() += 1;
    }

    counts
}

fn counts_from_top_items(items: &[TopItem]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for item in items {
        counts.insert(item.key.clone(), item.count);
    }
    counts
}

fn merge_counts(base: HashMap<String, usize>, extra: HashMap<String, usize>) -> HashMap<String, usize> {
    let mut merged = base;
    for (key, count) in extra {
        *merged.entry(key).or_default() += count;
    }
    merged
}

fn window_stats_from_counts(counts: WindowCounts, registered_devices: usize) -> WindowStats {
    WindowStats {
        total_events: counts.total_events,
        registered_devices,
        top_endpoints: top_n_from_counts(counts.endpoints, 10),
        top_user_agents: top_n_from_counts(counts.user_agents, 10),
        top_countries: top_n_from_counts(counts.countries, 10),
    }
}

fn merge_window_stats(base: &WindowStats, runtime: &WindowCounts, runtime_registered_devices: usize) -> WindowStats {
    let endpoint_counts = merge_counts(counts_from_top_items(&base.top_endpoints), runtime.endpoints.clone());
    let ua_counts = merge_counts(counts_from_top_items(&base.top_user_agents), runtime.user_agents.clone());
    let country_counts = merge_counts(counts_from_top_items(&base.top_countries), runtime.countries.clone());

    WindowStats {
        total_events: base.total_events + runtime.total_events,
        registered_devices: base.registered_devices + runtime_registered_devices,
        top_endpoints: top_n_from_counts(endpoint_counts, 10),
        top_user_agents: top_n_from_counts(ua_counts, 10),
        top_countries: top_n_from_counts(country_counts, 10),
    }
}

async fn load_persisted_stats() -> Result<Option<StatsResponse>> {
    if !StdPath::new(STATS_FILE).exists() {
        return Ok(None);
    }

    let data = fs::read_to_string(STATS_FILE).await?;
    let stats: StatsResponse = serde_json::from_str(&data)?;
    Ok(Some(stats))
}

async fn save_stats_file(stats: &StatsResponse) -> Result<()> {
    let json = serde_json::to_string_pretty(stats)?;
    fs::write(STATS_FILE, json).await?;
    Ok(())
}

fn compute_stats_response(base: &Option<StatsResponse>, events: &[AnalyticsEvent], runtime_registered_devices: usize) -> StatsResponse {
    let now = Utc::now().timestamp();
    let overall_counts = compute_window_counts(events, None);
    let last_7_counts = compute_window_counts(events, Some(now - 7 * 24 * 3600));
    let last_30_counts = compute_window_counts(events, Some(now - 30 * 24 * 3600));

    let base_overall = base.as_ref().map(|b| &b.overall);
    let base_7 = base.as_ref().map(|b| &b.last_7_days);
    let base_30 = base.as_ref().map(|b| &b.last_30_days);

    let overall = if let Some(base_stats) = base_overall {
        merge_window_stats(base_stats, &overall_counts, runtime_registered_devices)
    } else {
        window_stats_from_counts(overall_counts, runtime_registered_devices)
    };

    let last_7 = if let Some(base_stats) = base_7 {
        merge_window_stats(base_stats, &last_7_counts, runtime_registered_devices)
    } else {
        window_stats_from_counts(last_7_counts, runtime_registered_devices)
    };

    let last_30 = if let Some(base_stats) = base_30 {
        merge_window_stats(base_stats, &last_30_counts, runtime_registered_devices)
    } else {
        window_stats_from_counts(last_30_counts, runtime_registered_devices)
    };

    StatsResponse { overall, last_7_days: last_7, last_30_days: last_30 }
}

pub async fn init_stats() -> Result<()> {
    let loaded = load_persisted_stats().await?;
    let mut persisted = PERSISTED_STATS.write().await;
    *persisted = loaded;
    Ok(())
}

pub async fn save_stats_snapshot() -> Result<()> {
    let persisted = PERSISTED_STATS.read().await;
    let a = ANALYTICS.read().await;
    let stats = compute_stats_response(&*persisted, &a.events, a.registered_devices.len());
    save_stats_file(&stats).await
}

async fn fetch_invidious_json(client: &Client, path: &str) -> Option<(serde_json::Value, String)> {
    let instances = INSTANCE_MANAGER.all_instances(client).await;
    for instance in instances {
        let url = format!("{}/api/v1{}", instance.trim_end_matches('/'), path);
        match client.get(&url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!("Invidious request failed: {} status={}{}", url, resp.status(), if resp.status().is_server_error() { " (server error)" } else { "" });
                    continue;
                }
                match resp.json::<serde_json::Value>().await {
                    Ok(json) => return Some((json, instance.clone())),
                    Err(e) => {
                        warn!("Failed to parse JSON from {}: {}", url, e);
                        continue;
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch {} from Invidious instance {}: {}", path, instance, e);
                continue;
            }
        }
    }
    None
}

// Data structures

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    #[serde(rename = "start-index")]
    start_index: Option<usize>,
    #[serde(rename = "max-results")]
    max_results: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct SearchParams {
    q: Option<String>,
    #[serde(rename = "start-index")]
    start_index: Option<usize>,
    #[serde(rename = "max-results")]
    max_results: Option<usize>,
    #[serde(flatten)]
    _extra: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
struct InvidiousVideo {
    #[serde(rename = "videoId")]
    video_id: String,
    title: String,
    author: String,
    #[serde(rename = "authorId", default)]
    author_id: String,
    #[serde(rename = "lengthSeconds", default)]
    length_seconds: i32,
    #[serde(rename = "viewCount", default)]
    view_count: i64,
    #[serde(rename = "publishedText", default)]
    published_text: String,
    #[serde(default)]
    published: i64,
    /// Alternatively the API may return the date as a string in this field
    #[serde(rename = "uploadedText", default)]
    uploaded_text: String,
    #[serde(rename = "liveNow")]
    live_now: Option<bool>,
    #[serde(default)]
    likes: i64,
    #[serde(default)]
    dislikes: i64,
    #[serde(rename = "commentCount", default)]
    comment_count: i64,
}

pub async fn get_hd_video(Query(params): Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("id") else {
        return (StatusCode::BAD_REQUEST, "Missing id").into_response();
    };
    
    let format = params.get("format").map(|s| s.as_str()).unwrap_or("mp4");
    
    info!("GET_HD_VIDEO id={} format={}", video_id, format);
    
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    
    let url = format!("{}/api/v1/videos/{}", instance, video_id);
    
    match client.get(&url).send().await {
        Ok(resp) => {
            match resp.json::<serde_json::Value>().await {
                Ok(data) => {
                    let duration = data.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
                    
                    // Search formatStreams (audio+video)
                    if let Some(formats) = data.get("formatStreams").and_then(|v| v.as_array()) {
                        for fmt in formats {
                            let quality = fmt.get("qualityLabel").and_then(|v| v.as_str()).unwrap_or("");
                            let mime = fmt.get("type").and_then(|v| v.as_str()).unwrap_or("");
                                    if (quality == "1080p" || quality == "720p") && mime.contains("mp4a") {
                                if let Some(video_url) = fmt.get("url").and_then(|v| v.as_str()) {
                                            info!("Found {}p with audio in formatStreams", quality);
                                    return axum::response::Redirect::temporary(video_url).into_response();
                                }
                            }
                        }
                    }
                    
                    // Search 1080p in adaptiveFormats
                    let mut v_url: Option<String> = None;
                    let mut a_url: Option<String> = None;
                    
                    if let Some(formats) = data.get("adaptiveFormats").and_then(|v| v.as_array()) {
                        for fmt in formats {
                            let resolution = fmt.get("resolution").and_then(|v| v.as_str()).unwrap_or("");
                            let mime = fmt.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            
                            if resolution == "1080p" && mime.starts_with("video/mp4") {
                                if v_url.is_none() || mime.contains("avc1") {
                                    v_url = fmt.get("url").and_then(|v| v.as_str()).map(String::from);
                                }
                            }
                            if mime.starts_with("audio/mp4") && a_url.is_none() {
                                a_url = fmt.get("url").and_then(|v| v.as_str()).map(String::from);
                            }
                        }
                    }
                    
                    // Mux if both video and audio are present
                    if let (Some(ref video_ref), Some(ref audio_ref)) = (&v_url, &a_url) {
                        let v = video_ref.clone();
                        let a = audio_ref.clone();
                        info!("Muxing 1080p as {} for {}", format, video_id);

                        match format {
                            "mkv" => {
                                let response = Response::builder()
                                    .header("Content-Type", "video/x-matroska")
                                    .body(Body::from_stream(async_stream::stream! {
                                        let duration_metadata = format!("DURATION={}", duration * 1000);
                                        let mut child = match TokioCommand::new("ffmpeg")
                                            .arg("-i").arg(&v)
                                            .arg("-i").arg(&a)
                                            .arg("-c:v").arg("copy")
                                            .arg("-c:a").arg("copy")
                                            .arg("-metadata").arg(&duration_metadata)
                                            .arg("-f").arg("matroska")
                                            .arg("-bufsize").arg("8192k")
                                            .arg("pipe:1")
                                            .stdout(std::process::Stdio::piped())
                                            .stderr(std::process::Stdio::null())
                                            .spawn()
                                        {
                                            Ok(c) => c,
                                            Err(e) => {
                                                yield Err(std::io::Error::new(std::io::ErrorKind::Other, format!("ffmpeg spawn error: {}", e)));
                                                return;
                                            }
                                        };
                                        if let Some(mut stdout) = child.stdout.take() {
                                            let mut buf = vec![0u8; 65536];
                                            loop {
                                                match tokio::io::AsyncReadExt::read(&mut stdout, &mut buf).await {
                                                    Ok(0) => break,
                                                    Ok(n) => yield Ok(bytes::Bytes::from(buf[..n].to_vec())),
                                                    Err(e) => { yield Err(e); break; }
                                                }
                                            }
                                        } else {
                                            yield Err(std::io::Error::new(std::io::ErrorKind::Other, "ffmpeg stdout unavailable"));
                                        }
                                        let _ = child.wait().await;
                                    })).unwrap();
                                return response.into_response();
                            }
                            _ => {
                                let response = Response::builder()
                                    .header("Content-Type", "video/mp4")
                                    .body(Body::from_stream(async_stream::stream! {
                                        let mut child = match TokioCommand::new("ffmpeg")
                                            .args(["-i", &v, "-i", &a, "-c:v", "copy", "-c:a", "copy",
                                                   "-movflags", "frag_keyframe+empty_moov+faststart",
                                                   "-f", "mp4", "-bufsize", "8192k", "pipe:1"])
                                            .stdout(std::process::Stdio::piped())
                                            .stderr(std::process::Stdio::null())
                                            .spawn()
                                        {
                                            Ok(c) => c,
                                            Err(e) => {
                                                yield Err(std::io::Error::new(std::io::ErrorKind::Other, format!("ffmpeg spawn error: {}", e)));
                                                return;
                                            }
                                        };
                                        if let Some(mut stdout) = child.stdout.take() {
                                            let mut buf = vec![0u8; 65536];
                                            loop {
                                                match tokio::io::AsyncReadExt::read(&mut stdout, &mut buf).await {
                                                    Ok(0) => break,
                                                    Ok(n) => yield Ok(bytes::Bytes::from(buf[..n].to_vec())),
                                                    Err(e) => { yield Err(e); break; }
                                                }
                                            }
                                        } else {
                                            yield Err(std::io::Error::new(std::io::ErrorKind::Other, "ffmpeg stdout unavailable"));
                                        }
                                        let _ = child.wait().await;
                                    })).unwrap();
                                return response.into_response();
                            }
                        }
                    }
                    
                    // Fallback 1080p without audio
                    if let Some(ref v) = v_url {
                        warn!("1080p video-only for {}", video_id);
                        return axum::response::Redirect::temporary(v).into_response();
                    }
                    
                    // Fallback embed
                    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
                }
                Err(_) => axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
            }
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "Error").into_response()
    }
}

// Streamuj z oszacowanym Content-Length (formatStreams)
async fn stream_with_fake_length(url: &str, _duration_secs: i64) -> Response {
    // Szacujemy rozmiar: 1080p ~ 8 Mbps = 1 MB/s, 720p ~ 5 Mbps = 0.625 MB/s
    match HTTP_CLIENT.get(url).send().await {
        Ok(resp) => {
            let content_type = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string()).unwrap_or_else(|| "video/mp4".to_string());
            let body = match resp.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => return (StatusCode::SERVICE_UNAVAILABLE, format!("Proxy error: {}", e)).into_response(),
            };

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .header(header::CONTENT_LENGTH, body.len().to_string())
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CONNECTION, "close")
                .body(Body::from(body))
                .unwrap()
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, format!("Proxy error: {}", e)).into_response()
    }
}

// Streamuj ffmpeg z oszacowanym Content-Length
async fn stream_ffmpeg_with_fake_length(v: &str, a: &str, duration_secs: i64) -> Response {
    let estimated_size = (duration_secs as u64) * 1_000_000;
    
    let v_owned = v.to_string();
    let a_owned = a.to_string();
    
    let body_stream = async_stream::stream! {
        let mut child = match TokioCommand::new("ffmpeg")
            .args([
                "-i", &v_owned,
                "-i", &a_owned,
                "-c:v", "copy",
                "-c:a", "copy",
                "-movflags", "+faststart",  // TYLKO faststart, BEZ frag_keyframe!
                "-f", "mp4",
                "-bufsize", "32768k",
                "-max_muxing_queue_size", "9999",
                "-max_interleave_delta", "0",
                "-avoid_negative_ts", "make_zero",
                "-threads", "0",
                "pipe:1"
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                yield Err(std::io::Error::new(std::io::ErrorKind::Other, format!("ffmpeg error: {}", e)));
                return;
            }
        };
        
        if let Some(mut stdout) = child.stdout.take() {
            let mut buf = vec![0u8; 65536];
            loop {
                match tokio::io::AsyncReadExt::read(&mut stdout, &mut buf).await {
                    Ok(0) => break,
                    Ok(n) => yield Ok(bytes::Bytes::from(buf[..n].to_vec())),
                    Err(e) => { yield Err(e); break; }
                }
            }
        }
        let _ = child.wait().await;
    };
    
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/mp4")
        .header(header::CONTENT_LENGTH, estimated_size.to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONNECTION, "close")
        .body(Body::from_stream(body_stream))
        .unwrap()
}

// Text utilities

fn strip_problematic_chars(s: &str) -> String {
    s.chars()
        .filter(|&c| {
            let cp = c as u32;
            if cp < 0x20 && c != '\t' && c != '\n' && c != '\r' {
                return false;
            }
            if cp >= 0x1F000 {
                return false;
            }
            if (0x2600..=0x27BF).contains(&cp) {
                return false;
            }
            if (0x1F100..=0x1F1FF).contains(&cp) {
                return false;
            }
            if (0xFE00..=0xFE0F).contains(&cp) {
                return false;
            }
            if cp == 0x200D || cp == 0x200C || cp == 0xFEFF || cp == 0x00AD {
                return false;
            }
            true
        })
        .collect()
}

fn escape_xml(s: &str) -> String {
    let cleaned = strip_problematic_chars(s);
    let mut out = String::with_capacity(cleaned.len() + 16);
    for c in cleaned.chars() {
        match c {
            '&'  => out.push_str("&amp;"),
            '<'  => out.push_str("&lt;"),
            '>'  => out.push_str("&gt;"),
            '"'  => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c if (c as u32) > 127 => {
                out.push_str(&format!("&#x{:X};", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn seconds_to_time(seconds: i64) -> String {
    let s = seconds.max(0);
    format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
}

fn extract_published_date(item: &serde_json::Value) -> (i64, String) {
    // Try `published` as timestamp first
    if let Some(ts) = item.get("published").and_then(|v| v.as_i64()) {
        if ts > 1000000000 {
            return (ts, String::new());
        }
    }
    
    // Then try `publishedText`
    if let Some(text) = item.get("publishedText").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            if let Some(timestamp) = parse_relative_date(text) {
                return (timestamp, text.to_string());
            }
            return (0, text.to_string());
        }
    }
    
    // `uploadedText` as a fallback
    if let Some(text) = item.get("uploadedText").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            if let Some(timestamp) = parse_relative_date(text) {
                return (timestamp, text.to_string());
            }
            return (0, text.to_string());
        }
    }
    
    // Finally: no date available
    (0, String::new())
}

fn parse_relative_date(text: &str) -> Option<i64> {
    let now = Utc::now().timestamp();
    let text_lower = text.to_lowercase();
    
    let patterns = [
        ("year", 31536000),
        ("years", 31536000),
        ("rok", 31536000),
        ("lat", 31536000),
        ("lata", 31536000),
        ("month", 2592000),
        ("months", 2592000),
        ("miesiac", 2592000),
        ("miesiecy", 2592000),
        ("miesiąc", 2592000),
        ("miesięcy", 2592000),
        ("week", 604800),
        ("weeks", 604800),
        ("tydzien", 604800),
        ("tygodni", 604800),
        ("dzień", 86400),
        ("dni", 86400),
        ("day", 86400),
        ("days", 86400),
        ("hour", 3600),
        ("hours", 3600),
        ("godzin", 3600),
        ("minute", 60),
        ("minutes", 60),
        ("minut", 60),
    ];
    
    for (unit, seconds) in &patterns {
        if let Some(pos) = text_lower.find(unit) {
            let prefix = &text_lower[..pos].trim();
            if let Some(last_word) = prefix.split_whitespace().last() {
                if let Ok(num) = last_word.parse::<f64>() {
                    return Some(now - (num * *seconds as f64) as i64);
                }
            }
            return Some(now - *seconds);
        }
    }
    
    None
}

fn timestamp_to_rfc3339(ts: i64) -> String {
    if ts > 1000000000 {
        chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .unwrap_or_else(|| Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    } else {
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }
}

fn base_instance_url(instance: &str) -> String {
    let trimmed = instance.trim_end_matches('/');
    if let Some(scheme_end) = trimmed.find("://") {
        let after_scheme = &trimmed[scheme_end + 3..];
        if let Some(last_slash) = after_scheme.rfind('/') {
            return trimmed[..scheme_end + 3 + last_slash].to_string();
        }
    } else if let Some(last_slash) = trimmed.rfind('/') {
        return trimmed[..last_slash].to_string();
    }
    trimmed.to_string()
}

fn instance_debug_hint(instance: &str) -> String {
    let base = base_instance_url(instance);
    format!("video description is currently not supported debug info: using invidious instance: {} if your video don't want to load fully close app and open it again", base)
}

fn is_live(v: &InvidiousVideo) -> bool {
    if v.length_seconds == 0 {
        return true;
    }
    if v.live_now == Some(true) {
        return true;
    }
    false
}

// XML BUILDERS

fn build_feed_xml(
    feed_id: &str,
    title: &str,
    videos: &[InvidiousVideo],
    start: usize,
    limit: usize,
    next_link: Option<String>,
    instance_url: &str,
) -> String {
    let mut xml = String::with_capacity(4096 + videos.len() * 1024);
    let host = &crate::CONFIG.server_host;

    xml.push_str("<?xml version='1.0' encoding='UTF-8'?>\n");
    xml.push_str("<feed xmlns='http://www.w3.org/2005/Atom'\n");
    xml.push_str("      xmlns:openSearch='http://a9.com/-/spec/opensearch/1.1/'\n");
    xml.push_str("      xmlns:media='http://search.yahoo.com/mrss/'\n");
    xml.push_str("      xmlns:gd='http://schemas.google.com/g/2005'\n");
    xml.push_str("      xmlns:yt='http://gdata.youtube.com/schemas/2007'>\n");

    // ID feeda
    xml.push_str("  <id>http://");
    xml.push_str(host);
    xml.push_str("/feeds/api/");
    xml.push_str(feed_id);
    xml.push_str("</id>\n");
    
    xml.push_str("  <category scheme='http://schemas.google.com/g/2005#kind' term='http://gdata.youtube.com/schemas/2007#video'/>\n");
    xml.push_str("  <title type='text'>");
    xml.push_str(&escape_xml(title));
    xml.push_str("</title>\n");
    xml.push_str("  <logo>http://www.youtube.com/img/pic_youtubelogo_123x63.gif</logo>\n");
    xml.push_str("  <link rel='alternate' type='text/html' href='http://www.youtube.com'/>\n");
    
    xml.push_str("  <link rel='http://schemas.google.com/g/2005#feed' type='application/atom+xml' href='http://");
    xml.push_str(host);
    xml.push_str("/feeds/api/videos'/>\n");
    
    xml.push_str("  <link rel='http://schemas.google.com/g/2005#batch' type='application/atom+xml' href='http://");
    xml.push_str(host);
    xml.push_str("/feeds/api/videos/batch'/>\n");
    
    xml.push_str("  <author><name>YouTube/yt2009</name><uri>http://www.youtube.com/</uri></author>\n");
    
    xml.push_str("  <generator version='2.0' uri='http://");
    xml.push_str(host);
    xml.push_str("/'>YouTube data API</generator>\n");

    xml.push_str("  <openSearch:totalResults>");
    xml.push_str(&videos.len().to_string());
    xml.push_str("</openSearch:totalResults>\n");
    xml.push_str("  <openSearch:startIndex>");
    xml.push_str(&start.to_string());
    xml.push_str("</openSearch:startIndex>\n");
    xml.push_str("  <openSearch:itemsPerPage>");
    xml.push_str(&limit.to_string());
    xml.push_str("</openSearch:itemsPerPage>\n");

    if let Some(ref next) = next_link {
        xml.push_str("  <link rel='next' type='application/atom+xml' href='");
        xml.push_str(&escape_xml(next));
        xml.push_str("'/>\n");
    }

    for video in videos {
        let published = if video.published > 0 {
            timestamp_to_rfc3339(video.published)
        } else if !video.published_text.is_empty() {
            if let Some(ts) = parse_relative_date(&video.published_text) {
                timestamp_to_rfc3339(ts)
            } else {
                video.published_text.clone()
            }
        } else if !video.uploaded_text.is_empty() {
            if let Some(ts) = parse_relative_date(&video.uploaded_text) {
                timestamp_to_rfc3339(ts)
            } else {
                video.uploaded_text.clone()
            }
        } else {
            Utc::now().to_rfc3339()
        };

        let vid = &video.video_id;
        let title_esc = escape_xml(&video.title);
        let author_esc = escape_xml(if !video.author.is_empty() { &video.author } else { "Unknown" });
        let author_id = if !video.author_id.is_empty() { &video.author_id } else { "unknown" };

        let view_count   = video.view_count;
        let like_count   = if video.likes > 0 { video.likes } else { (view_count as f64 * 0.06) as i64 };
        let dislike_count = if video.dislikes > 0 { video.dislikes } else { (view_count as f64 * 0.01) as i64 };
        let comment_count = video.comment_count;
        let favorite_count = if view_count > 0 { view_count / 250 } else { 0 };
        let num_raters   = view_count / 1000;

        xml.push_str("  <entry>\n");

        // ID wpisu - z hostem
        xml.push_str("    <id>http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/videos/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</id>\n");

        xml.push_str("    <youTubeId id='");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'>");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</youTubeId>\n");

        xml.push_str("    <published>");
        xml.push_str(&published);
        xml.push_str("</published>\n");
        xml.push_str("    <updated>");
        xml.push_str(&published);
        xml.push_str("</updated>\n");

        xml.push_str("    <category scheme='http://gdata.youtube.com/schemas/2007/categories.cat' label='Science &amp; Technology' term='Science &amp; Technology'>Science &amp; Technology</category>\n");

        xml.push_str("    <title type='text'>");
        xml.push_str(&title_esc);
        xml.push_str("</title>\n");
        xml.push_str("    <content type='text'>");
        xml.push_str(&title_esc);
        xml.push_str("</content>\n");

        // Link to related videos - using host
        xml.push_str("    <link rel='http://gdata.youtube.com/schemas/2007#video.related' href='http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/videos/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/related'/>\n");

        xml.push_str("    <author>\n");
        xml.push_str("      <name>");
        xml.push_str(&author_esc);
        xml.push_str("</name>\n");
        
        // URI autora - z hostem
        xml.push_str("      <uri>http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/users/");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("</uri>\n");
        
        xml.push_str("      <yt:userId>");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("</yt:userId>\n");
        xml.push_str("    </author>\n");

        // Link do komentarzy - z hostem
        xml.push_str("    <gd:comments>\n");
        xml.push_str("      <gd:feedLink href='http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/videos/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/comments' countHint='");
        xml.push_str(&comment_count.to_string());
        xml.push_str("'/>\n");
        xml.push_str("    </gd:comments>\n");

        xml.push_str("    <media:group>\n");
        xml.push_str("      <media:title>");
        xml.push_str(&title_esc);
        xml.push_str("</media:title>\n");
        xml.push_str("      <media:category label='Science &amp; Technology' scheme='http://gdata.youtube.com/schemas/2007/categories.cat'>Science &amp; Technology</media:category>\n");

        // use host from the outer scope (crate::CONFIG.server_host)

        xml.push_str("      <media:content url='");
        xml.push_str(instance_url);
        xml.push_str("/embed/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("?raw=1' type='video/3gpp' medium='video' expression='full' duration='");
        xml.push_str(&video.length_seconds.to_string());
        xml.push_str("' yt:format='3'/>\n");
        
        /*xml.push_str("      <media:content url='http://");
        xml.push_str(host);
        xml.push_str("/get_hd_video?id=");
        xml.push_str(&escape_xml(vid));
        xml.push_str("' type='video/mp4' medium='video' expression='full' duration='999' yt:format='8'/>\n");
        */
        
        let hint = instance_debug_hint(instance_url);
        let hint_esc = escape_xml(&hint);
        let description_with_hint = format!("{}", hint_esc);

        xml.push_str("      <media:description type='plain'>");
        xml.push_str(&description_with_hint);
        xml.push_str("</media:description>\n");
        xml.push_str("      <media:keywords>");
        xml.push_str(&title_esc);
        xml.push_str("</media:keywords>\n");
        xml.push_str("      <media:player url='http://www.youtube.com/watch?v=");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'/>\n");
        xml.push_str("      <media:thumbnail yt:name='hqdefault' url='http://i.ytimg.com/vi/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/hqdefault.jpg' height='240' width='320' time='00:00:00'/>\n");
        xml.push_str("      <media:thumbnail yt:name='poster' url='http://i.ytimg.com/vi/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/0.jpg' height='240' width='320' time='00:00:00'/>\n");
        xml.push_str("      <media:thumbnail yt:name='default' url='http://i.ytimg.com/vi/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/0.jpg' height='240' width='320' time='00:00:00'/>\n");
        xml.push_str("      <yt:duration seconds='");
        xml.push_str(&video.length_seconds.to_string());
        xml.push_str("'/>\n");
        xml.push_str("      <yt:uploaded>");
        xml.push_str(&published);
        xml.push_str("</yt:uploaded>\n");
        xml.push_str("      <yt:uploaderId>");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("</yt:uploaderId>\n");
        xml.push_str("      <yt:videoid id='");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'>");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</yt:videoid>\n");
        xml.push_str("      <youTubeId id='");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'>");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</youTubeId>\n");
        xml.push_str("      <media:credit role='uploader' yt:display='");
        xml.push_str(&author_esc);
        xml.push_str("' name='");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("'>");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("</media:credit>\n");
        xml.push_str("    </media:group>\n");

        xml.push_str("    <gd:rating average='5' max='5' min='1' numRaters='");
        xml.push_str(&num_raters.to_string());
        xml.push_str("' rel='http://schemas.google.com/g/2005#overall'/>\n");
        xml.push_str("    <yt:statistics favoriteCount='");
        xml.push_str(&favorite_count.to_string());
        xml.push_str("' viewCount='");
        xml.push_str(&view_count.to_string());
        xml.push_str("'/>\n");
        xml.push_str("    <yt:rating numLikes='");
        xml.push_str(&like_count.to_string());
        xml.push_str("' numDislikes='");
        xml.push_str(&dislike_count.to_string());
        xml.push_str("'/>\n");

        xml.push_str("  </entry>\n");
    }

    xml.push_str("</feed>");
    xml
}

// Handlers - device / video

pub async fn register_device() -> axum::response::Response {
    let alphabet = b"qwertyuiopasdfghjklzxcvbnm1234567890";
    let mut rng = rand::rngs::StdRng::from_entropy();
    let device_id: String = (0..7)
        .map(|_| {
            let idx = rng.gen_range(0..alphabet.len());
            alphabet[idx] as char
        })
        .collect();

    let body = format!(
        "DeviceId={}\nDeviceKey=ULxlVAAVMhZ2GeqZA/X1GgqEEIP1ibcd3S+42pkWfmk=",
        device_id
    );

    info!("Device registered: {}", device_id);
    // record device id
    {
        let mut a = ANALYTICS.write().await;
        a.registered_devices.insert(device_id.clone());
    }
    tokio::spawn(async {
        let _ = save_stats_snapshot().await;
    });
    let response = axum::http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
        .body(Body::from(body))
        .expect("Failed to build register_device response");

    response
}

pub async fn device_registration() -> impl IntoResponse {
    let alphabet = b"qwertyuiopasdfghjklzxcvbnm1234567890";
    let mut rng = rand::rngs::StdRng::from_entropy();
    let device_id: String = (0..9)
        .map(|_| {
            let idx = rng.gen_range(0..alphabet.len());
            alphabet[idx] as char
        })
        .collect();

    let payload = serde_json::json!({
        "id": device_id,
        "key": "AP+lc79/lqV58X9FLDdn7SiOzH8hDb1ItXMmm25Cb4YDLWZkI+gXBiwwOvcssAY"
    });

    axum::Json(payload)
}

pub async fn stats() -> impl IntoResponse {
    let persisted = PERSISTED_STATS.read().await;
    let a = ANALYTICS.read().await;
    let resp = compute_stats_response(&*persisted, &a.events, a.registered_devices.len());
    (axum::http::StatusCode::OK, axum::Json(resp)).into_response()
}

pub async fn embed_video(Path(video_id): Path<String>) -> impl IntoResponse {
    info!("EMBED_VIDEO /embed/{}", video_id);
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    let url = format!("{}/embed/{}?raw=1", instance, video_id);
    axum::response::Redirect::temporary(&url).into_response()
}

pub async fn exp_hd(Query(params): Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("video_id") else {
        return (StatusCode::BAD_REQUEST, "Missing video_id").into_response();
    };
    info!("EXP_HD video_id={}", video_id);
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

pub async fn channel_fh264_getvideo(Query(params): Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("v") else {
        return (StatusCode::BAD_REQUEST, "Missing v").into_response();
    };
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

pub async fn get_video(Query(params): Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("video_id") else {
        return (StatusCode::BAD_REQUEST, "Missing video_id").into_response();
    };
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

pub async fn get_480(Query(params): Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("video_id") else {
        return (StatusCode::BAD_REQUEST, "Missing video_id").into_response();
    };
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

// STANDARD FEED (trending)

pub async fn standard_feed(
    Path((region, feed)): Path<(String, String)>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    let t0 = Instant::now();
    info!("STANDARD_FEED: /feeds/api/standardfeeds/{}/{}", region, feed);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let feed_title = match feed.as_str() {
        "most_popular"      => "Most Popular",
        "top_rated"         => "Top Rated",
        "most_discussed"    => "Most Discussed",
        "recently_featured" => "Recently Featured",
        "top_favorites"     => "Top Favorites",
        other => {
            if let Some(cat) = other.strip_prefix("most_popular_") { cat }
            else if let Some(cat) = other.strip_prefix("top_rated_") { cat }
            else { other }
        }
    };

    let mut videos: Vec<InvidiousVideo> = Vec::new();

    let url = format!("{}/api/v1/trending?region={}", instance, region);
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(v) = resp.json::<Vec<InvidiousVideo>>().await {
            let before = v.len();
            videos = v.into_iter().filter(|v| !is_live(v)).collect();
            info!("Trending: {}/{} videos after live filter", videos.len(), before);
        }
    }

    if videos.is_empty() {
        let url = format!("{}/api/v1/popular?region={}", instance, region);
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(v) = resp.json::<Vec<InvidiousVideo>>().await {
                videos = v.into_iter().filter(|v| !is_live(v)).collect();
                info!("Popular fallback: {} videos", videos.len());
            }
        }
    }

    let start = params.start_index.unwrap_or(1);
    let limit = params.max_results.unwrap_or(18).min(50);

    let paginated: Vec<InvidiousVideo> = videos
        .into_iter()
        .skip(start.saturating_sub(1))
        .take(limit)
        .collect();

    let xml = build_feed_xml(
        &format!("standardfeeds/{}/{}", region, feed),
        &format!("YouTube {} Videos for {}", feed_title, region),
        &paginated,
        start,
        limit,
        None,
        &instance,
    );

    info!("standard_feed: {} bytes, {}ms", xml.len(), t0.elapsed().as_millis());
    atom_response(xml)
}

fn format_view_count(view_count: i64) -> String {
    let mut s = view_count.abs().to_string();
    let mut out = String::new();
    while s.len() > 3 {
        let chunk = s.split_off(s.len() - 3);
        out = format!(
            ",{chunk}{out}",
            chunk = chunk,
            out = out
        );
    }
    if !s.is_empty() {
        out = format!("{s}{out}", s = s, out = out);
    }
    out
}

fn format_duration(seconds: i32) -> String {
    if seconds <= 0 {
        return "0:00".to_string();
    }
    let minutes = seconds / 60;
    let secs = seconds % 60;
    format!("{}:{:02}", minutes, secs)
}

fn format_time_ago(published_ts: Option<i64>, published_text: Option<&str>) -> String {
    if let Some(text) = published_text {
        let text = text.trim();
        if !text.is_empty() {
            return text.to_string();
        }
    }

    if let Some(ts) = published_ts {
        if ts > 0 {
            let diff = Utc::now().timestamp().saturating_sub(ts);
            if diff < 60 {
                return "just now".to_string();
            } else if diff < 3600 {
                return format!("{} minutes ago", diff / 60);
            } else if diff < 86400 {
                return format!("{} hours ago", diff / 3600);
            } else if diff < 604800 {
                return format!("{} days ago", diff / 86400);
            } else if diff < 2592000 {
                return format!("{} weeks ago", diff / 604800);
            } else if diff < 31536000 {
                return format!("{} months ago", diff / 2592000);
            } else {
                return format!("{} years ago", diff / 31536000);
            }
        }
    }

    String::new()
}

fn extract_comment_content(comment: &serde_json::Value) -> String {
    if let Some(text) = comment.get("content").and_then(|v| v.as_str()) {
        return text.to_string();
    }

    if let Some(content_obj) = comment.get("content") {
        if let Some(runs) = content_obj.get("runs").and_then(|v| v.as_array()) {
            return runs.iter().filter_map(|run| run.get("text").and_then(|t| t.as_str())).collect::<Vec<_>>().join("");
        }
        if let Some(simple_text) = content_obj.get("simpleText").and_then(|v| v.as_str()) {
            return simple_text.to_string();
        }
    }

    String::new()
}

fn build_mobile_video_object(video: &InvidiousVideo, instance: &str) -> serde_json::Value {
    serde_json::json!({
        "id": video.video_id,
        "video_id": video.video_id,
        "title": video.title,
        "duration": format_duration(video.length_seconds),
        "view_count": format_view_count(video.view_count),
        "is_playable": true,
        "username": video.author,
        "watch_link": format!("/watch?v={}", video.video_id),
        "thumbnail_for_list": format!("<img src=\"http://i.ytimg.com/vi/{}/default.jpg\"/>", video.video_id),
        "stream_url": format!("{}/embed/{}?raw=1", instance.trim_end_matches('/'), video.video_id),
        "landscape": true,
        "stitched_thumbnail_large": {
            "url": format!("http://i.ytimg.com/vi/{}/hqdefault.jpg", video.video_id),
            "width": 160,
            "height": 120,
            "posx": 60,
            "posy": 25
        },
        "length": video.length_seconds,
        "tags": Vec::<String>::new(),
        "short_description": "loading",
        "time_created_text": ""
    })
}

async fn search_videos_from_invidious(client: &Client, instance: &str, query: &str) -> Vec<InvidiousVideo> {
    let url = format!(
        "{}/api/v1/search?q={}&type=video",
        instance,
        urlencoding::encode(query)
    );

    let mut entries: Vec<InvidiousVideo> = Vec::new();
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            if let Some(items) = data.as_array() {
                for item in items {
                    let type_str = item.get("type").and_then(|v| v.as_str()).unwrap_or("video");
                    if type_str != "video" { continue; }

                    let Some(video_id) = item.get("videoId").and_then(|v| v.as_str()) else { continue };
                    let live_now = item.get("liveNow").and_then(|v| v.as_bool()).unwrap_or(false);
                    let length_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                    if live_now || length_seconds == 0 { continue; }

                    let (published_ts, published_text) = extract_published_date(item);

                    entries.push(InvidiousVideo {
                        video_id: video_id.to_string(),
                        title: item.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        author: item.get("author").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        author_id: item.get("authorId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        length_seconds,
                        view_count: item.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0),
                        published_text,
                        published: published_ts,
                        uploaded_text: String::new(),
                        live_now: Some(false),
                        likes: item.get("likes").and_then(|v| v.as_i64()).unwrap_or(0),
                        dislikes: item.get("dislikes").and_then(|v| v.as_i64()).unwrap_or(0),
                        comment_count: item.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0),
                    });
                }
            }
        }
    }
    entries
}

pub async fn mobile_blzr_home(Query(_params): Query<HashMap<String, String>>) -> Response {
    let t0 = Instant::now();
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let mut videos: Vec<InvidiousVideo> = Vec::new();
    let url = format!("{}/api/v1/trending?region=US", instance);
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(v) = resp.json::<Vec<InvidiousVideo>>().await {
            videos = v.into_iter().filter(|v| !is_live(v)).collect();
        }
    }
    if videos.is_empty() {
        let url = format!("{}/api/v1/popular?region=US", instance);
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(v) = resp.json::<Vec<InvidiousVideo>>().await {
                videos = v.into_iter().filter(|v| !is_live(v)).collect();
            }
        }
    }

    let mobile_videos: Vec<serde_json::Value> = videos.iter()
        .take(18)
        .map(|video| build_mobile_video_object(video, &instance))
        .collect();

    let response = serde_json::json!({
        "result": "ok",
        "content": {
            "home_videos": mobile_videos
        }
    });

    info!("mobile_blzr_home: {} items, {}ms", response["content"]["home_videos"].as_array().map(|a| a.len()).unwrap_or(0), t0.elapsed().as_millis());
    Json(response).into_response()
}

pub async fn mobile_blzr_results(Query(params): Query<SearchParams>) -> Response {
    let t0 = Instant::now();
    let query = match params.q.as_deref() {
        Some(q) if !q.is_empty() => q.to_string(),
        _ => return (StatusCode::BAD_REQUEST, "Missing query").into_response(),
    };

    info!("MOBILE_BLZR_RESULTS q={}", query);
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    let entries = search_videos_from_invidious(client, &instance, &query).await;

    let start = params.start_index.unwrap_or(1);
    let limit = params.max_results.unwrap_or(20).min(50);
    let paginated: Vec<serde_json::Value> = entries
        .into_iter()
        .skip(start.saturating_sub(1))
        .take(limit)
        .map(|video| build_mobile_video_object(&video, &instance))
        .collect();

    let response = serde_json::json!({
        "result": "ok",
        "content": {
            "videos": paginated,
            "results_count": ""
        }
    });

    info!("mobile_blzr_results: {} items, {}ms", response["content"]["videos"].as_array().map(|a| a.len()).unwrap_or(0), t0.elapsed().as_millis());
    Json(response).into_response()
}

pub async fn mobile_blzr_watch(Query(params): Query<HashMap<String, String>>) -> Response {
    let t0 = Instant::now();
    let Some(video_id) = params.get("v") else {
        return (StatusCode::BAD_REQUEST, "Missing v").into_response();
    };

    info!("MOBILE_BLZR_WATCH v={}", video_id);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    let video_url = format!("{}/api/v1/videos/{}", instance, video_id);

    let data = match client.get(&video_url).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(json) => json,
            Err(e) => {
                warn!("Parse watch data failed: {}", e);
                return (StatusCode::NOT_FOUND, "Video not found").into_response();
            }
        },
        Err(e) => {
            warn!("Fetch watch data failed: {}", e);
            return (StatusCode::NOT_FOUND, "Video not found").into_response();
        }
    };

    let title = data.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let author = data.get("author").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let category = data.get("category").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let duration = data.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let view_count = data.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let description = data.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let published_ts = data.get("published").and_then(|v| v.as_i64()).unwrap_or(0);
    let time_created_text = if let Some(text) = data.get("publishedText").and_then(|v| v.as_str()) {
        text.to_string()
    } else if published_ts > 0 {
        if let Some(dt) = chrono::DateTime::from_timestamp(published_ts, 0) {
            dt.format("%b %e, %Y").to_string()
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    let tags = data.get("tags").and_then(|v| v.as_array()).map(|arr| {
        arr.iter().filter_map(|tag| tag.as_str()).map(|tag| {
            serde_json::json!({
                "url": format!("/results?search_query={}", urlencoding::encode(tag)),
                "keyword": tag
            })
        }).collect::<Vec<_>>()
    }).unwrap_or_default();

    let related_videos = data.get("recommendedVideos").and_then(|v| v.as_array()).map(|arr| {
        arr.iter().filter_map(|item| {
            let Some(vid) = item.get("videoId").and_then(|v| v.as_str()) else { return None; };
            let Some(title) = item.get("title").and_then(|v| v.as_str()) else { return None; };
            let length_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
            if length_seconds == 0 { return None; }
            Some(serde_json::json!({
                "id": vid,
                "video_id": vid,
                "title": title,
                "duration": format_duration(length_seconds),
                "view_count": format_view_count(item.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0)),
                "is_playable": true,
                "username": item.get("author").and_then(|v| v.as_str()).unwrap_or("Unknown"),
                "watch_link": format!("/watch?v={}", vid),
                "thumbnail_for_list": format!("<img src=\"http://i.ytimg.com/vi/{}/default.jpg\"/>", vid),
                "stream_url": format!("{}/embed/{}?raw=1", instance.trim_end_matches('/'), vid),
                "landscape": true,
                "stitched_thumbnail_large": {
                    "url": format!("http://i.ytimg.com/vi/{}/hqdefault.jpg", vid),
                    "width": 160,
                    "height": 120,
                    "posx": 60,
                    "posy": 25
                },
                "length": length_seconds,
                "tags": Vec::<String>::new(),
                "short_description": "loading",
                "time_created_text": ""
            }))
        }).collect::<Vec<_>>()
    }).unwrap_or_default();

    let video_obj = serde_json::json!({
        "stitched_thumbnail_large": {
            "url": format!("http://i.ytimg.com/vi/{}/hqdefault.jpg", video_id),
            "width": 160,
            "height": 120,
            "posx": 60,
            "posy": 25
        },
        "id": video_id,
        "stream_url": format!("{}/embed/{}?raw=1", instance.trim_end_matches('/'), video_id),
        "is_playable": true,
        "title": title,
        "username": author,
        "category_name": category,
        "tags": tags,
        "full_description": description,
        "short_description": description.chars().take(120).collect::<String>(),
        "time_created_text": time_created_text,
        "view_count": format_view_count(view_count),
        "duration": format_duration(duration),
        "user_image_url": "/assets/site-assets/default.png",
        "hq_stream_url": format!("/exp_hd?video_id={}", video_id)
    });

    let response = serde_json::json!({
        "result": "ok",
        "content": {
            "video": video_obj,
            "related_videos": related_videos
        }
    });

    info!("mobile_blzr_watch: {} related, {}ms", response["content"]["related_videos"].as_array().map(|a| a.len()).unwrap_or(0), t0.elapsed().as_millis());
    Json(response).into_response()
}

pub async fn mobile_blzr_view_comment(Query(params): Query<HashMap<String, String>>) -> Response {
    let Some(video_id) = params.get("v") else {
        return (StatusCode::BAD_REQUEST, "Missing v").into_response();
    };

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    let url = format!("{}/api/v1/comments/{}", instance, video_id);

    let mut comments = Vec::new();
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            if let Some(items) = data.get("comments").and_then(|v| v.as_array()) {
                for comment in items.iter().take(50) {
                    let author_name = comment.get("author").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();
                    let published_text = comment.get("publishedText").and_then(|v| v.as_str());
                    let published = comment.get("published").and_then(|v| v.as_i64());
                    let time_ago = format_time_ago(published, published_text);
                    comments.push(serde_json::json!({
                        "author_name": author_name,
                        "comment": extract_comment_content(comment),
                        "time_ago": time_ago,
                    }));
                }
            }
        }
    }

    Json(serde_json::json!({
        "result": "ok",
        "content": {
            "comments": comments
        }
    })).into_response()
}

pub async fn mobile_blzr_profile(Query(params): Query<HashMap<String, String>>) -> Response {
    let Some(username) = params.get("user") else {
        return (StatusCode::BAD_REQUEST, "Missing user").into_response();
    };

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    let profile_path = format!("/channels/{}", username);
    let profile_data = match fetch_invidious_json(client, &profile_path).await {
        Some((data, _)) => data,
        None => return (StatusCode::NOT_FOUND, "User not found").into_response(),
    };

    let title = profile_data.get("author").and_then(|v| v.as_str()).unwrap_or(username).to_string();
    let image_url = profile_data
        .get("authorThumbnails")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.last())
        .and_then(|t| t.get("url").and_then(|u| u.as_str()))
        .unwrap_or("/assets/site-assets/default.png")
        .to_string();
    let subscriber_count = profile_data
        .get("subCount")
        .and_then(|v| v.as_i64())
        .or_else(|| profile_data.get("subscriberCount").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    let viewed_count = profile_data
        .get("viewCount")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let base_content = serde_json::json!({
        "title": title,
        "profile_user": {
            "image_url": image_url,
            "videos_url": "/",
            "subscriber_count": format_view_count(subscriber_count),
            "viewed_count": format_view_count(viewed_count)
        },
        "view_enabled": {
            "videos": true
        },
        "videos_url": format!("/profile?view=videos&user={}", username),
        "show_subscription_button": true,
        "is_subscribed": false
    });

    if let Some(view) = params.get("view") {
        if view == "videos" {
            let videos_path = format!("/channels/{}/videos?page=1", username);
            let mut video_entries = Vec::new();
            if let Some((data, _)) = fetch_invidious_json(client, &videos_path).await {
                if let Some(items) = data.get("videos").and_then(|v| v.as_array()) {
                    for item in items.iter().take(50) {
                        if let (Some(video_id), Some(title), Some(author)) = (
                            item.get("videoId").and_then(|v| v.as_str()),
                            item.get("title").and_then(|v| v.as_str()),
                            item.get("author").and_then(|v| v.as_str()),
                        ) {
                            let length_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                            if length_seconds == 0 {
                                continue;
                            }
                            let view_count = item.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0);
                            let video = InvidiousVideo {
                                video_id: video_id.to_string(),
                                title: title.to_string(),
                                author: author.to_string(),
                                author_id: item.get("authorId").and_then(|v| v.as_str()).unwrap_or(username).to_string(),
                                length_seconds,
                                view_count,
                                published_text: String::new(),
                                published: item.get("published").and_then(|v| v.as_i64()).unwrap_or(0),
                                uploaded_text: String::new(),
                                live_now: Some(false),
                                likes: item.get("likes").and_then(|v| v.as_i64()).unwrap_or(0),
                                dislikes: item.get("dislikes").and_then(|v| v.as_i64()).unwrap_or(0),
                                comment_count: item.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0),
                            };
                            video_entries.push(build_mobile_video_object(&video, &instance));
                        }
                    }
                }
            }

            return Json(serde_json::json!({
                "result": "ok",
                "content": {
                    "videos": video_entries
                }
            })).into_response();
        }
    }

    Json(serde_json::json!({
        "result": "ok",
        "content": base_content
    })).into_response()
}

// VIDEO SEARCH

pub async fn video_search(Query(params): Query<SearchParams>) -> impl IntoResponse {
    let t0 = Instant::now();
    let query = match params.q.as_deref() {
        Some(q) if !q.is_empty() => q.to_string(),
        _ => return (StatusCode::BAD_REQUEST, "Missing query").into_response(),
    };

    info!("VIDEO_SEARCH q={}", query);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let url = format!(
        "{}/api/v1/search?q={}&type=video",
        instance,
        urlencoding::encode(&query)
    );

    let mut entries: Vec<InvidiousVideo> = Vec::new();

    match client.get(&url).send().await {
        Ok(resp) => {
            match resp.json::<serde_json::Value>().await {
                Ok(data) => {
                    if let Some(items) = data.as_array() {
                        for item in items {
                            let type_str = item.get("type").and_then(|v| v.as_str()).unwrap_or("video");
                            if type_str != "video" { continue; }

                            let Some(video_id) = item.get("videoId").and_then(|v| v.as_str()) else { continue };
                            let live_now = item.get("liveNow").and_then(|v| v.as_bool()).unwrap_or(false);
                            let length_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

                            if live_now || length_seconds == 0 { continue; }
                            
                                    // Fetch published date
                            let (published_ts, published_text) = extract_published_date(item);

                            entries.push(InvidiousVideo {
                                video_id: video_id.to_string(),
                                title: item.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                author: item.get("author").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                author_id: item.get("authorId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                length_seconds,
                                view_count: item.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0),
                                published_text,
                                published: published_ts,
                                uploaded_text: String::new(),
                                live_now: Some(false),
                                likes: item.get("likes").and_then(|v| v.as_i64()).unwrap_or(0),
                                dislikes: item.get("dislikes").and_then(|v| v.as_i64()).unwrap_or(0),
                                comment_count: item.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0),
                            });
                        }
                    }
                    info!("Search found {} results in {}ms", entries.len(), t0.elapsed().as_millis());
                }
                Err(e) => warn!("Failed to parse search response: {}", e),
            }
        }
        Err(e) => {
            warn!("Failed to fetch search: {}", e);
            return (StatusCode::SERVICE_UNAVAILABLE, format!("Search error: {}", e)).into_response();
        }
    }

    let start = params.start_index.unwrap_or(1);
    let limit = params.max_results.unwrap_or(20).min(50);
    let total = entries.len();

    let paginated: Vec<InvidiousVideo> = entries
        .into_iter()
        .skip(start.saturating_sub(1))
        .take(limit)
        .collect();

    let next_link = if start + limit <= total {
        Some(format!(
            "http://{}/feeds/api/videos?q={}&start-index={}&max-results={}",
            crate::CONFIG.server_host,
            urlencoding::encode(&query),
            start + limit,
            limit
        ))
    } else {
        None
    };

    let xml = build_feed_xml(
        "videos",
        &format!("Search results: {}", query),
        &paginated,
        start,
        limit,
        next_link,
        &instance,
    );
    info!("video_search: {} bytes, {}ms", xml.len(), t0.elapsed().as_millis());
    atom_response(xml)
}

// VIDEO ENTRY (single video metadata)

pub async fn video_entry(Path(video_id): Path<String>) -> impl IntoResponse {
    info!("VIDEO_ENTRY: /feeds/api/videos/{}", video_id);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let resp = client
        .get(&format!("{}/api/v1/videos/{}", instance, video_id))
        .send()
        .await;

    let data = match resp {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(d) => d,
            Err(e) => { warn!("Parse error: {}", e); return (StatusCode::NOT_FOUND, "Video not found").into_response(); }
        },
        Err(e) => { warn!("Fetch error: {}", e); return (StatusCode::NOT_FOUND, "Video not found").into_response(); }
    };

    let title       = data.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let author      = data.get("author").and_then(|v| v.as_str()).unwrap_or("");
    let author_id   = data.get("authorId").and_then(|v| v.as_str()).unwrap_or("");
    let duration    = data.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
    let views       = data.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let description = data.get("description").and_then(|v| v.as_str()).unwrap_or("");
    let published   = data.get("published").and_then(|v| v.as_i64()).unwrap_or(0);
    let like_count  = data.get("likeCount").and_then(|v| v.as_i64())
        .unwrap_or_else(|| (views as f64 * 0.006) as i64);
    let dislike_count = (views as f64 * 0.001) as i64;
    let num_raters  = like_count;

    let published_date = if published > 0 {
        chrono::DateTime::from_timestamp(published, 0)
            .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .unwrap_or_else(|| Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    } else {
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    };

    let recorded_date = chrono::DateTime::from_timestamp(published, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_default();

    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let title_esc   = escape_xml(title);
    let author_esc  = escape_xml(author);
    let author_id_esc = escape_xml(author_id);
    let hint = instance_debug_hint(&instance);
    let description_with_hint = if description.trim().is_empty() {
        hint.clone()
    } else {
        format!("{} {}", description.trim_end(), hint)
    };
    let desc_esc = escape_xml(&description_with_hint.chars().take(5000).collect::<String>());
    let middle_time = duration / 2;
    let end_time    = if duration > 30 { duration - 30 } else { 0 };
    let host = &crate::CONFIG.server_host;
    let etag = format!("{}-{}", "yt", video_id);

    let feed_xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:media="http://search.yahoo.com/mrss/"
      xmlns:openSearch="http://a9.com/-/spec/opensearch/1.1/"
      xmlns:gd="http://schemas.google.com/g/2005"
      xmlns:yt="http://gdata.youtube.com/schemas/2007"
      xmlns:georss="http://www.georss.org/georss"
      xmlns:gml="http://www.opengis.net/gml"
      xmlns:app="http://www.w3.org/2007/app"
      gd:etag="{etag}">
  <id>http://{host}/feeds/api/videos/{video_id}</id>
  <updated>{now}</updated>
  <category scheme="http://schemas.google.com/g/2005#kind" term="http://gdata.youtube.com/schemas/2007#video"/>
  <title type="text">YouTube Video Feed</title>
  <logo>http://www.gstatic.com/youtube/img/logo.png</logo>
  <link rel="alternate" type="text/html" href="http://www.youtube.com/results?search_query=featured"/>
  <link rel="http://schemas.google.com/g/2005#feed" type="application/atom+xml" href="http://{host}/feeds/api/videos/{video_id}?v=2"/>
  <link rel="http://schemas.google.com/g/2005#batch" type="application/atom+xml" href="http://{host}/feeds/api/videos/{video_id}/batch?v=2"/>
  <link rel="self" type="application/atom+xml" href="http://{host}/feeds/api/videos/{video_id}?start-index=1&amp;max-results=1&amp;v=2"/>
  <link rel="service" type="application/atomsvc+xml" href="http://{host}/feeds/api/videos/{video_id}?alt=atom-service&amp;v=2"/>
  <author><name>YouTube</name><uri>http://www.youtube.com/</uri></author>
  <generator version="2.1" uri="http://{host}">YouTube data API</generator>
  <openSearch:totalResults>1</openSearch:totalResults>
  <openSearch:startIndex>1</openSearch:startIndex>
  <openSearch:itemsPerPage>1</openSearch:itemsPerPage>
  <entry gd:etag="{etag}">
    <id>tag:youtube.com,2008:video:{video_id}</id>
    <published>{published_date}</published>
    <updated>{published_date}</updated>
    <category scheme="http://schemas.google.com/g/2005#kind" term="http://gdata.youtube.com/schemas/2007#video"/>
    <category scheme="http://gdata.youtube.com/schemas/2007/categories.cat" term="Music" label="Music"/>
    <title type="text">{title_esc}</title>
    <content type="application/x-shockwave-flash" src="http://www.youtube.com/v/{video_id}?version=3&amp;f=videos&amp;app=youtube_gdata"/>
    <link rel="alternate" type="text/html" href="http://www.youtube.com/watch?v={video_id}&amp;feature=youtube_gdata"/>
    <link rel="http://gdata.youtube.com/schemas/2007#video.related" type="application/atom+xml" href="http://{host}/feeds/api/videos/{video_id}/related?v=2"/>
    <link rel="http://gdata.youtube.com/schemas/2007#mobile" type="text/html" href="http://m.youtube.com/details?v={video_id}"/>
    <link rel="http://gdata.youtube.com/schemas/2007#uploader" type="application/atom+xml" href="http://{host}/feeds/api/users/{author_id}?v=2"/>
    <link rel="self" type="application/atom+xml" href="http://{host}/feeds/api/videos/{video_id}?v=2"/>
    <author>
      <name>{author_esc}</name>
      <uri>http://{host}/feeds/api/users/{author_id_esc}</uri>
      <yt:userId>{author_id_esc}</yt:userId>
    </author>
    <yt:accessControl action="comment" permission="allowed"/>
    <yt:accessControl action="commentVote" permission="allowed"/>
    <yt:accessControl action="videoRespond" permission="moderated"/>
    <yt:accessControl action="rate" permission="allowed"/>
    <yt:accessControl action="embed" permission="allowed"/>
    <yt:accessControl action="list" permission="allowed"/>
    <yt:accessControl action="syndicate" permission="allowed"/>
    <gd:comments>
      <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#comments" href="http://{host}/feeds/api/videos/{video_id}/comments?v=2" countHint="0"/>
    </gd:comments>
    <media:group>
      <media:category label="Music" scheme="http://gdata.youtube.com/schemas/2007/categories.cat">Music</media:category>
      <media:content url="{primary}/embed/{video_id}?raw=1" type="video/mp4" medium="video" isDefault="true" expression="full" duration="{duration}" yt:format="3"/>
      <media:content url="{primary}/embed/{video_id}?raw=1" type="video/3gpp" medium="video" expression="full" duration="999" yt:format="14"/>
      <media:content url="http://{host}/get_hd_video?id={video_id}" type="video/mp4" medium="video" expression="full" duration="999" yt:format="8"/>
      <media:content url="http://{host}/get_hd_video?id={video_id}" type="video/3gpp" medium="video" expression="full" duration="{duration}" yt:format="9"/>
      <media:credit role="uploader" scheme="urn:youtube" yt:display="{author_esc}" yt:type="partner">{author_esc}</media:credit>
      <media:description type="plain">{desc_esc}</media:description>
      <media:keywords></media:keywords>
      <media:license type="text/html" href="http://www.youtube.com/t/terms">youtube</media:license>
      <media:player url="http://www.youtube.com/watch?v={video_id}&amp;feature=youtube_gdata"/>
      <media:thumbnail url="http://i.ytimg.com/vi/{video_id}/default.jpg" height="90" width="120" yt:name="default"/>
      <media:thumbnail url="http://i.ytimg.com/vi/{video_id}/mqdefault.jpg" height="180" width="320" yt:name="mqdefault"/>
      <media:thumbnail url="http://i.ytimg.com/vi/{video_id}/hqdefault.jpg" height="360" width="480" yt:name="hqdefault"/>
      <media:thumbnail url="http://i.ytimg.com/vi/{video_id}/0.jpg" height="90" width="120" time="00:00:37" yt:name="start"/>
      <media:thumbnail url="http://i.ytimg.com/vi/{video_id}/1.jpg" height="90" width="120" time="{middle_time_fmt}" yt:name="middle"/>
      <media:thumbnail url="http://i.ytimg.com/vi/{video_id}/2.jpg" height="90" width="120" time="{end_time_fmt}" yt:name="end"/>
      <media:title type="plain">{title_esc}</media:title>
      <yt:aspectratio>widescreen</yt:aspectratio>
      <yt:duration seconds="{duration}"/>
      <yt:uploaded>{published_date}</yt:uploaded>
      <yt:uploaderId>{author_id_esc}</yt:uploaderId>
      <yt:videoid>{video_id}</yt:videoid>
    </media:group>
    <gd:rating average="5" max="5" min="1" numRaters="{num_raters}" rel="http://schemas.google.com/g/2005#overall"/>
    <yt:recorded>{recorded_date}</yt:recorded>
    <yt:statistics favoriteCount="0" viewCount="{views}"/>
    <yt:rating numLikes="{like_count}" numDislikes="{dislike_count}"/>
    <yt:position>1</yt:position>
  </entry>
</feed>"#,
        host           = host,
        video_id       = video_id,
        now            = now,
        published_date = published_date,
        recorded_date  = recorded_date,
        title_esc      = title_esc,
        author_esc     = author_esc,
        author_id      = author_id,
        author_id_esc  = author_id_esc,
        desc_esc       = desc_esc,
        duration       = duration,
        views          = views,
        like_count     = like_count,
        dislike_count  = dislike_count,
        num_raters     = num_raters,
        middle_time_fmt = seconds_to_time(middle_time),
        end_time_fmt   = seconds_to_time(end_time),
        primary        = instance,
    );

    atom_response(feed_xml)
}

// USER PROFILE

pub async fn user_profile(Path(username): Path<String>) -> impl IntoResponse {
    info!("USER_PROFILE: /feeds/api/users/{}", username);

    let client = &*HTTP_CLIENT;

    let channel_path = format!("/channels/{}", username);
    let uploads_path = format!("/channels/{}/videos?page=1", username);

    let (channel_resp, uploads_resp) = tokio::join!(
        fetch_invidious_json(client, &channel_path),
        fetch_invidious_json(client, &uploads_path)
    );

    let (channel_name, thumbnail_url, subscriber_count, video_count, description) =
        if let Some((data, _instance)) = channel_resp {
            let name = data.get("author").and_then(|v| v.as_str())
                .unwrap_or(&username).to_string();
            let thumb = data.get("authorThumbnails")
                .and_then(|v| v.as_array())
                .and_then(|a| a.last())
                .and_then(|t| t.get("url").and_then(|u| u.as_str()))
                .unwrap_or("https://yt3.googleusercontent.com/default")
                .to_string();
            let subs  = data.get("subCount").and_then(|v| v.as_i64()).unwrap_or(0);
            let vcount = data.get("videoCount").and_then(|v| v.as_i64()).unwrap_or(0);
            let desc  = data.get("description").and_then(|v| v.as_str())
                .unwrap_or("").chars().take(500).collect::<String>();
            (name, thumb, subs, vcount, desc)
        } else {
            (username.clone(), "https://yt3.googleusercontent.com/default".to_string(), 0, 0, String::new())
        };

    let total_uploads = if video_count > 0 {
        video_count
    } else if let Some((data, _)) = uploads_resp {
        data.get("videoCount").and_then(|v| v.as_i64())
            .or_else(|| data.get("videos").and_then(|v| v.as_array()).map(|a| a.len() as i64))
            .unwrap_or(0)
    } else {
        0
    };

    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let name_esc = escape_xml(&channel_name);
    let desc_esc = escape_xml(&description);
    let host = &crate::CONFIG.server_host;

    let xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
<entry xmlns="http://www.w3.org/2005/Atom"
       xmlns:media="http://search.yahoo.com/mrss/"
       xmlns:gd="http://schemas.google.com/g/2005"
       xmlns:yt="http://gdata.youtube.com/schemas/2007">
  <id>http://{host}/feeds/api/users/{username}</id>
  <published>2005-04-15T00:00:00.000Z</published>
  <updated>{now}</updated>
  <category scheme="http://schemas.google.com/g/2005#kind" term="http://gdata.youtube.com/schemas/2007#userProfile"/>
  <title type="text">{name_esc}</title>
  <content type="text">{desc_esc}</content>
  <link rel="alternate" type="text/html" href="http://www.youtube.com/channel/{username}"/>
  <link rel="self" type="application/atom+xml" href="http://{host}/feeds/api/users/{username}?v=2"/>
  <author>
    <name>{name_esc}</name>
    <uri>http://{host}/feeds/api/users/{username}</uri>
  </author>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.contacts"
    href="http://{host}/feeds/api/users/{username}/contacts" countHint="0"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.inbox"
    href="http://{host}/feeds/api/users/{username}/inbox"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.playlists"
    href="http://{host}/feeds/api/users/{username}/playlists"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.subscriptions"
    href="http://{host}/feeds/api/users/{username}/subscriptions" countHint="0"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.uploads"
    href="http://{host}/feeds/api/users/{username}/uploads" countHint="{total_uploads}"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.newsubscriptionvideos"
    href="http://{host}/feeds/api/users/{username}/newsubscriptionvideos"/>
  <yt:maxUploadDuration seconds="0"/>
  <yt:statistics lastWebAccess="{now}" subscriberCount="{subscriber_count}"
    videoWatchCount="0" viewCount="0" totalUploadViews="0"/>
  <media:thumbnail url="{thumbnail_url}"/>
  <yt:username display="{name_esc}">{username}</yt:username>
</entry>"#,
        host            = host,
        username        = username,
        now             = now,
        name_esc        = name_esc,
        desc_esc        = desc_esc,
        subscriber_count = subscriber_count,
        total_uploads   = total_uploads,
        thumbnail_url   = thumbnail_url,
    );

    atom_response(xml)
}

// USER UPLOADS

pub async fn user_uploads(
    Path(username): Path<String>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    let t0 = Instant::now();
    info!("USER_UPLOADS: /feeds/api/users/{}/uploads", username);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let start = params.start_index.unwrap_or(1);
    let limit = params.max_results.unwrap_or(25).min(50);

    let page = ((start.saturating_sub(1)) / 30) + 1;
    let first_offset = (start.saturating_sub(1)) % 30;

    let mut entries: Vec<InvidiousVideo> = Vec::new();
    let mut total_video_count: usize = 0;
    let mut instance_url = instance.clone();
    let mut page_to_fetch = page;
    let mut fetched_any = false;

    while entries.len() < limit {
        let path = format!("/channels/{}/videos?page={}", username, page_to_fetch);
        if let Some((data, used_instance)) = fetch_invidious_json(client, &path).await {
            fetched_any = true;
            instance_url = used_instance;

            if total_video_count == 0 {
                total_video_count = data.get("videoCount").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            }

            if let Some(videos) = data.get("videos").and_then(|v| v.as_array()) {
                info!("Invidious page {} returned {} videos", page_to_fetch, videos.len());
                for (idx, video) in videos.iter().enumerate() {
                    if page_to_fetch == page && idx < first_offset {
                        continue;
                    }
                    if entries.len() >= limit {
                        break;
                    }

                    if let (Some(video_id), Some(title), Some(author)) = (
                        video.get("videoId").and_then(|v| v.as_str()),
                        video.get("title").and_then(|v| v.as_str()),
                        video.get("author").and_then(|v| v.as_str()),
                    ) {
                        let live_now = video.get("liveNow").and_then(|v| v.as_bool()).unwrap_or(false);
                        let length_seconds = video.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        if live_now || length_seconds == 0 {
                            continue;
                        }

                        entries.push(InvidiousVideo {
                            video_id: video_id.to_string(),
                            title: title.to_string(),
                            author: author.to_string(),
                            author_id: video.get("authorId").and_then(|v| v.as_str()).unwrap_or(&username).to_string(),
                            length_seconds,
                            view_count: video.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0),
                            published_text: String::new(),
                            published: video.get("published").and_then(|v| v.as_i64()).unwrap_or(0),
                            uploaded_text: video.get("uploadedText").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            live_now: Some(false),
                            likes: video.get("likes").and_then(|v| v.as_i64()).unwrap_or(0),
                            dislikes: video.get("dislikes").and_then(|v| v.as_i64()).unwrap_or(0),
                            comment_count: video.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0),
                        });
                    }
                }

                if entries.len() >= limit {
                    break;
                }
                if videos.is_empty() {
                    break;
                }
            } else {
                break;
            }

            page_to_fetch += 1;
            if page_to_fetch > page + 1 {
                break;
            }
        } else {
            warn!("No working Invidious instance found for channel uploads");
            break;
        }
    }

    if !fetched_any {
        warn!("Failed to fetch any uploads for user {}", username);
    }

    let next_link = if start + limit <= total_video_count {
        Some(format!(
            "http://{}/feeds/api/users/{}/uploads?start-index={}&max-results={}",
            crate::CONFIG.server_host,
            username,
            start + limit,
            limit
        ))
    } else {
        None
    };

    let xml = build_uploads_feed_xml(
        &format!("users/{}/uploads", username),
        &format!("Uploads from {}", username),
        &entries,
        start,
        limit,
        total_video_count.max(entries.len()),
        next_link,
        &instance_url,
    );

    info!("user_uploads: {} bytes, {}ms", xml.len(), t0.elapsed().as_millis());
    atom_response(xml)
}

fn build_uploads_feed_xml(
    feed_id: &str,
    title: &str,
    videos: &[InvidiousVideo],
    start: usize,
    limit: usize,
    total_results: usize,
    next_link: Option<String>,
    instance_url: &str,
) -> String {
    let base = build_feed_xml(feed_id, title, videos, start, limit, next_link, instance_url);
    base.replacen(
        &format!("<openSearch:totalResults>{}</openSearch:totalResults>", videos.len()),
        &format!("<openSearch:totalResults>{}</openSearch:totalResults>", total_results),
        1,
    )
}

// RELATED VIDEOS

pub async fn related_videos(Path(video_id): Path<String>) -> impl IntoResponse {
    info!("RELATED_VIDEOS: /feeds/api/videos/{}/related", video_id);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let mut entries: Vec<InvidiousVideo> = Vec::new();

    // Fetch related videos from `recommendedVideos`
    if let Ok(resp) = client
        .get(&format!("{}/api/v1/videos/{}", instance, video_id))
        .send()
        .await 
    {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            if let Some(recommended) = data.get("recommendedVideos").and_then(|v| v.as_array()) {
                for item in recommended {
                    if let (Some(vid), Some(title)) = (
                        item.get("videoId").and_then(|v| v.as_str()),
                        item.get("title").and_then(|v| v.as_str()),
                    ) {
                        let length_seconds = item.get("lengthSeconds")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) as i32;
                        
                        if length_seconds == 0 { continue; }

                        let (published_ts, published_text) = extract_published_date(item);
                        
                        // Convert `viewCountText` to a numeric view count
                        let view_count = parse_view_count_text(
                            item.get("viewCountText").and_then(|v| v.as_str()).unwrap_or("0")
                        );

                        entries.push(InvidiousVideo {
                            video_id: vid.to_string(),
                            title: title.to_string(),
                            author: item.get("author")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Unknown")
                                .to_string(),
                            author_id: item.get("authorId")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            length_seconds,
                            view_count,
                            published_text,
                            published: published_ts,
                            uploaded_text: String::new(),
                            live_now: None,
                            likes: 0, // API nie zwraca likes dla recommended
                            dislikes: 0, // API nie zwraca dislikes dla recommended
                            comment_count: 0, // API nie zwraca commentCount dla recommended
                        });
                    }
                }
            }
        }
    }

    let len = entries.len();
    let xml = build_feed_xml(
        &format!("videos/{}/related", video_id),
        "Related videos",
        &entries,
        1,
        len,
        None,
        &instance,
    );
    atom_response(xml)
}

// Helper function to convert `viewCountText` to a numeric view count
fn parse_view_count_text(text: &str) -> i64 {
    if text.is_empty() || text == "0" {
        return 0;
    }
    
    let text = text.trim().to_uppercase();
    
    // Remove suffixes like "VIEWS" or similar
    let text = text.split_whitespace().next().unwrap_or(&text);
    
    // Parsuj liczby z sufiksami
    if let Some(num_str) = text.strip_suffix('B') {
        if let Ok(num) = num_str.parse::<f64>() {
            return (num * 1_000_000_000.0) as i64;
        }
    } else if let Some(num_str) = text.strip_suffix('M') {
        if let Ok(num) = num_str.parse::<f64>() {
            return (num * 1_000_000.0) as i64;
        }
    } else if let Some(num_str) = text.strip_suffix('K') {
        if let Ok(num) = num_str.parse::<f64>() {
            return (num * 1_000.0) as i64;
        }
    } else if let Ok(num) = text.parse::<i64>() {
        return num;
    }
    
    // Try removing commas and dots
    let cleaned = text.replace(',', "").replace('.', "");
    if let Ok(num) = cleaned.parse::<i64>() {
        return num;
    }
    
    0
}

// ─────────────────────────────────────────────────────────────
//  VIDEO COMMENTS
// ─────────────────────────────────────────────────────────────

pub async fn video_comments(
    Path(video_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    let t0 = Instant::now();
    info!("VIDEO_COMMENTS: /feeds/api/videos/{}/comments", video_id);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let start_index  = params.start_index.unwrap_or(1);
    let max_results  = params.max_results.unwrap_or(25).min(100);

    let mut comments_xml = String::new();
    let mut total_results: usize = 0;
    let host = &crate::CONFIG.server_host;

    if let Ok(resp) = client.get(&format!("{}/api/v1/comments/{}", instance, video_id)).send().await {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            let comments = data.get("comments")
                .and_then(|v| v.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);

            total_results = data.get("commentCount")
                .and_then(|v| v.as_u64())
                .unwrap_or(comments.len() as u64) as usize;

            let now_str = Utc::now().to_rfc3339();

            for comment in comments.iter().skip(start_index.saturating_sub(1)).take(max_results) {
                let comment_id  = comment.get("commentId").and_then(|v| v.as_str()).unwrap_or("");
                let author      = comment.get("author").and_then(|v| v.as_str()).unwrap_or("Unknown");
                let author_id   = comment.get("authorId").and_then(|v| v.as_str()).unwrap_or("");
                let content     = comment.get("content").and_then(|v| v.as_str()).unwrap_or("");
                let likes       = comment.get("likeCount").and_then(|v| v.as_i64()).unwrap_or(0);
                let published   = comment.get("published").and_then(|v| v.as_i64())
                    .map(timestamp_to_rfc3339)
                    .unwrap_or_else(|| now_str.clone());

                comments_xml.push_str(&format!(
                    r#"  <entry>
    <id>tag:youtube.com,2008:comment:{video_id}:{cid}</id>
    <published>{pub}</published>
    <updated>{now}</updated>
    <category scheme='http://schemas.google.com/g/2005#kind' term='http://gdata.youtube.com/schemas/2007#comment'/>
    <title type='text'>Comment by {author_esc}</title>
    <content type='text'>{content_esc}</content>
    <link rel='alternate' type='text/html' href='http://www.youtube.com/watch?v={video_id}&amp;lc={cid}'/>
    <link rel='self' type='application/atom+xml' href='http://{host}/feeds/api/videos/{video_id}/comments/{cid}?v=2'/>
    <author>
      <name>{author_esc}</name>
      <uri>http://{host}/feeds/api/users/{author_id_esc}</uri>
      <yt:userId>{author_id_esc}</yt:userId>
    </author>
    <yt:videoid>{video_id}</yt:videoid>
    <gd:rating average='5' max='5' min='1' numRaters='{likes}'/>
  </entry>
"#,
                    host          = host,
                    video_id      = video_id,
                    cid           = escape_xml(comment_id),
                    pub           = published,
                    now           = now_str,
                    author_esc    = escape_xml(author),
                    content_esc   = escape_xml(content),
                    author_id_esc = escape_xml(author_id),
                    likes         = likes,
                ));
            }
        }
    }

    let now = Utc::now().to_rfc3339();
    let feed_xml = format!(
    r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns='http://www.w3.org/2005/Atom'
      xmlns:yt='http://gdata.youtube.com/schemas/2007'
      xmlns:gd='http://schemas.google.com/g/2005'
      xmlns:openSearch='http://a9.com/-/spec/opensearch/1.1/'>
  <id>http://{host}/feeds/api/videos/{video_id}/comments</id>
  <updated>{now}</updated>
  <category scheme='http://schemas.google.com/g/2005#kind' term='http://gdata.youtube.com/schemas/2007#comment'/>
  <title type='text'>Comments on Video</title>
  <logo>http://www.gstatic.com/youtube/img/logo.png</logo>
  <link rel='alternate' type='text/html' href='http://www.youtube.com/watch?v={video_id}'/>
  <link rel='self' type='application/atom+xml' href='http://{host}/feeds/api/videos/{video_id}/comments?v=2'/>
  <author><name>YouTube</name><uri>http://www.youtube.com/</uri></author>
  <generator version='2.1' uri='http://{host}'>YouTube data API</generator>
  <openSearch:totalResults>{total}</openSearch:totalResults>
  <openSearch:startIndex>{start}</openSearch:startIndex>
  <openSearch:itemsPerPage>{per_page}</openSearch:itemsPerPage>
{comments_xml}</feed>"#,
    host        = host,
    video_id    = video_id,
    now         = now,
    total       = total_results,
    start       = start_index,
    per_page    = max_results,
    comments_xml = comments_xml,
);
    info!("comments: {} bytes, {}ms", feed_xml.len(), t0.elapsed().as_millis());
    atom_response(feed_xml)
}

// ─────────────────────────────────────────────────────────────
//  PLAYLISTS (stub)
// ─────────────────────────────────────────────────────────────

pub async fn user_playlists(Path(username): Path<String>) -> impl IntoResponse {
    let host = &crate::CONFIG.server_host;
    let xml = format!(
        r#"<?xml version='1.0' encoding='UTF-8'?>
<feed xmlns='http://www.w3.org/2005/Atom'
      xmlns:openSearch='http://a9.com/-/spec/opensearch/1.1/'
      xmlns:yt='http://gdata.youtube.com/schemas/2007'>
  <id>http://{host}/feeds/api/users/{u}/playlists</id>
  <updated>{now}</updated>
  <title type='text'>Playlists of {u}</title>
  <openSearch:totalResults>0</openSearch:totalResults>
  <openSearch:startIndex>1</openSearch:startIndex>
  <openSearch:itemsPerPage>25</openSearch:itemsPerPage>
</feed>"#,
        host = host,
        u = escape_xml(&username),
        now = Utc::now().to_rfc3339()
    );
    atom_response(xml)
}

pub async fn playlist_details(Path(playlist_id): Path<String>) -> impl IntoResponse {
    let host = &crate::CONFIG.server_host;
    let xml = format!(
        r#"<?xml version='1.0' encoding='UTF-8'?>
<feed xmlns='http://www.w3.org/2005/Atom'
      xmlns:openSearch='http://a9.com/-/spec/opensearch/1.1/'>
  <id>http://{host}/feeds/api/playlists/{p}</id>
  <updated>{now}</updated>
  <title type='text'>Playlist</title>
  <openSearch:totalResults>0</openSearch:totalResults>
  <openSearch:startIndex>1</openSearch:startIndex>
  <openSearch:itemsPerPage>25</openSearch:itemsPerPage>
</feed>"#,
        host = host,
        p   = escape_xml(&playlist_id),
        now = Utc::now().to_rfc3339()
    );
    atom_response(xml)
}

// ─────────────────────────────────────────────────────────────
//  CATEGORIES
// ─────────────────────────────────────────────────────────────

pub async fn categories() -> impl IntoResponse {
    let xml = include_str!("../categories.xml");
    atom_response(xml.to_string())
}

// ─────────────────────────────────────────────────────────────
//  HELPER
// ─────────────────────────────────────────────────────────────

fn atom_response(xml: String) -> axum::response::Response {
    let mut resp = xml.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/atom+xml; charset=UTF-8".parse().unwrap(),
    );
    resp
}

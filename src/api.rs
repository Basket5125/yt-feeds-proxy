use axum::{
    extract::{Path, Query},
    response::{IntoResponse, Json, Response},
    http::{StatusCode, header},
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn, debug};
use chrono::Utc;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand::Rng;
use reqwest::Client;
use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use std::path::Path as StdPath;
use anyhow::Result;
use tokio::fs;
use serde_json::json;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;


// ============================================================
// CACHE SYSTEM
// ============================================================

#[derive(Clone)]
struct CacheEntry {
    data: YouTubeVideoListResponse,
    timestamp: i64,
    expires_at: i64,
    hits: u64,
}

struct ResponseCache {
    cache: RwLock<HashMap<u64, CacheEntry>>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    last_cleanup: AtomicU64,
}

impl ResponseCache {
    fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            last_cleanup: AtomicU64::new(0),
        }
    }

    fn generate_cache_key(region: &str, category_id: Option<&str>, page_token: Option<&str>, max_results: i32) -> u64 {
        let mut hasher = DefaultHasher::new();
        region.hash(&mut hasher);
        if let Some(cat) = category_id {
            cat.hash(&mut hasher);
        }
        if let Some(token) = page_token {
            token.hash(&mut hasher);
        }
        max_results.hash(&mut hasher);
        hasher.finish()
    }

    async fn get(&self, key: u64) -> Option<YouTubeVideoListResponse> {
        let now = Utc::now().timestamp() as u64;
        
        let last_clean = self.last_cleanup.load(Ordering::Relaxed);
        if now.saturating_sub(last_clean) > 300 {
            self.cleanup().await;
            self.last_cleanup.store(now, Ordering::Relaxed);
        }

        let cache = self.cache.read().await;
        if let Some(entry) = cache.get(&key) {
            if entry.expires_at > now as i64 {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                debug!("Cache HIT for key: {}", key);
                return Some(entry.data.clone());
            } else {
                debug!("Cache EXPIRED for key: {}", key);
            }
        }
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    async fn set(&self, key: u64, data: YouTubeVideoListResponse, ttl_seconds: u64) {
        let now = Utc::now().timestamp();
        let entry = CacheEntry {
            data,
            timestamp: now,
            expires_at: now + ttl_seconds as i64,
            hits: 0,
        };
        
        let mut cache = self.cache.write().await;
        cache.insert(key, entry);
        debug!("Cached response for key: {}, expires in {} seconds", key, ttl_seconds);
    }

    async fn cleanup(&self) {
        let now = Utc::now().timestamp();
        let mut cache = self.cache.write().await;
        let before = cache.len();
        
        cache.retain(|_, entry| entry.expires_at > now);
        
        let after = cache.len();
        if before != after {
            info!("Cache cleaned: removed {} expired entries, {} remaining", before - after, after);
        }
    }

    async fn get_stats(&self) -> (u64, u64, usize) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let size = self.cache.read().await.len();
        (hits, misses, size)
    }

    async fn clear(&self) {
        let mut cache = self.cache.write().await;
        let size = cache.len();
        cache.clear();
        info!("Cache cleared: {} entries removed", size);
    }
}

static RESPONSE_CACHE: Lazy<Arc<ResponseCache>> = Lazy::new(|| {
    Arc::new(ResponseCache::new())
});

const CACHE_TTL_SECONDS: u64 = 10800;

// ============================================================
// CATEGORY MAPPING
// ============================================================

const CATEGORY_MAP: &[(&str, &str)] = &[
    ("1", "Film & Animation"),
    ("2", "Autos & Vehicles"),
    ("10", "Music"),
    ("15", "Pets & Animals"),
    ("17", "Sports"),
    ("20", "Gaming"),
    ("22", "People & Blogs"),
    ("23", "Comedy"),
    ("24", "Entertainment"),
    ("25", "News & Politics"),
    ("26", "Howto & Style"),
    ("27", "Education"),
    ("28", "Science & Technology"),
];

lazy_static::lazy_static! {
    static ref CATEGORY_NAME_TO_ID: HashMap<String, String> = {
        let mut map = HashMap::new();
        for (id, name) in CATEGORY_MAP {
            map.insert(name.to_string(), id.to_string());
            let simple_name = name.replace(" & ", " ");
            map.insert(simple_name.clone(), id.to_string());

            if let Some(first_part) = name.split(" & ").next() {
                map.insert(first_part.to_string(), id.to_string());
            }

            if let Some(last_part) = name.split(" & ").nth(1) {
                map.insert(last_part.to_string(), id.to_string());
            }
        }
        map
    };
}

// ============================================================
// STRUCTURES FOR VIDEO FORMATS
// ============================================================

#[derive(Debug, Clone)]
struct StreamInfo {
    url: String,
    mime_type: String,
    itag: u32,
    content_length: Option<u64>,
    codecs: String,
    quality: String,
}

#[derive(Debug, Deserialize, Clone)]
struct InvidiousVideoFormats {
    #[serde(rename = "adaptiveFormats")]
    adaptive_formats: Vec<AdaptiveFormat>,
    #[serde(rename = "formatStreams")]
    format_streams: Vec<FormatStream>,
}

#[derive(Debug, Deserialize, Clone)]
struct AdaptiveFormat {
    itag: u32,
    url: String,
    #[serde(rename = "type")]
    format_type: String,
    bitrate: Option<String>,
    #[serde(rename = "mimeType")]
    mime_type: Option<String>,
    #[serde(rename = "qualityLabel")]
    quality_label: Option<String>,
    #[serde(rename = "resolution")]
    resolution: Option<String>,
    #[serde(rename = "fps")]
    fps: Option<u32>,
    #[serde(rename = "clen")]
    content_length: Option<String>,
    #[serde(rename = "audioChannels")]
    audio_channels: Option<u32>,
    #[serde(rename = "audioSampleRate")]
    audio_sample_rate: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
struct FormatStream {
    itag: u32,
    url: String,
    #[serde(rename = "type")]
    format_type: String,
    #[serde(rename = "qualityLabel")]
    quality_label: Option<String>,
    #[serde(rename = "resolution")]
    resolution: Option<String>,
}

// ============================================================
// YOUTUBE API STRUCTURES
// ============================================================

#[derive(Debug, Deserialize, Clone)]
struct YouTubeVideoListResponse {
    items: Vec<YouTubeVideoItem>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeVideoItem {
    id: String,
    snippet: YouTubeSnippet,
    statistics: YouTubeStatistics,
    #[serde(rename = "contentDetails")]
    content_details: YouTubeContentDetails,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeContentDetails {
    duration: String,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeSnippet {
    title: String,
    #[serde(rename = "channelId")]
    channel_id: String,
    #[serde(rename = "channelTitle")]
    channel_title: String,
    #[serde(rename = "publishedAt")]
    published_at: String,
    description: Option<String>,
    thumbnails: YouTubeThumbnails,
    #[serde(rename = "categoryId")]
    category_id: String,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeThumbnails {
    high: YouTubeThumbnail,
    default: Option<YouTubeThumbnail>,
    medium: Option<YouTubeThumbnail>,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeThumbnail {
    url: String,
    width: Option<i32>,
    height: Option<i32>,
}

#[derive(Debug, Deserialize, Clone)]
struct YouTubeStatistics {
    #[serde(rename = "viewCount")]
    view_count: Option<String>,
    #[serde(rename = "likeCount")]
    like_count: Option<String>,
    #[serde(rename = "commentCount")]
    comment_count: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct PageInfo {
    #[serde(rename = "totalResults")]
    total_results: i32,
    #[serde(rename = "resultsPerPage")]
    results_per_page: i32,
}

// ============================================================
// INVIDIOUS VIDEO STRUCT
// ============================================================

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

// ============================================================
// STATS SYSTEM
// ============================================================

static REQUEST_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static PERSISTED_STATS: Lazy<Arc<RwLock<Option<StatsResponse>>>> = Lazy::new(|| Arc::new(RwLock::new(None)));

#[derive(Clone)]
struct AnalyticsEvent {
    path: String,
    user_agent: String,
    country: String,
    ts: i64,
}

struct Analytics {
    events: Vec<AnalyticsEvent>,
    registered_devices: HashSet<String>,
}

static ANALYTICS: Lazy<Arc<RwLock<Analytics>>> = Lazy::new(|| Arc::new(RwLock::new(Analytics {
    events: Vec::new(),
    registered_devices: HashSet::new(),
})));

const STATS_FILE: &str = "stats.json";

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

pub async fn init_stats() -> Result<()> {
    if StdPath::new(STATS_FILE).exists() {
        let data = fs::read_to_string(STATS_FILE).await?;
        if let Ok(stats) = serde_json::from_str::<StatsResponse>(&data) {
            let mut persisted = PERSISTED_STATS.write().await;
            *persisted = Some(stats);
        }
    }
    info!("Stats system initialized");
    Ok(())
}

pub async fn save_stats_snapshot() -> Result<()> {
    let persisted = PERSISTED_STATS.read().await;
    let a = ANALYTICS.read().await;
    let stats = compute_stats_response(&*persisted, &a.events, a.registered_devices.len());
    let json = serde_json::to_string_pretty(&stats)?;
    fs::write(STATS_FILE, json).await?;
    Ok(())
}

pub async fn record_request(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path().to_string();
    let headers = req.headers();
    let ua = headers.get(axum::http::header::USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("Unknown").to_string();
    let country = headers.get("cf-ipcountry").or_else(|| headers.get("CF-IPCountry")).and_then(|v| v.to_str().ok()).unwrap_or("unknown").to_string();
    let ts = Utc::now().timestamp();

    let event = AnalyticsEvent { path: path.clone(), user_agent: ua.clone(), country: country.clone(), ts };
    {
        let mut a = ANALYTICS.write().await;
        a.events.push(event);
    }

    REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
    next.run(req).await
}

pub async fn stats() -> impl IntoResponse {
    let persisted = PERSISTED_STATS.read().await;
    let a = ANALYTICS.read().await;
    let resp = compute_stats_response(&*persisted, &a.events, a.registered_devices.len());
    (StatusCode::OK, Json(resp)).into_response()
}

fn compute_stats_response(base: &Option<StatsResponse>, events: &[AnalyticsEvent], registered_devices: usize) -> StatsResponse {
    let now = Utc::now().timestamp();
    let overall_counts = compute_window_counts(events, None);
    let last_7_counts = compute_window_counts(events, Some(now - 7 * 24 * 3600));
    let last_30_counts = compute_window_counts(events, Some(now - 30 * 24 * 3600));

    let base_overall = base.as_ref().map(|b| &b.overall);
    let base_7 = base.as_ref().map(|b| &b.last_7_days);
    let base_30 = base.as_ref().map(|b| &b.last_30_days);

    let overall = if let Some(base_stats) = base_overall {
        merge_window_stats(base_stats, &overall_counts, registered_devices)
    } else {
        window_stats_from_counts(overall_counts, registered_devices)
    };

    let last_7 = if let Some(base_stats) = base_7 {
        merge_window_stats(base_stats, &last_7_counts, registered_devices)
    } else {
        window_stats_from_counts(last_7_counts, registered_devices)
    };

    let last_30 = if let Some(base_stats) = base_30 {
        merge_window_stats(base_stats, &last_30_counts, registered_devices)
    } else {
        window_stats_from_counts(last_30_counts, registered_devices)
    };

    StatsResponse { overall, last_7_days: last_7, last_30_days: last_30 }
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

fn window_stats_from_counts(counts: WindowCounts, registered_devices: usize) -> WindowStats {
    WindowStats {
        total_events: counts.total_events,
        registered_devices,
        top_endpoints: top_n_from_counts(counts.endpoints, 10),
        top_user_agents: top_n_from_counts(counts.user_agents, 10),
        top_countries: top_n_from_counts(counts.countries, 10),
    }
}

fn top_n_from_counts(counts: HashMap<String, usize>, n: usize) -> Vec<TopItem> {
    let mut v: Vec<(String, usize)> = counts.into_iter().collect();
    v.sort_by(|a,b| b.1.cmp(&a.1));
    v.into_iter().take(n).map(|(k,c)| TopItem{ key: k, count: c }).collect()
}

// ============================================================
// YOUTUBE API FETCH WITH CACHE
// ============================================================

async fn fetch_popular_videos_from_youtube(
    client: &Client,
    region_code: &str,
    category_id: Option<&str>,
    max_results: i32,
    page_token: Option<&str>,
) -> Result<YouTubeVideoListResponse> {
    let cache_key = ResponseCache::generate_cache_key(
        region_code,
        category_id,
        page_token,
        max_results
    );
    
    if let Some(cached_data) = RESPONSE_CACHE.get(cache_key).await {
        info!("Using cached response for region={}, category={:?}", region_code, category_id);
        return Ok(cached_data);
    }
    
    info!("Cache miss - fetching from YouTube API for region={}, category={:?}", region_code, category_id);
    
    let api_key = if !crate::CONFIG.youtube_api_key.is_empty() {
        crate::CONFIG.youtube_api_key.clone()
    } else {
        std::env::var("YOUTUBE_API_KEY").unwrap_or_default()
    };
    
    if api_key.is_empty() {
        return Err(anyhow::anyhow!("No YouTube API key configured. Set YOUTUBE_API_KEY env var or add youtube_api_key to config.json"));
    }
    
    let mut url = format!(
        "https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&chart=mostPopular&regionCode={}&maxResults={}&key={}",
        region_code, max_results, api_key
    );
    
    if let Some(cat_id) = category_id {
        url.push_str(&format!("&videoCategoryId={}", cat_id));
    }
    
    if let Some(token) = page_token {
        url.push_str(&format!("&pageToken={}", token));
    }
    
    debug!("YouTube API URL: {}", url);
    
    let response = client.get(&url).send().await?;
    
    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        warn!("YouTube API error ({}): {}", status, error_text);
        return Err(anyhow::anyhow!("YouTube API error ({}): {}", status, error_text));
    }
    
    let data: YouTubeVideoListResponse = response.json().await?;
    
    RESPONSE_CACHE.set(cache_key, data.clone(), CACHE_TTL_SECONDS).await;
    info!("Cached response for key: {} (expires in {} seconds)", cache_key, CACHE_TTL_SECONDS);
    
    Ok(data)
}

// ============================================================
// MAIN FEED HANDLER
// ============================================================

pub async fn standard_feed(
    Path((region, feed)): Path<(String, String)>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    let t0 = Instant::now();
    info!("STANDARD_FEED: /feeds/api/standardfeeds/{}/{}", region, feed);
    
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    
    let (category_name, category_id) = if let Some(cat_name) = feed.strip_prefix("most_popular_") {
        let cat_name = cat_name.replace("_", " ");
        let cat_id = CATEGORY_NAME_TO_ID.get(&cat_name)
            .or_else(|| CATEGORY_NAME_TO_ID.get(&format!("{} & {}", cat_name, cat_name)))
            .cloned();
        (Some(cat_name), cat_id)
    } else {
        match feed.as_str() {
            "most_popular" => (None, None),
            "top_rated" => (None, None),
            "most_discussed" => (None, None),
            "recently_featured" => (None, None),
            "top_favorites" => (None, None),
            _ => {
                if let Some(cat_name) = feed.strip_prefix("top_rated_") {
                    let cat_name = cat_name.replace("_", " ");
                    let cat_id = CATEGORY_NAME_TO_ID.get(&cat_name)
                        .or_else(|| CATEGORY_NAME_TO_ID.get(&format!("{} & {}", cat_name, cat_name)))
                        .cloned();
                    (Some(cat_name), cat_id)
                } else {
                    (None, None)
                }
            }
        }
    };
    
    let start = params.start_index.unwrap_or(1);
    let limit = params.max_results.unwrap_or(18).min(50);
    
    let yt_response = match fetch_popular_videos_from_youtube(
        client,
        &region,
        category_id.as_deref(),
        limit as i32,
        None,
    ).await {
        Ok(resp) => resp,
        Err(e) => {
            warn!("Failed to fetch from YouTube API: {}", e);
            return fallback_standard_feed(&region, &feed, &params, &client, &instance).await;
        }
    };
    
    let videos: Vec<InvidiousVideo> = yt_response.items
        .iter()
        .map(|item| convert_youtube_to_invidious(item))
        .collect();
    
    let paginated: Vec<InvidiousVideo> = videos
        .into_iter()
        .skip(start.saturating_sub(1))
        .take(limit)
        .collect();
    
    let feed_title = if let Some(cat_name) = category_name {
        format!("YouTube Most Popular {} Videos in {}", cat_name, region)
    } else {
        match feed.as_str() {
            "most_popular" => format!("YouTube Most Popular Videos in {}", region),
            "top_rated" => format!("YouTube Top Rated Videos in {}", region),
            "most_discussed" => format!("YouTube Most Discussed Videos in {}", region),
            "recently_featured" => format!("YouTube Recently Featured Videos in {}", region),
            "top_favorites" => format!("YouTube Top Favorites Videos in {}", region),
            _ => format!("YouTube Videos in {}", region),
        }
    };
    
    // TYLKO WYWOŁANIE, BEZ DEFINICJI
    let xml = build_feed_xml(
        &format!("standardfeeds/{}/{}", region, feed),
        &feed_title,
        &paginated,
        start,
        limit,
        None,
        &instance,
    );
    
    info!("standard_feed: {} bytes, {}ms", xml.len(), t0.elapsed().as_millis());
    atom_response(xml)
}  // <-- zamknięcie funkcji standard_feed
    


async fn fallback_standard_feed(
    region: &str,
    feed: &str,
    params: &PaginationParams,
    client: &Client,
    instance: &str,
) -> Response {
    let start = params.start_index.unwrap_or(1);
    let limit = params.max_results.unwrap_or(18).min(50);
    
    let mut videos: Vec<InvidiousVideo> = Vec::new();
    
    let url = format!("{}/api/v1/trending?region={}", instance, region);
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(v) = resp.json::<Vec<InvidiousVideo>>().await {
            let v_len = v.len();
            videos = v.into_iter().filter(|v| !is_live(v)).collect();
            info!("Trending fallback: {}/{} videos", videos.len(), v_len);
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
    
    let feed_title = match feed {
        "most_popular" => "Most Popular",
        "top_rated" => "Top Rated",
        "most_discussed" => "Most Discussed",
        "recently_featured" => "Recently Featured",
        "top_favorites" => "Top Favorites",
        _ => feed,
    };
    
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
        instance,
    );
    
    atom_response(xml)
}

// ============================================================
// ALL ENDPOINTS
// ============================================================

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
    
    let title = data.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let author = data.get("author").and_then(|v| v.as_str()).unwrap_or("");
    let author_id = data.get("authorId").and_then(|v| v.as_str()).unwrap_or("");
    let author_thumbnail_url = data.get("authorThumbnails")
    .and_then(|v| v.as_array())
    .and_then(|arr| arr.last()) 
    .and_then(|thumb| thumb.get("url"))
    .and_then(|url| url.as_str())
    .unwrap_or("");
    let duration = data.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
    let views = data.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let favorite_count = data.get("favoriteCount").and_then(|v| v.as_i64()).unwrap_or(views);
    let description = data.get("description").and_then(|v| v.as_str()).unwrap_or("");
    let published = data.get("published").and_then(|v| v.as_i64()).unwrap_or(0);
    let uploaded = data.get("uploaded").and_then(|v| v.as_i64()).unwrap_or(published);
    let category_name = data.get("category").and_then(|v| v.as_str()).unwrap_or("");
    let keyword_list = data.get("keywords")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|item| item.as_str()).collect::<Vec<_>>())
        .or_else(|| data.get("tags").and_then(|v| v.as_array()).map(|arr| arr.iter().filter_map(|item| item.as_str()).collect::<Vec<_>>() ))
        .unwrap_or_default();
    let like_count = data.get("likeCount").and_then(|v| v.as_i64()).unwrap_or_else(|| (views as f64 * 0.006) as i64);
    let dislike_count = data.get("dislikeCount").and_then(|v| v.as_i64()).unwrap_or_else(|| (views as f64 * 0.001) as i64);
    let num_raters = like_count.max(0);
    let comment_count = data.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let embed_url = format!("{}/embed/{}?raw=1", instance.trim_end_matches('/'), video_id);

    let published_date = if published > 0 {
        chrono::DateTime::from_timestamp(published, 0)
            .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .unwrap_or_else(|| Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    } else {
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    };
    let updated_date = if uploaded > 0 {
        chrono::DateTime::from_timestamp(uploaded, 0)
            .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .unwrap_or_else(|| published_date.clone())
    } else {
        published_date.clone()
    };

    let title_esc = escape_xml(title);
    let author_esc = escape_xml(author);
    let author_id_esc = escape_xml(author_id);
    let description_esc = escape_xml(description);
    let category_esc = escape_xml(category_name);
    let host = &crate::CONFIG.server_host;
    
    let keywords_entries = if !keyword_list.is_empty() {
        keyword_list.iter().filter(|k| !k.trim().is_empty()).map(|keyword| {
            let keyword_esc = escape_xml(keyword);
            format!("<category scheme='http://gdata.youtube.com/schemas/2007/keywords.cat' term='{}'/>", keyword_esc)
        }).collect::<Vec<_>>().join("")
    } else {
        String::new()
    };

    let category_entry = if !category_esc.is_empty() {
        format!("<category scheme='http://gdata.youtube.com/schemas/2007/categories.cat' term='{}' label='{}'/>", category_esc, category_esc)
    } else {
        String::new()
    };

    let xml = format!(r#"<?xml version='1.0' encoding='UTF-8'?><entry xmlns='http://www.w3.org/2005/Atom' xmlns:media='http://search.yahoo.com/mrss/' xmlns:gd='http://schemas.google.com/g/2005' xmlns:yt='http://gdata.youtube.com/schemas/2007'>
<id>http://{host}/feeds/api/videos/{video_id}</id>
<youTubeId id='{video_id}'>{video_id}</youTubeId>
<published>{published_date}</published>
<updated>{updated_date}</updated>
<category scheme='http://schemas.google.com/g/2005#kind' term='http://gdata.youtube.com/schemas/2007#video'/>
{category_entry}
{keywords_entries}
<title type='text'>{title_esc}</title>
<content type='text'>{description_esc}</content>
<link rel='http://gdata.youtube.com/schemas/2007#video.related' type='application/atom+xml' href='http://{host}/feeds/api/videos/{video_id}/related'/>
<link rel='http://gdata.youtube.com/schemas/2007#live.event' href='http://{host}/sabr_hls_adapter/{video_id}/stream.m3u8' start='0' end='1' video='w'/>
<link rel='self' type='application/atom+xml' href='http://{host}/feeds/api/videos/{video_id}'/>
<author>
<name>{author_esc}</name>
<uri>http://{host}/feeds/api/users/{author_id_esc}</uri>
<yt:userId></yt:userId>
<media:thumbnail url='{author_thumbnail_url}'/>
</author>
<gd:comments>
<gd:feedLink href='http://{host}/feeds/api/videos/{video_id}/comments' countHint='{comment_count}'/>
</gd:comments>
<media:group>
<media:category label='{category_esc}' scheme='http://gdata.youtube.com/schemas/2007/categories.cat'>{category_esc}</media:category>
<media:content url='{embed_url}' type='video/3gpp' medium='video' expression='full' duration='{duration}' yt:format='3'/>
<media:content url='{embed_url}' type='video/3gpp' medium='video' expression='full' duration='{duration}' yt:format='1'/>
<media:content url='{embed_url}' type='video/3gpp' medium='video' expression='full' duration='{duration}' yt:format='6'/>
<media:description type='plain'>{description_esc}</media:description>
<media:keywords>{keywords_esc}</media:keywords>
<media:player url='{embed_url}'/>
<media:thumbnail yt:name='hqdefault' url='http://i.ytimg.com/vi/{video_id}/hqdefault.jpg' height='240' width='320' time='00:00:00'/>
<media:thumbnail yt:name='poster' url='http://i.ytimg.com/vi/{video_id}/0.jpg' height='240' width='320' time='00:00:00'/>
<media:thumbnail yt:name='default' url='http://i.ytimg.com/vi/{video_id}/0.jpg' height='240' width='320' time='00:00:00'/>
<media:title type='plain'>{title_esc}</media:title>
<yt:duration seconds='{duration}'/>
<yt:uploaded>{uploaded_date}</yt:uploaded>
<yt:uploaderId>{author_id_esc}</yt:uploaderId>
<yt:videoid id='{video_id}'>{video_id}</yt:videoid>
<media:credit role='uploader' yt:display='{author_esc}' name='{author_esc}'>{author_esc}</media:credit>
</media:group>
<gd:rating average='{average_rating}' max='5' min='1' numRaters='{num_raters}' rel='http://schemas.google.com/g/2005#overall'/>
<yt:statistics favoriteCount='{favorite_count}' viewCount='{views}'/>
<yt:rating numLikes='{like_count}' numDislikes='{dislike_count}'/>
</entry>"#,
        host = host,
        video_id = video_id,
        published_date = published_date,
        updated_date = updated_date,
        category_entry = category_entry,
        keywords_entries = keywords_entries,
        title_esc = title_esc,
        description_esc = description_esc,
        author_esc = author_esc,
        author_id_esc = author_id_esc,
        comment_count = comment_count,
        category_esc = category_esc,
        duration = duration,
        keywords_esc = escape_xml(&keyword_list.join(", ")),
        average_rating = format!("{:.6}", (like_count as f64 / num_raters.max(1) as f64 * 5.0).min(5.0)),
        num_raters = num_raters,
        favorite_count = favorite_count,
        like_count = like_count,
        dislike_count = dislike_count,
        views = views,
        uploaded_date = updated_date,
        embed_url = embed_url,
    );
    
    atom_response(xml)
}

pub async fn video_search(Query(params): Query<SearchParams>) -> impl IntoResponse {
    let t0 = Instant::now();
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let (entries, feed_title, feed_id) = if let Some(q) = params.q.as_deref() {
        if q.is_empty() {
            return (StatusCode::BAD_REQUEST, "Missing query").into_response();
        }

        info!("VIDEO_SEARCH q={}", q);
        let entries = search_videos_from_invidious(client, &instance, q).await;
        (
            entries,
            format!("Search results: {}", q),
            format!("videos?q={}", urlencoding::encode(q)),
        )
    } else if let Some(category_name) = params.category.as_deref() {
        if category_name.is_empty() {
            return (StatusCode::BAD_REQUEST, "Missing category").into_response();
        }

        let region_code = params.region.as_deref().unwrap_or("US");
        let category_id = CATEGORY_NAME_TO_ID
            .get(category_name)
            .cloned()
            .or_else(|| {
                if category_name.chars().all(|c| c.is_ascii_digit()) {
                    Some(category_name.to_string())
                } else {
                    None
                }
            });

        let category_id = match category_id {
            Some(id) => id,
            None => return (StatusCode::BAD_REQUEST, "Unknown category").into_response(),
        };

        info!("VIDEO_SEARCH category={} region={}", category_name, region_code);

        let limit = params.max_results.unwrap_or(20).min(50) as i32;
        let yt_response = match fetch_popular_videos_from_youtube(client, region_code, Some(&category_id), limit, None).await {
            Ok(resp) => resp,
            Err(e) => {
                warn!("Failed to fetch category videos from YouTube API: {}", e);
                return (StatusCode::SERVICE_UNAVAILABLE, "YouTube API error").into_response();
            }
        };

        let entries = yt_response
            .items
            .iter()
            .map(convert_youtube_to_invidious)
            .collect();

        (
            entries,
            format!("YouTube Most Popular {} Videos", category_name),
            format!("videos?category={}", urlencoding::encode(category_name)),
        )
    } else {
        return (StatusCode::BAD_REQUEST, "Missing query or category").into_response();
    };

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
            urlencoding::encode(params.q.as_deref().unwrap_or_default()),
            start + limit,
            limit
        ))
    } else {
        None
    };

    let xml = build_feed_xml(
        &feed_id,
        &feed_title,
        &paginated,
        start,
        limit,
        next_link,
        &instance,
    );

    info!("video_search: {} bytes, {}ms", xml.len(), t0.elapsed().as_millis());
    atom_response(xml)
}

pub async fn user_profile(Path(channel_id): Path<String>) -> impl IntoResponse {
    info!("USER_PROFILE: /feeds/api/users/{}", channel_id);

    let client = &*HTTP_CLIENT;
    let channel_path = format!("/channels/{}", channel_id);
    let uploads_path = format!("/channels/{}/videos?page=1", channel_id);

    let (channel_resp, uploads_resp) = tokio::join!(
        fetch_invidious_json(client, &channel_path),
        fetch_invidious_json(client, &uploads_path)
    );

    let (channel_name, thumbnail_url, subscriber_count, video_count, description) =
        if let Some((data, _instance)) = channel_resp {
            let name = data.get("author").and_then(|v| v.as_str())
                .unwrap_or(&channel_id).to_string();
            let thumb = data.get("authorThumbnails")
                .and_then(|v| v.as_array())
                .and_then(|a| a.last())
                .and_then(|t| t.get("url").and_then(|u| u.as_str()))
                .unwrap_or("https://yt3.googleusercontent.com/default")
                .to_string();
            let subs = data.get("subCount").and_then(|v| v.as_i64()).unwrap_or(0);
            let vcount = data.get("videoCount").and_then(|v| v.as_i64()).unwrap_or(0);
            let desc = data.get("description").and_then(|v| v.as_str())
                .unwrap_or("").chars().take(500).collect::<String>();
            (name, thumb, subs, vcount, desc)
        } else {
            (channel_id.clone(), "https://yt3.googleusercontent.com/default".to_string(), 0, 0, String::new())
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
  <id>http://{host}/feeds/api/users/{channel_id}</id>
  <published>2005-04-15T00:00:00.000Z</published>
  <updated>{now}</updated>
  <category scheme="http://schemas.google.com/g/2005#kind" term="http://gdata.youtube.com/schemas/2007#userProfile"/>
  <title type="text">{name_esc}</title>
  <content type="text">{desc_esc}</content>
  <link rel="alternate" type="text/html" href="http://www.youtube.com/channel/{channel_id}"/>
  <link rel="self" type="application/atom+xml" href="http://{host}/feeds/api/users/{channel_id}?v=2"/>
  <author>
    <name>{name_esc}</name>
    <uri>http://{host}/feeds/api/users/{channel_id}</uri>
  </author>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.contacts"
    href="http://{host}/feeds/api/users/{channel_id}/contacts" countHint="0"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.inbox"
    href="http://{host}/feeds/api/users/{channel_id}/inbox"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.playlists"
    href="http://{host}/feeds/api/users/{channel_id}/playlists"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.subscriptions"
    href="http://{host}/feeds/api/users/{channel_id}/subscriptions" countHint="0"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.uploads"
    href="http://{host}/feeds/api/users/{channel_id}/uploads" countHint="{total_uploads}"/>
  <gd:feedLink rel="http://gdata.youtube.com/schemas/2007#user.newsubscriptionvideos"
    href="http://{host}/feeds/api/users/{channel_id}/newsubscriptionvideos"/>
  <yt:maxUploadDuration seconds="0"/>
  <yt:statistics lastWebAccess="{now}" subscriberCount="{subscriber_count}"
    videoWatchCount="0" viewCount="0" totalUploadViews="0"/>
  <media:thumbnail url="{thumbnail_url}"/>
  <yt:username display="{name_esc}">{channel_id}</yt:username>
</entry>"#,
        host = host,
        channel_id = channel_id,
        now = now,
        name_esc = name_esc,
        desc_esc = desc_esc,
        subscriber_count = subscriber_count,
        total_uploads = total_uploads,
        thumbnail_url = thumbnail_url,
    );

    atom_response(xml)
}

pub async fn user_uploads(Path(channel_id): Path<String>, Query(params): Query<PaginationParams>) -> impl IntoResponse {
    let t0 = Instant::now();
    info!("USER_UPLOADS: /feeds/api/users/{}/uploads", channel_id);

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let start = params.start_index.unwrap_or(1);
    let limit = params.max_results.unwrap_or(25).min(50);

    let mut entries: Vec<InvidiousVideo> = Vec::new();
    let mut instance_url = instance.clone();

    let source_paths = [
        format!("/channels/{}/latest", channel_id),
        format!("/channels/{}/videos?page=1", channel_id),
    ];

    for path in source_paths.iter() {
        if let Some((data, used_instance)) = fetch_invidious_json(client, path).await {
            instance_url = used_instance;

            let videos_opt = if data.is_array() {
                data.as_array().cloned()
            } else {
                data.get("videos").and_then(|v| v.as_array()).cloned()
            };

            if let Some(videos) = videos_opt {
                for video in videos {
                    if let (Some(video_id), Some(title), Some(author)) = (
                        video.get("videoId").and_then(|v| v.as_str()),
                        video.get("title").and_then(|v| v.as_str()),
                        video.get("author").and_then(|v| v.as_str()),
                    ) {
                        let length_seconds = video.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        if length_seconds == 0 { continue; }

                        entries.push(InvidiousVideo {
                            video_id: video_id.to_string(),
                            title: title.to_string(),
                            author: author.to_string(),
                            author_id: video.get("authorId").and_then(|v| v.as_str()).unwrap_or(&channel_id).to_string(),
                            length_seconds,
                            view_count: video.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0),
                            published_text: String::new(),
                            published: video.get("published").and_then(|v| v.as_i64()).unwrap_or(0),
                            uploaded_text: String::new(),
                            live_now: Some(false),
                            likes: 0,
                            dislikes: 0,
                            comment_count: 0,
                        });
                    }
                }
            }

            if !entries.is_empty() {
                break;
            }
        }
    }

    let total = entries.len();
    let paginated: Vec<InvidiousVideo> = entries
        .into_iter()
        .skip(start.saturating_sub(1))
        .take(limit)
        .collect();

    let next_link = if start + limit <= total {
        Some(format!(
            "http://{}/feeds/api/users/{}/uploads?start-index={}&max-results={}",
            crate::CONFIG.server_host,
            channel_id,
            start + limit,
            limit
        ))
    } else {
        None
    };

    let xml = build_feed_xml(
        &format!("users/{}/uploads", channel_id),
        &format!("Uploads from {}", channel_id),
        &paginated,
        start,
        limit,
        next_link,
        &instance_url,
    );

    info!("user_uploads: {} bytes, {}ms", xml.len(), t0.elapsed().as_millis());
    atom_response(xml)
}

pub async fn user_playlists() -> impl IntoResponse {
    info!("user_playlists");
    let host = &crate::CONFIG.server_host;
    let xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:openSearch="http://a9.com/-/spec/opensearch/1.1/">
  <id>http://{host}/feeds/api/users/default/playlists</id>
  <updated>{now}</updated>
  <title type="text">Playlists</title>
  <openSearch:totalResults>0</openSearch:totalResults>
  <openSearch:startIndex>1</openSearch:startIndex>
  <openSearch:itemsPerPage>25</openSearch:itemsPerPage>
</feed>"#,
        host = host,
        now = Utc::now().to_rfc3339()
    );
    atom_response(xml)
}

pub async fn playlist_details(Path(playlist_id): Path<String>) -> impl IntoResponse {
    info!("playlist_details: {}", playlist_id);
    let host = &crate::CONFIG.server_host;
    let xml = format!(r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:openSearch="http://a9.com/-/spec/opensearch/1.1/">
  <id>http://{host}/feeds/api/playlists/{p}</id>
  <updated>{now}</updated>
  <title type="text">Playlist</title>
  <openSearch:totalResults>0</openSearch:totalResults>
  <openSearch:startIndex>1</openSearch:startIndex>
  <openSearch:itemsPerPage>25</openSearch:itemsPerPage>
</feed>"#,
        host = host,
        p = escape_xml(&playlist_id),
        now = Utc::now().to_rfc3339()
    );
    atom_response(xml)
}

pub async fn related_videos(Path(video_id): Path<String>) -> impl IntoResponse {
    info!("RELATED_VIDEOS: /feeds/api/videos/{}/related", video_id);
    
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    
    let mut entries: Vec<InvidiousVideo> = Vec::new();
    
    if let Ok(resp) = client.get(&format!("{}/api/v1/videos/{}", instance, video_id)).send().await {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            if let Some(recommended) = data.get("recommendedVideos").and_then(|v| v.as_array()) {
                for item in recommended {
                    if let (Some(vid), Some(title)) = (
                        item.get("videoId").and_then(|v| v.as_str()),
                        item.get("title").and_then(|v| v.as_str()),
                    ) {
                        let length_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        if length_seconds == 0 { continue; }

                        let published = parse_invidious_timestamp(item.get("published")).unwrap_or(0);
                        let published_text = item.get("publishedText").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let uploaded_text = item.get("uploadedText").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let views = item.get("viewCount").and_then(|v| v.as_i64()).unwrap_or_else(|| {
                            item.get("viewCountText")
                                .and_then(|v| v.as_str())
                                .map(parse_view_count_text)
                                .unwrap_or(0)
                        });
                        let likes = item.get("likeCount").and_then(|v| v.as_i64()).unwrap_or(0);
                        let dislikes = item.get("dislikeCount").and_then(|v| v.as_i64()).unwrap_or(0);
                        let comment_count = item.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0);
                        let author = item.get("author").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();
                        let author_id = item.get("authorId").and_then(|v| v.as_str()).unwrap_or("").to_string();

                        entries.push(InvidiousVideo {
                            video_id: vid.to_string(),
                            title: title.to_string(),
                            author,
                            author_id,
                            length_seconds,
                            view_count: views,
                            published_text,
                            published,
                            uploaded_text,
                            live_now: None,
                            likes,
                            dislikes,
                            comment_count,
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

pub async fn video_comments(
    Path(video_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    info!("VIDEO_COMMENTS: /feeds/api/videos/{}/comments", video_id);
    
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    
    let start_index = params.start_index.unwrap_or(1);
    let max_results = params.max_results.unwrap_or(25).min(100);
    
    let mut comments_xml = String::new();
    let mut total_results: usize = 0;
    let host = &crate::CONFIG.server_host;
    
    if let Ok(resp) = client.get(&format!("{}/api/v1/comments/{}", instance, video_id)).send().await {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            let comments = data.get("comments").and_then(|v| v.as_array()).map(|a| a.as_slice()).unwrap_or(&[]);
            total_results = comments.len();
            let now_str = Utc::now().to_rfc3339();
            
            for comment in comments.iter().skip(start_index.saturating_sub(1)).take(max_results) {
                let comment_id = comment.get("commentId").and_then(|v| v.as_str()).unwrap_or("");
                let author = comment.get("author").and_then(|v| v.as_str()).unwrap_or("Unknown");
                let author_id = comment.get("authorId").and_then(|v| v.as_str()).unwrap_or("");
                let content = comment.get("content").and_then(|v| v.as_str()).unwrap_or("");
                
                comments_xml.push_str(&format!(
                    r#"  <entry>
    <id>tag:youtube.com,2008:comment:{video_id}:{cid}</id>
    <published>{now}</published>
    <updated>{now}</updated>
    <title type="text">Comment by {author_esc}</title>
    <content type="text">{content_esc}</content>
    <author>
      <name>{author_esc}</name>
      <uri>http://{host}/feeds/api/users/{author_id_esc}</uri>
    </author>
    <yt:videoid>{video_id}</yt:videoid>
  </entry>
"#,
                    host = host,
                    video_id = video_id,
                    cid = escape_xml(comment_id),
                    now = now_str,
                    author_esc = escape_xml(author),
                    content_esc = escape_xml(content),
                    author_id_esc = escape_xml(author_id),
                ));
            }
        }
    }
    
    let now = Utc::now().to_rfc3339();
    let feed_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://gdata.youtube.com/schemas/2007" xmlns:gd="http://schemas.google.com/g/2005" xmlns:openSearch="http://a9.com/-/spec/opensearch/1.1/">
  <id>http://{host}/feeds/api/videos/{video_id}/comments</id>
  <updated>{now}</updated>
  <title type="text">Comments on Video</title>
  <openSearch:totalResults>{total}</openSearch:totalResults>
  <openSearch:startIndex>{start}</openSearch:startIndex>
  <openSearch:itemsPerPage>{per_page}</openSearch:itemsPerPage>
{comments_xml}</feed>"#,
        host = host,
        video_id = video_id,
        now = now,
        total = total_results,
        start = start_index,
        per_page = max_results,
        comments_xml = comments_xml,
    );
    
    atom_response(feed_xml)
}

pub async fn categories() -> impl IntoResponse {
    info!("categories");
    // Try to load categories.xml from the project root
    let xml = match std::fs::read_to_string("categories.xml") {
        Ok(content) => content,
        Err(_) => {
            // Fallback inline XML
            r#"<?xml version="1.0" encoding="UTF-8"?>
<app:categories xmlns:app="http://www.w3.org/2007/app" xmlns:yt="http://gdata.youtube.com/schemas/2007" xmlns:atom="http://www.w3.org/2005/Atom">
  <atom:category term="Film" label="Film &amp; Animation"/>
  <atom:category term="Autos" label="Autos &amp; Vehicles"/>
  <atom:category term="Music" label="Music"/>
  <atom:category term="Animals" label="Pets &amp; Animals"/>
  <atom:category term="Sports" label="Sports"/>
  <atom:category term="Gaming" label="Gaming"/>
  <atom:category term="People" label="People &amp; Blogs"/>
  <atom:category term="Comedy" label="Comedy"/>
  <atom:category term="Entertainment" label="Entertainment"/>
  <atom:category term="News" label="News &amp; Politics"/>
  <atom:category term="Howto" label="Howto &amp; Style"/>
  <atom:category term="Education" label="Education"/>
  <atom:category term="Tech" label="Science &amp; Technology"/>
</app:categories>"#.to_string()
        }
    };
    atom_response(xml)
}

pub async fn register_device() -> impl IntoResponse {
    let alphabet = b"qwertyuiopasdfghjklzxcvbnm1234567890";
    let mut rng = rand::rngs::StdRng::from_entropy();
    let device_id: String = (0..7).map(|_| {
        let idx = rng.gen_range(0..alphabet.len());
        alphabet[idx] as char
    }).collect();
    
    let body = format!("DeviceId={}\nDeviceKey=ULxlVAAVMhZ2GeqZA/X1GgqEEIP1ibcd3S+42pkWfmk=", device_id);
    info!("Device registered: {}", device_id);
    
    {
        let mut a = ANALYTICS.write().await;
        a.registered_devices.insert(device_id.clone());
    }
    
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
        .body(axum::body::Body::from(body))
        .unwrap()
}

pub async fn device_registration() -> impl IntoResponse {
    let alphabet = b"qwertyuiopasdfghjklzxcvbnm1234567890";
    let mut rng = rand::rngs::StdRng::from_entropy();
    let device_id: String = (0..9).map(|_| {
        let idx = rng.gen_range(0..alphabet.len());
        alphabet[idx] as char
    }).collect();
    
    Json(json!({
        "id": device_id,
        "key": "AP+lc79/lqV58X9FLDdn7SiOzH8hDb1ItXMmm25Cb4YDLWZkI+gXBiwwOvcssAY"
    }))
}

pub async fn channel_fh264_getvideo(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("v") else {
        return (StatusCode::BAD_REQUEST, "Missing v").into_response();
    };
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

pub async fn exp_hd(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("video_id") else {
        return (StatusCode::BAD_REQUEST, "Missing video_id").into_response();
    };
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

pub async fn get_480(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("video_id") else {
        return (StatusCode::BAD_REQUEST, "Missing video_id").into_response();
    };
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

pub async fn get_video(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("video_id") else {
        return (StatusCode::BAD_REQUEST, "Missing video_id").into_response();
    };
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

pub async fn embed_video(Path(video_id): Path<String>) -> impl IntoResponse {
    info!("EMBED_VIDEO /embed/{}", video_id);
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

/// Główna funkcja do pobierania filmów
async fn fetch_videos_unified(
    client: &Client,
    region: &str,
    category_id: Option<&str>,
    query: Option<&str>,
    max_results: i32,
    page_token: Option<&str>,
) -> Result<(Vec<InvidiousVideo>, Option<String>)> {
    // PRIORYTET 1: YouTube API (z cache)
    if let Some(q) = query {
        if !q.is_empty() {
            // Dla wyszukiwania - używamy Invidious (YouTube Search API wymaga płatności)
            let instance = INSTANCE_MANAGER.pick(client).await;
            let videos = search_videos_from_invidious(client, &instance, q).await;
            return Ok((videos, None));
        }
    }

    // PRIORYTET 1: YouTube API dla popularnych/trending
    let api_key = if !crate::CONFIG.youtube_api_key.is_empty() {
        crate::CONFIG.youtube_api_key.clone()
    } else {
        std::env::var("YOUTUBE_API_KEY").unwrap_or_default()
    };

    if !api_key.is_empty() {
        // Sprawdź cache
        let cache_key = ResponseCache::generate_cache_key(
            region,
            category_id,
            page_token,
            max_results
        );
        
        if let Some(cached_data) = RESPONSE_CACHE.get(cache_key).await {
            info!("Unified fetch: using cached YouTube API response");
            let videos: Vec<InvidiousVideo> = cached_data.items
                .iter()
                .map(convert_youtube_to_invidious)
                .collect();
            return Ok((videos, cached_data.next_page_token));
        }

        // Fetch z YouTube API
        match fetch_popular_videos_from_youtube(client, region, category_id, max_results, page_token).await {
            Ok(response) => {
                let videos: Vec<InvidiousVideo> = response.items
                    .iter()
                    .map(convert_youtube_to_invidious)
                    .collect();
                return Ok((videos, response.next_page_token));
            }
            Err(e) => {
                warn!("YouTube API failed, using Invidious fallback: {}", e);
            }
        }
    }

    // PRIORYTET 2: Invidious fallback
    let instance = INSTANCE_MANAGER.pick(client).await;
    
    // Próbuj najpierw trending
    let url = if let Some(cat_id) = category_id {
        format!("{}/api/v1/trending?region={}&category={}", instance, region, cat_id)
    } else {
        format!("{}/api/v1/trending?region={}", instance, region)
    };
    
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(v) = resp.json::<Vec<InvidiousVideo>>().await {
            let videos: Vec<InvidiousVideo> = v.into_iter()
                .filter(|v| !is_live(v))
                .collect();
            if !videos.is_empty() {
                info!("Unified fetch: using Invidious trending ({} videos)", videos.len());
                return Ok((videos, None));
            }
        }
    }

    // Ostateczny fallback: popular
    let url = format!("{}/api/v1/popular?region={}", instance, region);
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(v) = resp.json::<Vec<InvidiousVideo>>().await {
            let videos: Vec<InvidiousVideo> = v.into_iter()
                .filter(|v| !is_live(v))
                .collect();
            if !videos.is_empty() {
                info!("Unified fetch: using Invidious popular ({} videos)", videos.len());
                return Ok((videos, None));
            }
        }
    }

    Err(anyhow::anyhow!("No data source available"))
}

async fn build_mobile_video_objects(
    client: &Client,
    videos: &[InvidiousVideo],
) -> Vec<serde_json::Value> {
    let mut result = Vec::with_capacity(videos.len());
    for video in videos {
        let instance = INSTANCE_MANAGER.pick(client).await;
        result.push(build_mobile_video_object(video, &instance));
    }
    result
}

fn build_mobile_video_object(video: &InvidiousVideo, _instance: &str) -> serde_json::Value {
    let duration_formatted = format_duration(video.length_seconds);
    let view_count_formatted = format_view_count(video.view_count);
        // Stream URL - używamy localnego endpointu
    let stream_url = format!("/get_video?video_id={}/mp4", video.video_id);
    
    // Czas publikacji
    let time_created = if video.published > 0 {
        format_time_ago(Some(video.published), Some(&video.published_text))
    } else if !video.published_text.is_empty() {
        video.published_text.clone()
    } else {
        String::new()
    };
    
    json!({
        "id": video.video_id,
        "video_id": video.video_id,
        "title": video.title,
        "duration": duration_formatted,
        "view_count": view_count_formatted,
        "is_playable": true,
        "username": video.author,
        "watch_link": format!("/watch?v={}", video.video_id),
        "thumbnail_for_list": format!("<img src=\"http://i.ytimg.com/vi/{}/default.jpg\"/>", video.video_id),
        "stream_url": stream_url,
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
        "time_created_text": time_created
    })
}

/// Wersja dla wyników wyszukiwania z instancją
fn blzr_from_video_with_instance(video: InvidiousVideo, instance: &str) -> serde_json::Value {
    build_mobile_video_object(&video, instance)
}

pub async fn mobile_blzr_home(Query(_params): Query<HashMap<String, String>>) -> Response {
    let t0 = Instant::now();
    let client = &*HTTP_CLIENT;

    // UŻYJ ZUNIFIKOWANEGO ŹRÓDŁA DANYCH
    let (videos, _next_token) = match fetch_videos_unified(
        client,
        "US",        // region
        None,        // brak kategorii
        None,        // brak query
        18,          // max_results
        None,        // page_token
    ).await {
        Ok(result) => result,
        Err(e) => {
            warn!("Failed to fetch unified videos for home: {}", e);
            return (StatusCode::SERVICE_UNAVAILABLE, "Failed to fetch videos").into_response();
        }
    };

    // Buduj obiekty mobile - tutaj używamy async
    let mobile_videos = build_mobile_video_objects(client, &videos).await;
    
    // Ogranicz do 18
    let mobile_videos: Vec<serde_json::Value> = mobile_videos.into_iter().take(18).collect();

    let response = serde_json::json!({
        "result": "ok",
        "content": {
            "home_videos": mobile_videos
        }
    });

    info!("mobile_blzr_home: {} items, {}ms", 
        response["content"]["home_videos"].as_array().map(|a| a.len()).unwrap_or(0), 
        t0.elapsed().as_millis()
    );
    Json(response).into_response()
}

pub async fn mobile_blzr_results(Query(params): Query<SearchParams>) -> Response {
    info!("mobile_blzr_results");
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;

    let videos = if let Some(q) = params.q.as_deref() {
        if q.is_empty() {
            return (StatusCode::BAD_REQUEST, "Missing query").into_response();
        }
        
        // UŻYJ ZUNIFIKOWANEGO ŹRÓDŁA DANYCH
        let (items, _) = match fetch_videos_unified(
            client,
            "US",
            None,
            Some(q),
            20,
            None,
        ).await {
            Ok(result) => result,
            Err(e) => {
                warn!("Search failed: {}", e);
                return (StatusCode::SERVICE_UNAVAILABLE, "Search failed").into_response();
            }
        };
        
        // Używamy poprawnego formatu
        items.into_iter()
            .map(|video| build_mobile_video_object(&video, &instance))
            .collect::<Vec<_>>()
        
    } else if let Some(category_name) = params.category.as_deref() {
        if category_name.is_empty() {
            return (StatusCode::BAD_REQUEST, "Missing category").into_response();
        }
        
        let region_code = params.region.as_deref().unwrap_or("US");
        let category_id = CATEGORY_NAME_TO_ID
            .get(category_name)
            .cloned()
            .or_else(|| {
                CATEGORY_NAME_TO_ID
                    .iter()
                    .find_map(|(name, id)| if name.eq_ignore_ascii_case(category_name) { Some(id.clone()) } else { None })
            });
        
        let category_id = match category_id {
            Some(id) => id,
            None => return (StatusCode::BAD_REQUEST, "Unknown category").into_response(),
        };
        
        let (items, _) = match fetch_videos_unified(
            client,
            region_code,
            Some(&category_id),
            None,
            params.max_results.unwrap_or(20).min(50) as i32,
            None,
        ).await {
            Ok(result) => result,
            Err(e) => {
                warn!("Category fetch failed: {}", e);
                return (StatusCode::SERVICE_UNAVAILABLE, "Failed to fetch category").into_response();
            }
        };
        
        items.into_iter()
            .map(|video| build_mobile_video_object(&video, &instance))
            .collect::<Vec<_>>()
        
    } else {
        return (StatusCode::BAD_REQUEST, "Missing query or category").into_response();
    };

    Json(json!({
        "result": "ok",
        "content": {
            "videos": videos,
            "results_count": videos.len().to_string()
        }
    })).into_response()
}

pub async fn mobile_blzr_watch(Query(params): Query<HashMap<String, String>>) -> Response {
    info!("mobile_blzr_watch");
    let video_id = params.get("videoId")
        .or_else(|| params.get("id"))
        .or_else(|| params.get("v"));

    let Some(video_id) = video_id else {
        return (StatusCode::BAD_REQUEST, "Missing videoId").into_response();
    };

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    
    let url = format!("{}/api/v1/videos/{}", instance, video_id);
    
    let data = if let Ok(resp) = client.get(&url).send().await {
        resp.json::<serde_json::Value>().await.ok()
    } else {
        let fallback_instances = INSTANCE_MANAGER.all_instances(client).await;
        let mut result = None;
        for inst in fallback_instances {
            if inst == instance { continue; }
            let url = format!("{}/api/v1/videos/{}", inst, video_id);
            if let Ok(resp) = client.get(&url).send().await {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    result = Some(json);
                    break;
                }
            }
        }
        result
    };

    if let Some(data) = data {
        let result = blzr_watch_response(&instance, video_id, &data);
        Json(json!({
            "result": "ok",
            "content": result
        })).into_response()
    } else {
        (StatusCode::NOT_FOUND, "Video not found").into_response()
    }
}

fn blzr_watch_response(instance: &str, video_id: &str, data: &serde_json::Value) -> serde_json::Value {
    let host = &crate::CONFIG.server_host;
    // Podstawowe dane z video
    let title = data.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let author = data.get("author").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let _author_id = data.get("authorId").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let duration_seconds = data.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
    let view_count = data.get("viewCount").and_then(|v| v.as_i64()).unwrap_or_else(|| {
        data.get("viewCountText")
            .and_then(|v| v.as_str())
            .map(parse_view_count_text)
            .unwrap_or(0)
    });
    let like_count = data.get("likeCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let dislike_count = data.get("dislikeCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let comment_count = data.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let description = data.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let published_ts = parse_invidious_timestamp(data.get("published")).unwrap_or(0);
    let published_text = data.get("publishedText").and_then(|v| v.as_str()).unwrap_or("").to_string();
    
    // Kategoria i tagi
    let category_name = data.get("category")
        .and_then(|v| v.as_str())
        .unwrap_or("People & Blogs")
        .to_string();
    
    let tags: Vec<serde_json::Value> = data.get("keywords")
        .and_then(|v| v.as_array())
        .or_else(|| data.get("tags").and_then(|v| v.as_array()))
        .map(|arr| {
            arr.iter()
                .filter_map(|item| item.as_str())
                .map(|keyword| {
                    json!({
                        "url": format!("/results?search_query={}", urlencoding::encode(keyword)),
                        "keyword": keyword
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    
    // Avatar użytkownika
    let user_image_url = data.get("authorThumbnails")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.last())
        .and_then(|thumb| thumb.get("url").and_then(|u| u.as_str()))
        .unwrap_or("/assets/yVXKYrUI8hckCQdyUuOWf5ZJk2keT8WO3TV2b8RYk3RKgjz5Rh8v1UsH7Yz2j_hbDQRk32rZ_rM=s88-c-k-c0x00ffffff-no-rj.png")
        .to_string();
    
    // Formatowanie danych
    let duration_formatted = format_duration(duration_seconds as i32);
    let view_count_formatted = format_view_count(view_count);
    let stream_url = format!("http://{}/get_video?video_id={}", host, video_id);
    let hq_stream_url = format!("http://{}/get_hd_video?id={}", host, video_id);
    
    // Czas utworzenia
    let time_created = if published_ts > 0 {
        chrono::DateTime::from_timestamp(published_ts, 0)
            .map(|dt| dt.format("%b %e, %Y").to_string())
            .unwrap_or_else(|| published_text.clone())
    } else if !published_text.is_empty() {
        published_text.clone()
    } else {
        "".to_string()
    };
    
    // Short description (pierwsze 200 znaków)
    let short_description = if description.len() > 200 {
        format!("{}...", &description[..200])
    } else {
        description.clone()
    };
    
    // Budowanie obiektu video
    let video_obj = json!({
        "stitched_thumbnail_large": {
            "url": format!("http://i.ytimg.com/vi/{}/hqdefault.jpg", video_id),
            "width": 160,
            "height": 120,
            "posx": 60,
            "posy": 25
        },
        "id": video_id,
        "video_id": video_id,
        "stream_url": stream_url,
        "is_playable": true,
        "title": title,
        "username": author,
        "category_name": category_name,
        "tags": tags,
        "full_description": description,
        "short_description": short_description,
        "time_created_text": time_created,
        "view_count": view_count_formatted,
        "duration": duration_formatted,
        "user_image_url": user_image_url,
        "hq_stream_url": hq_stream_url,
        "like_count": like_count,
        "dislike_count": dislike_count,
        "comment_count": comment_count,
        "length": duration_seconds,
        "thumbnail": format!("http://i.ytimg.com/vi/{}/hqdefault.jpg", video_id),
        "watch_url": format!("http://2009frontend.truehosting.net/watch?id={}", video_id),
        "embed_url": format!("{}/embed/{}?raw=1", instance.trim_end_matches('/'), video_id),
    });
    
    // Pobieranie powiązanych filmów
    let related_videos: Vec<serde_json::Value> = data.get("recommendedVideos")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let vid = item.get("videoId").and_then(|v| v.as_str())?;
                    let title = item.get("title").and_then(|v| v.as_str())?;
                    let author = item.get("author").and_then(|v| v.as_str()).unwrap_or("");
                    let length_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
                    let view_count = item.get("viewCount").and_then(|v| v.as_i64())
                        .or_else(|| item.get("viewCountText").and_then(|v| v.as_str()).map(parse_view_count_text))
                        .unwrap_or(0);
                    
                    Some(json!({
                        "id": vid,
                        "video_id": vid,
                        "title": title.to_string(),
                        "duration": format_duration(length_seconds as i32),
                        "view_count": format_view_count(view_count),
                        "is_playable": true,
                        "username": author.to_string(),
                        "watch_link": format!("/watch?v={}", vid),
                        "thumbnail_for_list": format!("<img src=\"http://i.ytimg.com/vi/{}/default.jpg\"/>", vid),
                        "stream_url": format!("/get_video?video_id={}/mp4", vid),
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
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    
    // Końcowa odpowiedź
    json!({
        "video": video_obj,
        "related_videos": related_videos
    })
}

fn blzr_comment(comment: &serde_json::Value) -> serde_json::Value {
    // Pobieramy autora
    let author_name = comment.get("author")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown")
        .to_string();
    
    // Pobieramy treść komentarza - uproszczona wersja bez problemów z typami
    let content = if let Some(text) = comment.get("content").and_then(|v| v.as_str()) {
        text.to_string()
    } else if let Some(runs) = comment.get("content").and_then(|v| v.get("runs")).and_then(|r| r.as_array()) {
        runs.iter()
            .filter_map(|run| run.get("text").and_then(|t| t.as_str()))
            .collect::<String>()
    } else if let Some(text) = comment.get("content").and_then(|v| v.get("simpleText")).and_then(|v| v.as_str()) {
        text.to_string()
    } else {
        String::new()
    };
    
    // Pobieramy czas publikacji - uproszczona wersja
    let time_ago = if let Some(text) = comment.get("publishedText").and_then(|v| v.as_str()) {
        text.to_string()
    } else if let Some(ts) = comment.get("published").and_then(|v| v.as_i64()) {
        format_time_ago_comment(ts)
    } else {
        String::new()
    };
    
    // Sprawdź czy komentarz jest edytowany
    let is_edited = comment.get("isEdited")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    
    // Dodaj "(edited)" jeśli edytowany
    let time_ago = if is_edited && !time_ago.is_empty() {
        format!("{} (edited)", time_ago)
    } else {
        time_ago
    };
    
    json!({
        "author_name": author_name,
        "comment": content,
        "time_ago": time_ago
    })
}

pub async fn mobile_blzr_view_comment(Query(params): Query<HashMap<String, String>>) -> Response {
    info!("mobile_blzr_view_comment");
    let video_id = params.get("videoId")
        .or_else(|| params.get("id"))
        .or_else(|| params.get("v"));

    let Some(video_id) = video_id else {
        return (StatusCode::BAD_REQUEST, "Missing videoId").into_response();
    };

    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    let url = format!("{}/api/v1/comments/{}", instance, video_id);

    let comments = if let Ok(resp) = client.get(&url).send().await {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            data.get("comments")
                .and_then(|v| v.as_array())
                .map(|items| items.iter().map(blzr_comment).collect::<Vec<_>>())
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    Json(json!({
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

// ============================================================
// VIDEO FORMAT PARSING
// ============================================================

fn extract_codecs(mime_type: &str) -> String {
    if let Some(start) = mime_type.find("codecs=\"") {
        let start = start + 8;
        if let Some(end) = mime_type[start..].find('"') {
            return mime_type[start..start + end].to_string();
        }
    }
    "avc1.640028".to_string()
}

async fn parse_video_formats(video_id: &str) -> Result<(StreamInfo, StreamInfo)> {
    let client = &*HTTP_CLIENT;
    
    // Używamy INSTANCE_MANAGER do pobrania instancji
    let instance = INSTANCE_MANAGER.pick(client).await;
    let url = format!("{}/api/v1/videos/{}", instance, video_id);
    
    info!("Fetching video formats from: {}", instance);
    
    let response = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0")
        .header("Accept", "application/json")
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to fetch video info: {}", response.status()));
    }
    
    let data: serde_json::Value = response.json().await?;
    
    // Debug - sprawdzamy czy są formaty
    if let Some(adaptive) = data.get("adaptiveFormats") {
        info!("Found adaptiveFormats: {}", 
            adaptive.as_array().map(|a| a.len()).unwrap_or(0));
    } else {
        warn!("No adaptiveFormats found in response");
        return Err(anyhow::anyhow!("No adaptiveFormats in response"));
    }
    
    let video_info = find_best_video_format(&data)?;
    let audio_info = find_best_audio_format(&data)?;
    
    info!("Video: itag={}, quality={}", video_info.itag, video_info.quality);
    info!("Audio: itag={}, quality={}", audio_info.itag, audio_info.quality);
    
    Ok((video_info, audio_info))
}

fn find_best_audio_format(data: &serde_json::Value) -> Result<StreamInfo> {
    // Szukamy adaptiveFormats
    let adaptive_formats = data
        .get("adaptiveFormats")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("No adaptiveFormats found in response"))?;
    
    if adaptive_formats.is_empty() {
        return Err(anyhow::anyhow!("adaptiveFormats array is empty"));
    }
    
    let mut best_format = None;
    let mut best_score = 0u64;
    
    for format in adaptive_formats {
        // Pobieramy mimeType - używamy obu wariantów nazwy
        let mime = format
            .get("mimeType")
            .or_else(|| format.get("mimeType"))
            .or_else(|| format.get("type"))
            .and_then(|v| v.as_str());
        
        let mime = match mime {
            Some(m) if m.contains("audio/") => m,
            _ => continue,
        };
        
        // Pobieramy URL
        let url = match format.get("url").and_then(|v| v.as_str()) {
            Some(u) => u.to_string(),
            None => continue,
        };
        
        // Pobieramy bitrate - może być string lub number
        let bitrate = format
            .get("bitrate")
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    s.parse::<u64>().ok()
                } else if let Some(n) = v.as_u64() {
                    Some(n)
                } else {
                    None
                }
            })
            .unwrap_or(0);
        
        // Pobieramy itag
        let itag = format
            .get("itag")
            .and_then(|v| {
                if let Some(n) = v.as_u64() {
                    Some(n as u32)
                } else if let Some(s) = v.as_str() {
                    s.parse::<u32>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        
        // Pobieramy długość
        let content_length = format
            .get("clen")
            .or_else(|| format.get("contentLength"))
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    s.parse::<u64>().ok()
                } else if let Some(n) = v.as_u64() {
                    Some(n)
                } else {
                    None
                }
            });
        
        // Obliczamy score dla formatu audio
        // Preferujemy: AAC/MP4 > Opus > inne
        let quality_score = if mime.contains("mp4a") || mime.contains("aac") || mime.contains("mp4") {
            1000 // AAC/MP4 - najlepsza jakość
        } else if mime.contains("opus") {
            800 // Opus - dobra jakość
        } else if mime.contains("webm") {
            600 // WebM audio - średnia
        } else {
            400 // Inne - najgorsza
        };
        
        // Dodajemy bitrate do score (max 1000)
        let bitrate_score = (bitrate / 1000).min(1000) as u64;
        
        // Dodajemy kanały audio (stereo lepsze niż mono)
        let channels = format
            .get("audioChannels")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let channels_score = (channels * 50).min(100);
        
        // Łączny score
        let score = quality_score + bitrate_score + channels_score;
        
        // Dodatkowo preferujemy wyższą częstotliwość próbkowania
        if let Some(sample_rate) = format.get("audioSampleRate").and_then(|v| v.as_u64()) {
            if sample_rate >= 48000 {
                // High quality audio
                let bonus = if score < 2000 { 200 } else { 100 };
                let score = score + bonus;
                // Używamy score bez przypisania do zmiennej, tylko do porównania
                let final_score = score;
                if final_score > best_score {
                    best_score = final_score;
                    best_format = Some(StreamInfo {
                        url,
                        mime_type: mime.to_string(),
                        itag,
                        content_length,
                        codecs: extract_codecs(mime),
                        quality: if mime.contains("mp4a") || mime.contains("aac") || mime.contains("mp4") {
                            "high".to_string()
                        } else if mime.contains("opus") {
                            "medium".to_string()
                        } else {
                            "low".to_string()
                        },
                    });
                }
                continue;
            }
        }
        
        if score > best_score {
            best_score = score;
            best_format = Some(StreamInfo {
                url,
                mime_type: mime.to_string(),
                itag,
                content_length,
                codecs: extract_codecs(mime),
                quality: if mime.contains("mp4a") || mime.contains("aac") || mime.contains("mp4") {
                    "high".to_string()
                } else if mime.contains("opus") {
                    "medium".to_string()
                } else {
                    "low".to_string()
                },
            });
        }
    }
    
    best_format.ok_or_else(|| {
        // Debug - wypisz dostępne formaty
        let available: Vec<String> = adaptive_formats
            .iter()
            .filter_map(|f| f.get("mimeType").or_else(|| f.get("type")).and_then(|v| v.as_str()))
            .map(|s| s.to_string())
            .collect();
        warn!("Available formats: {:?}", available);
        anyhow::anyhow!("No audio format found")
    })
}

fn find_best_video_format(data: &serde_json::Value) -> Result<StreamInfo> {
    let adaptive_formats = data
        .get("adaptiveFormats")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("No adaptiveFormats found"))?;
    
    let priorities = vec!["1080p", "720p", "480p", "360p", "240p", "144p"];
    
    for priority in priorities {
        for format in adaptive_formats {
            // Sprawdzamy czy to video
            let mime = format
                .get("mimeType")
                .or_else(|| format.get("type"))
                .and_then(|v| v.as_str());
            
            if let Some(m) = mime {
                if !m.contains("video/") {
                    continue;
                }
            } else {
                continue;
            }
            
            let quality_label = format
                .get("qualityLabel")
                .or_else(|| format.get("qualityLabel"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            
            if quality_label.contains(priority) {
                let url = format
                    .get("url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("No URL"))?
                    .to_string();
                
                let mime_type = mime.unwrap_or("video/mp4").to_string();
                
                let itag = format
                    .get("itag")
                    .and_then(|v| {
                        if let Some(n) = v.as_u64() {
                            Some(n as u32)
                        } else if let Some(s) = v.as_str() {
                            s.parse::<u32>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                
                return Ok(StreamInfo {
                    url,
                    mime_type: mime_type.clone(),
                    itag,
                    content_length: format.get("clen").and_then(|v| {
                        if let Some(s) = v.as_str() {
                            s.parse::<u64>().ok()
                        } else if let Some(n) = v.as_u64() {
                            Some(n)
                        } else {
                            None
                        }
                    }),
                    codecs: extract_codecs(&mime_type),
                    quality: priority.to_string(),
                });
            }
        }
    }
    
    // Fallback - weź pierwszy video format
    for format in adaptive_formats {
        let mime = format
            .get("mimeType")
            .or_else(|| format.get("type"))
            .and_then(|v| v.as_str());
        
        if let Some(m) = mime {
            if m.contains("video/") {
                let url = format
                    .get("url")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("No URL"))?
                    .to_string();
                
                return Ok(StreamInfo {
                    url,
                    mime_type: m.to_string(),
                    itag: format.get("itag").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                    content_length: format.get("clen").and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok()),
                    codecs: extract_codecs(m),
                    quality: "unknown".to_string(),
                });
            }
        }
    }
    
    Err(anyhow::anyhow!("No video format found"))
}

// ============================================================
// GET HD VIDEO - Z OBSŁUGĄ FFMPEG
// ============================================================

pub async fn get_hd_video(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let Some(video_id) = params.get("id") else {
        return (StatusCode::BAD_REQUEST, "Missing id").into_response();
    };
    
    info!("GET_HD_VIDEO: id={}", video_id);
    
    // Pobieramy informacje o formatach
    let (video_info, audio_info) = match parse_video_formats(video_id).await {
        Ok(formats) => formats,
        Err(e) => {
            warn!("Failed to parse video formats: {}", e);
            return (StatusCode::NOT_FOUND, "Video not found").into_response();
        }
    };
    
    info!("Selected video: itag={}, quality={}", video_info.itag, video_info.quality);
    info!("Selected audio: itag={}, quality={}", audio_info.itag, audio_info.quality);
    
    // Sprawdzamy czy FFmpeg jest dostępny
    let ffmpeg_check = tokio::process::Command::new("ffmpeg")
        .arg("-version")
        .output()
        .await;
    
    if ffmpeg_check.is_ok() {
        // Używamy FFmpeg do muxowania
        match stream_muxed_with_ffmpeg(video_info, audio_info).await {
            Ok(body) => {
                let mut response = body.into_response();
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    "video/mp4".parse().unwrap(),
                );
                response.headers_mut().insert(
                    header::CONTENT_DISPOSITION,
                    format!("inline; filename=\"{}.mp4\"", video_id).parse().unwrap(),
                );
                return response;
            }
            Err(e) => {
                warn!("FFmpeg muxing failed: {}", e);
            }
        }
    } else {
        warn!("FFmpeg not found, using fallback");
    }
    
    // Fallback - przekieruj do embed
    let client = &*HTTP_CLIENT;
    let instance = INSTANCE_MANAGER.pick(client).await;
    axum::response::Redirect::temporary(&format!("{}/embed/{}?raw=1", instance, video_id)).into_response()
}

async fn stream_muxed_with_ffmpeg(
    video_info: StreamInfo,
    audio_info: StreamInfo,
) -> Result<axum::body::Body> {
    use tokio::process::Command;
    use tokio_util::io::ReaderStream;
    
    // Uruchamiamy FFmpeg do strumieniowego muxowania
    let mut child = Command::new("ffmpeg")
        .arg("-i")
        .arg(&video_info.url)
        .arg("-i")
        .arg(&audio_info.url)
        .arg("-c:v")
        .arg("copy")
        .arg("-c:a")
        .arg("aac")
        .arg("-b:a")
        .arg("192k")
        .arg("-movflags")
        .arg("frag_keyframe+empty_moov")
        .arg("-f")
        .arg("mp4")
        .arg("-")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| anyhow::anyhow!("Failed to start FFmpeg: {}", e))?;
    
    let stdout = child.stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to get FFmpeg stdout"))?;
    
    // Tworzymy strumień z stdout FFmpeg
    let stream = ReaderStream::new(stdout);
    let body = axum::body::Body::from_stream(stream);
    
    // W tle czekamy na zakończenie procesu
    tokio::spawn(async move {
        let status = child.wait().await;
        if let Ok(status) = status {
            if !status.success() {
                warn!("FFmpeg process failed with status: {}", status);
            }
        }
    });
    
    Ok(body)
}

// ============================================================
// CACHE MANAGEMENT ENDPOINTS
// ============================================================

pub async fn cache_stats() -> impl IntoResponse {
    let (hits, misses, size) = RESPONSE_CACHE.get_stats().await;
    let total = hits + misses;
    let hit_rate = if total > 0 {
        (hits as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    
    Json(json!({
        "cache_hits": hits,
        "cache_misses": misses,
        "total_requests": total,
        "hit_rate": format!("{:.2}%", hit_rate),
        "cache_size": size,
        "ttl_seconds": CACHE_TTL_SECONDS,
        "ttl_hours": CACHE_TTL_SECONDS / 3600
    }))
}

pub async fn clear_cache() -> impl IntoResponse {
    RESPONSE_CACHE.clear().await;
    Json(json!({
        "status": "success",
        "message": "Cache cleared successfully"
    }))
}

// ============================================================
// YOUTUBE -> INVIDIOUS CONVERSION
// ============================================================

fn convert_youtube_to_invidious(item: &YouTubeVideoItem) -> InvidiousVideo {
    let view_count = item.statistics.view_count.as_ref()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    
    let like_count = item.statistics.like_count.as_ref()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    
    let comment_count = item.statistics.comment_count.as_ref()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    
    let published = chrono::DateTime::parse_from_rfc3339(&item.snippet.published_at)
        .map(|dt| dt.timestamp())
        .unwrap_or(0);

    let length_seconds = parse_iso8601_duration(&item.content_details.duration).unwrap_or(0) as i32;
    
    InvidiousVideo {
        video_id: item.id.clone(),
        title: item.snippet.title.clone(),
        author: item.snippet.channel_title.clone(),
        author_id: item.snippet.channel_id.clone(),
        length_seconds,
        view_count,
        published_text: String::new(),
        published,
        uploaded_text: String::new(),
        live_now: Some(false),
        likes: like_count,
        dislikes: 0,
        comment_count,
    }
}

fn parse_iso8601_duration(duration: &str) -> Option<i64> {
    if !duration.starts_with('P') {
        return None;
    }

    let mut total = 0i64;
    let mut num = String::new();
    let mut in_time = false;

    for c in duration.chars().skip(1) {
        match c {
            'T' => {
                in_time = true;
            }
            'H' => {
                if let Ok(v) = num.parse::<i64>() {
                    total += v * 3600;
                }
                num.clear();
            }
            'M' => {
                if let Ok(v) = num.parse::<i64>() {
                    if in_time {
                        total += v * 60;
                    } else {
                        total += v * 86400 * 30; // months approximate
                    }
                }
                num.clear();
            }
            'S' => {
                if let Ok(v) = num.parse::<i64>() {
                    total += v;
                }
                num.clear();
            }
            'D' => {
                if let Ok(v) = num.parse::<i64>() {
                    total += v * 86400;
                }
                num.clear();
            }
            'W' => {
                if let Ok(v) = num.parse::<i64>() {
                    total += v * 7 * 86400;
                }
                num.clear();
            }
            'Y' => {
                if let Ok(v) = num.parse::<i64>() {
                    total += v * 365 * 86400;
                }
                num.clear();
            }
            c if c.is_ascii_digit() => {
                num.push(c);
            }
            _ => {
                num.clear();
            }
        }
    }

    Some(total)
}

// ============================================================
// PAGINATION PARAMS
// ============================================================

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
    category: Option<String>,
    region: Option<String>,
    #[serde(rename = "start-index")]
    start_index: Option<usize>,
    #[serde(rename = "max-results")]
    max_results: Option<usize>,
    #[serde(flatten)]
    _extra: std::collections::HashMap<String, String>,
}

// ============================================================
// HELPER FUNCTIONS
// ============================================================

fn format_view_count(view_count: i64) -> String {
    let mut s = view_count.abs().to_string();
    let mut out = String::new();
    while s.len() > 3 {
        let chunk = s.split_off(s.len() - 3);
        out = format!("{},{}", chunk, out.trim_start_matches(','));
    }
    if !s.is_empty() {
        if out.is_empty() {
            out = s;
        } else {
            out = format!("{},{}", s, out);
        }
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

fn is_live(v: &InvidiousVideo) -> bool {
    if v.length_seconds == 0 {
        return true;
    }
    if v.live_now == Some(true) {
        return true;
    }
    false
}

fn seconds_to_time(seconds: i64) -> String {
    let s = seconds.max(0);
    format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
}

fn extract_published_date(item: &serde_json::Value) -> (i64, String) {
    if let Some(ts) = item.get("published").and_then(|v| v.as_i64()) {
        if ts > 1000000000 {
            return (ts, String::new());
        }
    }

    if let Some(text) = item.get("publishedText").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            if let Some(timestamp) = parse_relative_date(text) {
                return (timestamp, text.to_string());
            }
            return (0, text.to_string());
        }
    }

    if let Some(text) = item.get("uploadedText").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            if let Some(timestamp) = parse_relative_date(text) {
                return (timestamp, text.to_string());
            }
            return (0, text.to_string());
        }
    }

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

fn parse_invidious_timestamp(value: Option<&serde_json::Value>) -> Option<i64> {
    if let Some(v) = value {
        if let Some(ts) = v.as_i64() {
            return Some(ts);
        }
        if let Some(text) = v.as_str() {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(text) {
                return Some(dt.timestamp());
            }
        }
    }
    None
}

fn parse_view_count_text(text: &str) -> i64 {
    let normalized = text.trim().to_lowercase().replace([',', ' '], "");
    if normalized.is_empty() {
        return 0;
    }
    let multiplier = if normalized.ends_with('m') {
        1_000_000
    } else if normalized.ends_with('k') {
        1_000
    } else {
        1
    };
    let digits = if multiplier == 1 {
        normalized.as_str()
    } else {
        &normalized[..normalized.len() - 1]
    };
    digits.parse::<f64>().map(|value| (value * multiplier as f64) as i64).unwrap_or(0)
}

fn blzr_from_invidious_item(item: &serde_json::Value) -> Option<serde_json::Value> {
    let video_id = item.get("videoId").and_then(|v| v.as_str())?;
    let title = item.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let author = item.get("author").and_then(|v| v.as_str()).unwrap_or("");
    let author_id = item.get("authorId").and_then(|v| v.as_str()).unwrap_or("");
    let duration_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0);
    let view_count = item.get("viewCount").and_then(|v| v.as_i64()).unwrap_or_else(|| {
        item.get("viewCountText")
            .and_then(|v| v.as_str())
            .map(parse_view_count_text)
            .unwrap_or(0)
    });
    let likes = item.get("likeCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let dislikes = item.get("dislikeCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let comment_count = item.get("commentCount").and_then(|v| v.as_i64()).unwrap_or(0);
    let published = parse_invidious_timestamp(item.get("published")).unwrap_or(0);
    let published_text = item.get("publishedText").and_then(|v| v.as_str()).unwrap_or("").to_string();

    Some(json!({
        "videoId": video_id,
        "title": title,
        "author": author,
        "authorId": author_id,
        "durationSeconds": duration_seconds,
        "viewCount": view_count,
        "likeCount": likes,
        "dislikeCount": dislikes,
        "commentCount": comment_count,
        "published": if published > 0 { timestamp_to_rfc3339(published) } else { published_text },
        "thumbnail": format!("http://i.ytimg.com/vi/{}/hqdefault.jpg", video_id),
        "watchUrl": format!("http://2009frontend.truehosting.net/watch?id={}", video_id)
    }))
}

fn format_time_ago_comment(timestamp: i64) -> String {
    let now = Utc::now().timestamp();
    let diff = now.saturating_sub(timestamp);
    
    if diff < 60 {
        return "Just now".to_string();
    } else if diff < 3600 {
        let minutes = diff / 60;
        return format!("{} {} ago", minutes, if minutes == 1 { "minute" } else { "minutes" });
    } else if diff < 86400 {
        let hours = diff / 3600;
        return format!("{} {} ago", hours, if hours == 1 { "hour" } else { "hours" });
    } else if diff < 604800 {
        let days = diff / 86400;
        return format!("{} {} ago", days, if days == 1 { "day" } else { "days" });
    } else if diff < 2592000 {
        let weeks = diff / 604800;
        return format!("{} {} ago", weeks, if weeks == 1 { "week" } else { "weeks" });
    } else if diff < 31536000 {
        let months = diff / 2592000;
        return format!("{} {} ago", months, if months == 1 { "month" } else { "months" });
    } else {
        let years = diff / 31536000;
        return format!("{} {} ago", years, if years == 1 { "year" } else { "years" });
    }
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

fn escape_xml(s: &str) -> String {
    let cleaned = strip_problematic_chars(s);
    let mut out = String::with_capacity(cleaned.len() + 16);
    for c in cleaned.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

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
            true
        })
        .collect()
}

fn instance_debug_hint(instance: &str) -> String {
    let base = base_instance_url(instance);
    format!("video description is currently not supported debug info: using invidious instance: {}", base)
}

fn base_instance_url(instance: &str) -> String {
    let trimmed = instance.trim_end_matches('/');
    if let Some(scheme_end) = trimmed.find("://") {
        let after_scheme = &trimmed[scheme_end + 3..];
        if let Some(last_slash) = after_scheme.rfind('/') {
            return trimmed[..scheme_end + 3 + last_slash].to_string();
        }
    }
    trimmed.to_string()
}

fn atom_response(xml: String) -> axum::response::Response {
    let mut resp = xml.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/atom+xml; charset=UTF-8".parse().unwrap(),
    );
    resp
}

async fn fetch_invidious_json(client: &Client, path: &str) -> Option<(serde_json::Value, String)> {
    let instances = INSTANCE_MANAGER.all_instances(client).await;
    for instance in instances {
        let url = format!("{}/api/v1{}", instance.trim_end_matches('/'), path);
        match client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        return Some((json, instance.clone()));
                    }
                }
            }
            Err(_) => continue,
        }
    }
    None
}

async fn search_videos_from_invidious(client: &Client, instance: &str, query: &str) -> Vec<InvidiousVideo> {
    let url = format!("{}/api/v1/search?q={}&type=video", instance, urlencoding::encode(query));
    let mut entries: Vec<InvidiousVideo> = Vec::new();
    
    if let Ok(resp) = client.get(&url).send().await {
        if let Ok(data) = resp.json::<serde_json::Value>().await {
            if let Some(items) = data.as_array() {
                for item in items {
                    let type_str = item.get("type").and_then(|v| v.as_str()).unwrap_or("video");
                    if type_str != "video" { continue; }
                    
                    if let (Some(video_id), Some(title)) = (
                        item.get("videoId").and_then(|v| v.as_str()),
                        item.get("title").and_then(|v| v.as_str()),
                    ) {
                        let length_seconds = item.get("lengthSeconds").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                        if length_seconds == 0 { continue; }
                        
                        entries.push(InvidiousVideo {
                            video_id: video_id.to_string(),
                            title: title.to_string(),
                            author: item.get("author").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            author_id: item.get("authorId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            length_seconds,
                            view_count: item.get("viewCount").and_then(|v| v.as_i64()).unwrap_or(0),
                            published_text: String::new(),
                            published: item.get("published").and_then(|v| v.as_i64()).unwrap_or(0),
                            uploaded_text: String::new(),
                            live_now: Some(false),
                            likes: 0,
                            dislikes: 0,
                            comment_count: 0,
                        });
                    }
                }
            }
        }
    }
    entries
}

// ============================================================
// BUILD FEED XML
// ============================================================

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

    xml.push_str("<?xml version='1.0' encoding='UTF-8'?>
");
    xml.push_str("<feed xmlns='http://www.w3.org/2005/Atom'
");
    xml.push_str("      xmlns:openSearch='http://a9.com/-/spec/opensearch/1.1/'
");
    xml.push_str("      xmlns:media='http://search.yahoo.com/mrss/'
");
    xml.push_str("      xmlns:gd='http://schemas.google.com/g/2005'
");
    xml.push_str("      xmlns:yt='http://gdata.youtube.com/schemas/2007'>
");

    xml.push_str("  <id>http://");
    xml.push_str(host);
    xml.push_str("/feeds/api/");
    xml.push_str(feed_id);
    xml.push_str("</id>
");

    xml.push_str("  <category scheme='http://schemas.google.com/g/2005#kind' term='http://gdata.youtube.com/schemas/2007#video'/>
");
    xml.push_str("  <title type='text'>");
    xml.push_str(&escape_xml(title));
    xml.push_str("</title>
");
    xml.push_str("  <logo>http://www.youtube.com/img/pic_youtubelogo_123x63.gif</logo>
");
    xml.push_str("  <link rel='alternate' type='text/html' href='http://www.youtube.com'/>");
    xml.push_str("
");

    xml.push_str("  <link rel='http://schemas.google.com/g/2005#feed' type='application/atom+xml' href='http://");
    xml.push_str(host);
    xml.push_str("/feeds/api/videos'/>");
    xml.push_str("
");

    xml.push_str("  <link rel='http://schemas.google.com/g/2005#batch' type='application/atom+xml' href='http://");
    xml.push_str(host);
    xml.push_str("/feeds/api/videos/batch'/>");
    xml.push_str("
");

    xml.push_str("  <author><name>YouTube/yt2009</name><uri>http://www.youtube.com/</uri></author>
");

    xml.push_str("  <generator version='2.0' uri='http://");
    xml.push_str(host);
    xml.push_str("/'>YouTube data API</generator>
");

    xml.push_str("  <openSearch:totalResults>");
    xml.push_str(&videos.len().to_string());
    xml.push_str("</openSearch:totalResults>
");
    xml.push_str("  <openSearch:startIndex>");
    xml.push_str(&start.to_string());
    xml.push_str("</openSearch:startIndex>
");
    xml.push_str("  <openSearch:itemsPerPage>");
    xml.push_str(&limit.to_string());
    xml.push_str("</openSearch:itemsPerPage>
");

    if let Some(ref next) = next_link {
        xml.push_str("  <link rel='next' type='application/atom+xml' href='");
        xml.push_str(&escape_xml(next));
        xml.push_str("'/>
");
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
        let author_id_esc = escape_xml(author_id);

        let view_count = video.view_count;
        let like_count = if video.likes > 0 { video.likes } else { (view_count as f64 * 0.06) as i64 };
        let dislike_count = if video.dislikes > 0 { video.dislikes } else { (view_count as f64 * 0.01) as i64 };
        let comment_count = video.comment_count;
        let favorite_count = if view_count > 0 { view_count / 250 } else { 0 };
        let num_raters = view_count / 1000;

        let hint = instance_debug_hint(instance_url);
        let hint_esc = escape_xml(&hint);

        xml.push_str("  <entry>
");

        xml.push_str("    <id>http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/videos/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</id>
");

        xml.push_str("    <youTubeId id='");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'>");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</youTubeId>
");

        xml.push_str("    <published>");
        xml.push_str(&published);
        xml.push_str("</published>
");
        xml.push_str("    <updated>");
        xml.push_str(&published);
        xml.push_str("</updated>
");

        xml.push_str("    <category scheme='http://gdata.youtube.com/schemas/2007/categories.cat' label='Science &amp; Technology' term='Science &amp; Technology'>Science &amp; Technology</category>
");

        xml.push_str("    <title type='text'>");
        xml.push_str(&title_esc);
        xml.push_str("</title>
");
        xml.push_str("    <content type='text'>");
        xml.push_str(&title_esc);
        xml.push_str("</content>
");

        xml.push_str("    <link rel='http://gdata.youtube.com/schemas/2007#video.related' href='http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/videos/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/related'/>
");

        xml.push_str("    <author>
");
        xml.push_str("      <name>");
        xml.push_str(&author_esc);
        xml.push_str("</name>
");
        xml.push_str("      <uri>http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/users/");
        xml.push_str(&author_id_esc);
        xml.push_str("</uri>
");
        xml.push_str("      <yt:userId>");
        xml.push_str(&author_id_esc);
        xml.push_str("</yt:userId>
");
        xml.push_str("    </author>
");

        xml.push_str("    <gd:comments>
");
        xml.push_str("      <gd:feedLink href='http://");
        xml.push_str(host);
        xml.push_str("/feeds/api/videos/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/comments' countHint='");
        xml.push_str(&comment_count.to_string());
        xml.push_str("'/>
");
        xml.push_str("    </gd:comments>
");

        xml.push_str("    <media:group>
");
        xml.push_str("      <media:title>");
        xml.push_str(&title_esc);
        xml.push_str("</media:title>
");
        xml.push_str("      <media:category label='Science &amp; Technology' scheme='http://gdata.youtube.com/schemas/2007/categories.cat'>Science &amp; Technology</media:category>
");
        let embed_url = format!("{}/embed/{}?raw=1", instance_url.trim_end_matches('/'), escape_xml(vid));

        xml.push_str("      <media:content url='");
        xml.push_str(&embed_url);
        xml.push_str("' type='video/3gpp' medium='video' expression='full' duration='");
        xml.push_str(&video.length_seconds.to_string());
        xml.push_str("' yt:format='3'/>
");
        xml.push_str("      <media:description type='plain'>");
        xml.push_str(&hint_esc);
        xml.push_str("</media:description>
");
        xml.push_str("      <media:keywords>");
        xml.push_str(&title_esc);
        xml.push_str("</media:keywords>
");
        xml.push_str("      <media:player url='http://2009frontend.truehosting.net/watch?v=");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'/>
");
        xml.push_str("      <media:thumbnail yt:name='hqdefault' url='http://i.ytimg.com/vi/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/hqdefault.jpg' height='240' width='320' time='00:00:00'/>
");
        xml.push_str("      <media:thumbnail yt:name='poster' url='http://i.ytimg.com/vi/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/0.jpg' height='240' width='320' time='00:00:00'/>
");
        xml.push_str("      <media:thumbnail yt:name='default' url='http://i.ytimg.com/vi/");
        xml.push_str(&escape_xml(vid));
        xml.push_str("/0.jpg' height='240' width='320' time='00:00:00'/>
");
        xml.push_str("      <yt:duration seconds='");
        xml.push_str(&video.length_seconds.to_string());
        xml.push_str("'/>
");
        xml.push_str("      <yt:uploaded>");
        xml.push_str(&published);
        xml.push_str("</yt:uploaded>
");
        xml.push_str("      <yt:uploaderId>");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("</yt:uploaderId>
");
        xml.push_str("      <yt:videoid id='");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'>");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</yt:videoid>
");
        xml.push_str("      <youTubeId id='");
        xml.push_str(&escape_xml(vid));
        xml.push_str("'>");
        xml.push_str(&escape_xml(vid));
        xml.push_str("</youTubeId>
");
        xml.push_str("      <media:credit role='uploader' yt:display='");
        xml.push_str(&author_esc);
        xml.push_str("' name='");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("'>");
        xml.push_str(&escape_xml(author_id));
        xml.push_str("</media:credit>
");
        xml.push_str("    </media:group>
");

        xml.push_str("    <gd:rating average='5' max='5' min='1' numRaters='");
        xml.push_str(&num_raters.to_string());
        xml.push_str("' rel='http://schemas.google.com/g/2005#overall'/>
");
        xml.push_str("    <yt:statistics favoriteCount='");
        xml.push_str(&favorite_count.to_string());
        xml.push_str("' viewCount='");
        xml.push_str(&view_count.to_string());
        xml.push_str("'/>
");
        xml.push_str("    <yt:rating numLikes='");
        xml.push_str(&like_count.to_string());
        xml.push_str("' numDislikes='");
        xml.push_str(&dislike_count.to_string());
        xml.push_str("'/>
");

        xml.push_str("  </entry>
");
    }

    xml.push_str("</feed>");
    xml
}
// ============================================================
// INSTANCE MANAGER
// ============================================================

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

static INSTANCE_MANAGER: Lazy<Arc<InstanceManager>> = Lazy::new(|| Arc::new(InstanceManager::new()));

static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    Client::builder()
        .pool_max_idle_per_host(20)
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(4))
        .tcp_keepalive(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client")
});
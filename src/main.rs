// src/main.rs
mod api;
mod protobuf;

use axum::{Router, routing::get, routing::post};
use std::net::SocketAddr;
use tracing::info;
use once_cell::sync::Lazy;
use tower_http::trace::TraceLayer;
use tower_http::classify::ServerErrorsFailureClass;
use std::str::FromStr;
use tracing_subscriber::EnvFilter;
use crate::api::video_comments;
use crate::protobuf::browse::handle_browse;

pub static CONFIG: Lazy<Config> = Lazy::new(Config::load);

use axum::{body::Bytes, response::IntoResponse};
use serde::{Deserialize, Serialize};
use std::fs;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    pub host: String,
    #[serde(default)]
    pub primary_instance: String,
    #[serde(default)]
    pub primary_instances: Vec<String>,
    pub primary_rpm_limit: u64,
    pub instances_list_url: String,
    pub instances_refresh_secs: u64,
    pub server_host: String,
}

impl Config {
    fn load() -> Self {
        // Try to load from config.json
        if let Ok(content) = fs::read_to_string("config.json") {
            if let Ok(mut config) = serde_json::from_str::<Config>(&content) {
                if config.primary_instances.is_empty() {
                    if config.primary_instance.is_empty() {
                        config.primary_instances = vec!["http://inv.truehosting.net".to_string()];
                    } else {
                        config.primary_instances = vec![config.primary_instance.clone()];
                    }
                } else if config.primary_instance.is_empty() {
                    config.primary_instance = config.primary_instances[0].clone();
                }
                return config;
            }
        }
        
        // Fallback to default config
        Self::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        let primary = "http://inv.truehosting.net".to_string();
        Self {
            port: std::env::var("PORT").unwrap_or_else(|_| "8080".to_string()).parse().unwrap_or(8080),
            host: std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            primary_instance: primary.clone(),
            primary_instances: vec![primary.clone()],
            primary_rpm_limit: 5,
            instances_list_url: "http://144.31.189.129/notPipe.json".to_string(),
            instances_refresh_secs: 300,
            server_host: "192.168.0.31:80".to_string(),
            // innertube fields removed — this proxy uses Invidious only
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .with_env_filter(EnvFilter::from_str("info,yt_feeds_proxy=debug").unwrap_or(EnvFilter::new("info")))
        .init();

    info!("YouTube GData V2 Server (using Invidious)");
    info!("Port: {}", CONFIG.port);
    info!("Host: {}", CONFIG.host);
    info!("Primary Invidious: {}", CONFIG.primary_instances.join(", "));
    info!("Primary RPM Limit: {}", CONFIG.primary_rpm_limit);
    info!("Instances Refresh: {}s", CONFIG.instances_refresh_secs);
    info!("Server Host: {}", CONFIG.server_host);

    api::init_stats().await?;
    tokio::spawn(async {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            if let Err(err) = api::save_stats_snapshot().await {
                tracing::warn!("Failed to save stats snapshot: {}", err);
            }
        }
    });

    let app = Router::new()
        .route("/feeds/api/standardfeeds/:region/:feed", get(api::standard_feed))
        .route("/feeds/api/videos/:video_id", get(api::video_entry))
        .route("/feeds/api/videos/:video_id/", get(api::video_entry))
        .route("/feeds/api/videos", get(api::video_search))
        .route("/feeds/api/users/:username/uploads", get(api::user_uploads))
        .route("/feeds/api/users/:username", get(api::user_profile))
        .route("/feeds/api/users/default/playlists", get(api::user_playlists))
        .route("/feeds/api/playlists/:playlist_id", get(api::playlist_details))
        .route("/feeds/api/videos/:video_id/related", get(api::related_videos))
        .route("/feeds/api/videos/:video_id/comments", get(video_comments))
        .route("/schemas/2007/categories.cat", get(api::categories))
        .route("/youtube/accounts/registerDevice", get(api::register_device))
        .route("/youtube/accounts/registerDevice", post(api::register_device))
        .route("/deviceregistration/v1/devices", post(api::device_registration))
        .route("/channel_fh264_getvideo", get(api::channel_fh264_getvideo))
        .route("/exp_hd", get(api::exp_hd))
        .route("/get_480", get(api::get_480))
        .route("/get_video", get(api::get_video))
        .route("/embed/:video_id", get(api::embed_video))
        .route("/mobile/blzr/home", get(api::mobile_blzr_home))
        .route("/mobile/blzr/results", get(api::mobile_blzr_results))
        .route("/mobile/blzr/watch", get(api::mobile_blzr_watch))
        .route("/mobile/blzr/view_comment", get(api::mobile_blzr_view_comment))
        .route("/mobile/blzr/profile", get(api::mobile_blzr_profile))
        .route("/health", get(health_check))
        .route("/youtubei/v1/browse", post(browse_endpoint))
        .route("/get_hd_video", get(api::get_hd_video))
        .route("/admin/stats", get(api::stats))
        .route("/admin/stats.json", get(api::stats))
        .layer(axum::middleware::from_fn(api::record_request))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    tracing::info_span!(
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                    )
                })
                .on_request(|request: &axum::http::Request<_>, _span: &tracing::Span| {
                    info!("-> {} {}", request.method(), request.uri());
                })
                .on_response(
                    |response: &axum::http::Response<_>, latency: std::time::Duration, _span: &tracing::Span| {
                        let status = response.status();
                        info!("<- {} ({}ms)", status, latency.as_millis());
                    },
                )
                .on_failure(
                    |error: ServerErrorsFailureClass, latency: std::time::Duration, _span: &tracing::Span| {
                        info!("Request failed: {:?} ({}ms)", error, latency.as_millis());
                    },
                )
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], CONFIG.port));
    info!("Listening on http://{}", addr);
    info!("Waiting for requests...");
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

async fn browse_endpoint(body: Bytes) -> impl IntoResponse {
    handle_browse(body).await
}

async fn health_check() -> &'static str {
    "OK"
}

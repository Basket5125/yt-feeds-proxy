use serde::{Serialize, Deserialize};
use chrono::Utc;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feed {
    pub id: String,
    pub updated: String,
    pub title: String,
    #[serde(rename = "openSearch:totalResults")]
    pub total_results: usize,
    #[serde(rename = "openSearch:startIndex")]
    pub start_index: usize,
    #[serde(rename = "openSearch:itemsPerPage")]
    pub items_per_page: usize,
    #[serde(rename = "entry")]
    pub entries: Vec<VideoEntry>,
}

impl Default for Feed {
    fn default() -> Self {
        Self {
            id: String::new(),
            updated: Utc::now().to_rfc3339(),
            title: String::new(),
            total_results: 0,
            start_index: 1,
            items_per_page: 25,
            entries: vec![],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VideoEntry {
    pub id: String,
    pub title: String,
    pub author: String,
    #[serde(rename = "yt:userId")]
    pub author_id: String,
    pub published: String,
    pub updated: String,
    pub duration: i32,
    #[serde(rename = "yt:statistics")]
    pub statistics: Statistics,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Statistics {
    #[serde(rename = "@viewCount")]
    pub view_count: i32,
}

impl VideoEntry {
    pub fn from_json(data: &serde_json::Value) -> Option<Self> {
        let video_id = data.get("videoId")?.as_str()?.to_string();
        let title = data.get("title")?
            .get("runs")?.get(0)?
            .get("text")?.as_str()
            .unwrap_or("").to_string();
        
        let author = data.get("ownerText")?
            .get("runs")?.get(0)?
            .get("text")?.as_str()
            .unwrap_or("").to_string();
        
        let author_id = data.get("ownerText")?
            .get("runs")?.get(0)?
            .get("navigationEndpoint")?
            .get("browseEndpoint")?
            .get("browseId")?.as_str()
            .unwrap_or("").to_string();
        
        let duration = data.get("lengthSeconds")?
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        
        let view_count = 0;
        
        Some(Self {
            id: video_id,
            title,
            author,
            author_id,
            published: Utc::now().to_rfc3339(),
            updated: Utc::now().to_rfc3339(),
            duration,
            statistics: Statistics { view_count },
        })
    }

    pub fn from_playlist_json(data: &serde_json::Value) -> Option<Self> {
        let video_id = data.get("videoId")?.as_str()?.to_string();
        let title = data.get("title")?
            .get("simpleText")?.as_str()
            .unwrap_or("").to_string();
        
        let author = data.get("shortBylineText")?
            .get("runs")?.get(0)?
            .get("text")?.as_str()
            .unwrap_or("").to_string();
        
        let author_id = data.get("shortBylineText")?
            .get("runs")?.get(0)?
            .get("navigationEndpoint")?
            .get("browseEndpoint")?
            .get("browseId")?.as_str()
            .unwrap_or("").to_string();
        
        let duration = data.get("lengthSeconds")?
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        
        Some(Self {
            id: video_id,
            title,
            author,
            author_id,
            published: Utc::now().to_rfc3339(),
            updated: Utc::now().to_rfc3339(),
            duration,
            statistics: Statistics { view_count: 0 },
        })
    }
    
    pub fn simple(id: String, title: String, author: String, author_id: String, duration: i32, view_count: i32, _description: String) -> Self {
        Self {
            id,
            title,
            author,
            author_id,
            published: Utc::now().to_rfc3339(),
            updated: Utc::now().to_rfc3339(),
            duration,
            statistics: Statistics { view_count },
        }
    }
}

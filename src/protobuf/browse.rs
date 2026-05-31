use axum::{body::Bytes, http::header, response::IntoResponse};

pub async fn handle_browse(body: Bytes) -> impl IntoResponse {
    let browse_id = super::extract_browse_id(&body);
    tracing::debug!("Browse ID: {}", browse_id);
    
    let response_bytes = match browse_id.as_str() {
        id if id.starts_with("UC") => build_channel_response(id).await,
        id if id.starts_with("VL") => build_playlist_response(id).await,
        "FEwhat_to_watch" => build_homepage_response().await,
        "FEtrending" => build_trending_response().await,
        _ => vec![]
    };
    
    let mut response = response_bytes.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/x-protobuf".parse().unwrap()
    );
    response
}

// Helper function to write a varint
fn write_varint(buf: &mut Vec<u8>, value: u64) {
    let mut v = value;
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

// Write field key + varint
fn write_field_varint(buf: &mut Vec<u8>, field: u32, value: u64) {
    write_varint(buf, (field as u64) << 3);
    write_varint(buf, value);
}

// Write field key + string
fn write_field_string(buf: &mut Vec<u8>, field: u32, value: &str) {
    write_varint(buf, ((field as u64) << 3) | 2);
    write_varint(buf, value.len() as u64);
    buf.extend_from_slice(value.as_bytes());
}

// Write field key for a nested message and reserve length
fn write_field_message_start(buf: &mut Vec<u8>, field: u32) -> usize {
    write_varint(buf, ((field as u64) << 3) | 2);
    let pos = buf.len();
    buf.push(0); // placeholder for length
    pos
}

fn write_field_message_end(buf: &mut Vec<u8>, start_pos: usize) {
    let len = buf.len() - start_pos - 1;
    let len_bytes = encode_varint_to_vec(len as u64);
    buf.splice(start_pos..start_pos+1, len_bytes);
}

fn encode_varint_to_vec(value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    write_varint(&mut buf, value);
    buf
}

async fn build_channel_response(channel_id: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    
    // Root message
    let root_start = write_field_message_start(&mut buf, 1); // header
    write_field_string(&mut buf, 1, channel_id); // channel id
    write_field_string(&mut buf, 2, "YouTube Channel"); // channel name
    write_field_message_end(&mut buf, root_start);
    
    // Contents
    let content_start = write_field_message_start(&mut buf, 2); // contents
    write_field_string(&mut buf, 1, "Videos"); // tab name
    write_field_varint(&mut buf, 2, 1); // selected
    write_field_message_end(&mut buf, content_start);
    
    buf
}

async fn build_playlist_response(_playlist_id: &str) -> Vec<u8> {
    vec![0x08, 0x00] // temporarily empty
}

async fn build_homepage_response() -> Vec<u8> {
    vec![0x08, 0x01] // temporary
}

async fn build_trending_response() -> Vec<u8> {
    vec![0x08, 0x02] // temporary
}
pub mod browse;

pub fn extract_browse_id(bytes: &[u8]) -> String {
    // Szukamy browse_id w surowym protobufie
    // Format: field_number(1) + wire_type(2) + length + string
    let mut i = 0;
    while i < bytes.len() {
        if i + 2 > bytes.len() { break; }
        
        let tag = bytes[i];
        let field_number = tag >> 3;
        let wire_type = tag & 0x07;
        
        if field_number == 1 && wire_type == 2 {
            let len = bytes[i + 1] as usize;
            if i + 2 + len <= bytes.len() {
                return String::from_utf8_lossy(&bytes[i+2..i+2+len]).to_string();
            }
        }
        
        // Skip to next field
        if wire_type == 0 { // varint
            while i < bytes.len() && bytes[i] & 0x80 != 0 { i += 1; }
            i += 1;
        } else if wire_type == 2 { // length-delimited
            if i + 1 < bytes.len() {
                let len = bytes[i + 1] as usize;
                i += 2 + len;
            } else { break; }
        } else {
            i += 1;
        }
    }
    "FEwhat_to_watch".to_string()
}
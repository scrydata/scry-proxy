use super::*;
use bytes::{Buf, Bytes};
use std::sync::Mutex;
use tracing::{debug, trace, warn};

/// Extracts query information from Postgres wire protocol messages
pub struct MessageExtractor {
    buffer: Mutex<Vec<u8>>,
}

impl MessageExtractor {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Try to extract a query from the given data
    ///
    /// Returns the query text if a Query message is found
    pub fn extract_query(&self, data: &[u8]) -> Option<String> {
        if data.is_empty() {
            return None;
        }

        let mut buffer = self.buffer.lock().ok()?;

        // Append new data to buffer
        buffer.extend_from_slice(data);

        // Try to parse messages from buffer
        let query = Self::parse_messages_from(&buffer);

        // Clear buffer after processing (for simplicity, may want to keep partial messages)
        // For now, we'll process complete messages and discard
        buffer.clear();

        query
    }

    /// Check if the data indicates a query is complete
    ///
    /// Looks for CommandComplete or ReadyForQuery messages
    pub fn is_query_complete(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }

        // Look for CommandComplete (C) or ReadyForQuery (Z) message
        for &msg_type in data {
            if msg_type == MSG_COMMAND_COMPLETE || msg_type == MSG_READY_FOR_QUERY {
                trace!(msg_type = msg_type, "Found query completion marker");
                return true;
            }
        }

        false
    }

    /// Check if the data contains an error response and extract the error message
    ///
    /// Returns Some(error_message) if an ErrorResponse (E) message is found
    pub fn extract_error(&self, data: &[u8]) -> Option<String> {
        if data.is_empty() {
            return None;
        }

        // Look for ErrorResponse message
        let mut bytes = Bytes::copy_from_slice(data);

        while bytes.remaining() >= 5 {
            let msg_type = bytes[0];

            if msg_type == MSG_ERROR_RESPONSE {
                bytes.advance(1); // Skip type
                let length = bytes.get_i32() as usize;

                if bytes.remaining() >= length - 4 {
                    let error_data = &bytes[..length - 4];
                    return Self::parse_error_fields(error_data);
                }
            } else if bytes.remaining() >= 5 {
                // Try to skip this message
                bytes.advance(1);
                if bytes.remaining() >= 4 {
                    let length = bytes.get_i32() as usize;
                    if length > 4 && bytes.remaining() >= length - 4 {
                        bytes.advance(length - 4);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        None
    }

    /// Parse error fields from ErrorResponse message payload
    ///
    /// Extracts the 'M' (message) field as the primary error description
    fn parse_error_fields(data: &[u8]) -> Option<String> {
        let mut message = None;
        let mut severity = None;
        let mut i = 0;

        while i < data.len() {
            let field_type = data[i];
            i += 1;

            if field_type == 0 {
                // End of fields
                break;
            }

            // Find null terminator for field value
            let start = i;
            while i < data.len() && data[i] != 0 {
                i += 1;
            }

            if i >= data.len() {
                break;
            }

            // Extract field value
            if let Ok(value) = std::str::from_utf8(&data[start..i]) {
                match field_type {
                    b'S' => severity = Some(value.to_string()),
                    b'M' => message = Some(value.to_string()),
                    _ => {} // Ignore other fields for now
                }
            }

            i += 1; // Skip null terminator
        }

        // Construct error message with severity if available
        match (severity, message) {
            (Some(sev), Some(msg)) => Some(format!("{}: {}", sev, msg)),
            (None, Some(msg)) => Some(msg),
            _ => None,
        }
    }

    fn parse_messages_from(buffer: &[u8]) -> Option<String> {
        if buffer.len() < 5 {
            // Need at least: type (1) + length (4)
            return None;
        }

        let mut bytes = Bytes::copy_from_slice(buffer);
        let mut query = None;

        while bytes.remaining() >= 5 {
            let msg_type = bytes[0];

            match msg_type {
                MSG_QUERY => {
                    // Query message: 'Q' + length (4) + query string (null-terminated)
                    bytes.advance(1); // Skip type
                    let length = bytes.get_i32() as usize;

                    if bytes.remaining() >= length - 4 {
                        let query_data = &bytes[..length - 4];
                        if let Some(query_text) = Self::extract_query_text(query_data) {
                            debug!(query = %query_text, "Extracted query from Query message");
                            query = Some(query_text);
                        }
                        bytes.advance(length - 4);
                    } else {
                        // Incomplete message
                        break;
                    }
                }
                MSG_PARSE => {
                    // Parse message: 'P' + length + statement name + query + ...
                    bytes.advance(1); // Skip type
                    if bytes.remaining() < 4 {
                        break;
                    }
                    let length = bytes.get_i32() as usize;

                    if bytes.remaining() >= length - 4 {
                        // Skip statement name (null-terminated string)
                        let mut remaining_data = &bytes[..length - 4];
                        if let Some(null_pos) = remaining_data.iter().position(|&b| b == 0) {
                            remaining_data = &remaining_data[null_pos + 1..];

                            // Now extract query
                            if let Some(query_text) = Self::extract_query_text(remaining_data) {
                                debug!(query = %query_text, "Extracted query from Parse message");
                                query = Some(query_text);
                            }
                        }
                        bytes.advance(length - 4);
                    } else {
                        break;
                    }
                }
                _ => {
                    // Unknown or unhandled message type, try to skip
                    trace!(msg_type = msg_type, "Skipping message type");
                    bytes.advance(1);
                }
            }
        }

        query
    }

    fn extract_query_text(data: &[u8]) -> Option<String> {
        // Find null terminator
        let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());

        // Convert to string
        match std::str::from_utf8(&data[..end]) {
            Ok(s) => {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to parse query as UTF-8");
                None
            }
        }
    }
}

impl Default for MessageExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_query() {
        let mut extractor = MessageExtractor::new();

        // Construct a Query message: 'Q' + length + "SELECT 1" + null
        let query = b"SELECT 1";
        let length = (query.len() + 1 + 4) as i32; // query + null + length field itself

        let mut msg = vec![MSG_QUERY];
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(query);
        msg.push(0); // null terminator

        let result = extractor.extract_query(&msg);
        assert_eq!(result, Some("SELECT 1".to_string()));
    }

    #[test]
    fn test_is_query_complete() {
        let extractor = MessageExtractor::new();

        // CommandComplete message
        let mut msg = vec![MSG_COMMAND_COMPLETE];
        msg.extend_from_slice(&[0, 0, 0, 10]); // length
        msg.extend_from_slice(b"SELECT 1");
        msg.push(0);

        assert!(extractor.is_query_complete(&msg));
    }

    #[test]
    fn test_ready_for_query() {
        let extractor = MessageExtractor::new();

        // ReadyForQuery message
        let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I'];

        assert!(extractor.is_query_complete(&msg));
    }

    #[test]
    fn test_incomplete_message() {
        let mut extractor = MessageExtractor::new();

        // Incomplete message (just type byte)
        let msg = vec![MSG_QUERY];

        let result = extractor.extract_query(&msg);
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_error_message() {
        let extractor = MessageExtractor::new();

        // Construct an ErrorResponse message
        // Format: 'E' + length + fields + terminator
        // Field format: type byte + value string + null
        let mut msg = vec![MSG_ERROR_RESPONSE];

        // Build the fields
        let mut fields = Vec::new();
        // Severity field
        fields.push(b'S');
        fields.extend_from_slice(b"ERROR");
        fields.push(0);
        // Message field
        fields.push(b'M');
        fields.extend_from_slice(b"syntax error at or near \"SELEC\"");
        fields.push(0);
        // Terminator
        fields.push(0);

        let length = (fields.len() + 4) as i32;
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&fields);

        let result = extractor.extract_error(&msg);
        assert!(result.is_some());
        let error = result.unwrap();
        assert!(error.contains("ERROR"));
        assert!(error.contains("syntax error"));
    }

    #[test]
    fn test_no_error_in_normal_message() {
        let extractor = MessageExtractor::new();

        // CommandComplete message (not an error)
        let mut msg = vec![MSG_COMMAND_COMPLETE];
        msg.extend_from_slice(&[0, 0, 0, 10]);
        msg.extend_from_slice(b"SELECT 1");
        msg.push(0);

        let result = extractor.extract_error(&msg);
        assert_eq!(result, None);
    }
}

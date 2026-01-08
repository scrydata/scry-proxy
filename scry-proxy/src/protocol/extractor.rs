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
        Self { buffer: Mutex::new(Vec::new()) }
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

    /// Extract a typed message from raw protocol data
    ///
    /// Returns a parsed Message enum for extended query protocol messages
    pub fn extract_message(&self, data: &[u8]) -> Option<Message> {
        if data.len() < 5 {
            return None;
        }

        let msg_type = data[0];
        let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;

        if data.len() < 1 + length {
            return None; // Incomplete message
        }

        let payload = &data[5..1 + length];

        match msg_type {
            MSG_QUERY => self.parse_query_message(payload),
            MSG_PARSE => self.parse_parse_message(payload),
            MSG_BIND => self.parse_bind_message(payload),
            MSG_EXECUTE => self.parse_execute_message(payload),
            MSG_CLOSE => self.parse_close_message(payload),
            MSG_SYNC => Some(Message::Sync),
            MSG_TERMINATE => Some(Message::Terminate),
            _ => None,
        }
    }

    /// Extract ALL messages from raw protocol data
    ///
    /// Returns a Vec of all parsed Message enums from the buffer.
    /// Extended query protocol bundles multiple messages (Parse+Bind+Execute+Sync)
    /// in a single TCP packet, so we need to extract them all.
    pub fn extract_messages(&self, data: &[u8]) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut offset = 0;

        while offset + 5 <= data.len() {
            let msg_type = data[offset];
            let length = i32::from_be_bytes([
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
            ]) as usize;

            // Check if we have the complete message
            if offset + 1 + length > data.len() {
                break; // Incomplete message
            }

            let payload = &data[offset + 5..offset + 1 + length];

            let msg = match msg_type {
                MSG_QUERY => self.parse_query_message(payload),
                MSG_PARSE => self.parse_parse_message(payload),
                MSG_BIND => self.parse_bind_message(payload),
                MSG_EXECUTE => self.parse_execute_message(payload),
                MSG_CLOSE => self.parse_close_message(payload),
                MSG_SYNC => Some(Message::Sync),
                MSG_TERMINATE => Some(Message::Terminate),
                _ => None,
            };

            if let Some(m) = msg {
                messages.push(m);
            }

            offset += 1 + length;
        }

        messages
    }

    fn parse_query_message(&self, payload: &[u8]) -> Option<Message> {
        let query = Self::read_cstring(payload, 0)?;
        Some(Message::Query { query })
    }

    fn parse_parse_message(&self, payload: &[u8]) -> Option<Message> {
        let mut offset = 0;

        // Statement name
        let name = Self::read_cstring(payload, offset)?;
        offset += name.len() + 1;

        // Query string
        let query = Self::read_cstring(payload, offset)?;
        offset += query.len() + 1;

        // Number of parameter types
        if offset + 2 > payload.len() {
            return None;
        }
        let num_params = i16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;

        // Parameter OIDs
        let mut param_oids = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            if offset + 4 > payload.len() {
                return None;
            }
            let oid = u32::from_be_bytes([
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
            ]);
            param_oids.push(oid);
            offset += 4;
        }

        Some(Message::Parse { name, query, param_oids })
    }

    fn parse_bind_message(&self, payload: &[u8]) -> Option<Message> {
        let mut offset = 0;

        // Portal name
        let portal = Self::read_cstring(payload, offset)?;
        offset += portal.len() + 1;

        // Statement name
        let statement = Self::read_cstring(payload, offset)?;
        offset += statement.len() + 1;

        // Number of format codes
        if offset + 2 > payload.len() {
            return None;
        }
        let num_format_codes = i16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;

        // Format codes
        let mut format_codes = Vec::with_capacity(num_format_codes);
        for _ in 0..num_format_codes {
            if offset + 2 > payload.len() {
                return None;
            }
            let code = i16::from_be_bytes([payload[offset], payload[offset + 1]]);
            format_codes.push(code);
            offset += 2;
        }

        // Number of parameters
        if offset + 2 > payload.len() {
            return None;
        }
        let num_params = i16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
        offset += 2;

        // Parameters
        let mut params_raw = Vec::with_capacity(num_params);
        for _ in 0..num_params {
            if offset + 4 > payload.len() {
                return None;
            }
            let len = i32::from_be_bytes([
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
            ]);
            offset += 4;

            if len == -1 {
                // NULL value
                params_raw.push(None);
            } else {
                let len = len as usize;
                if offset + len > payload.len() {
                    return None;
                }
                params_raw.push(Some(payload[offset..offset + len].to_vec()));
                offset += len;
            }
        }

        Some(Message::Bind { portal, statement, format_codes, params_raw })
    }

    fn parse_execute_message(&self, payload: &[u8]) -> Option<Message> {
        let portal = Self::read_cstring(payload, 0)?;
        Some(Message::Execute { portal })
    }

    fn parse_close_message(&self, payload: &[u8]) -> Option<Message> {
        if payload.is_empty() {
            return None;
        }
        let kind = payload[0] as char;
        let name = Self::read_cstring(payload, 1)?;
        Some(Message::Close { kind, name })
    }

    fn read_cstring(data: &[u8], offset: usize) -> Option<String> {
        let start = offset;
        let mut end = start;
        while end < data.len() && data[end] != 0 {
            end += 1;
        }
        if end >= data.len() {
            return None; // No null terminator found
        }
        String::from_utf8(data[start..end].to_vec()).ok()
    }

    /// Extract ReadyForQuery status from backend response stream
    ///
    /// Scans through the message stream looking for ReadyForQuery ('Z') message
    /// and returns the transaction status byte: 'I' (idle), 'T' (in transaction), 'E' (error)
    ///
    /// For pipelined commands, a single buffer may contain multiple ReadyForQuery messages.
    /// This function returns the LAST ReadyForQuery status, which represents the final
    /// transaction state after all commands in the pipeline have completed.
    ///
    /// This is a streaming scan - no buffering required.
    pub fn extract_ready_for_query(&self, data: &[u8]) -> Option<u8> {
        let mut offset = 0;
        let mut last_status: Option<u8> = None;

        while offset + 5 <= data.len() {
            let msg_type = data[offset];

            if msg_type == MSG_READY_FOR_QUERY {
                // ReadyForQuery is always 6 bytes: type(1) + length(4) + status(1)
                // Length is always 5 (includes itself but not type byte)
                if offset + 6 <= data.len() {
                    last_status = Some(data[offset + 5]);
                }
            }

            // Skip to next message
            if offset + 5 <= data.len() {
                let length = i32::from_be_bytes([
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                ]) as usize;

                if length < 4 || offset + 1 + length > data.len() {
                    break; // Invalid or incomplete message
                }
                offset += 1 + length;
            } else {
                break;
            }
        }

        last_status
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

    #[test]
    fn test_extract_bind_message() {
        let extractor = MessageExtractor::new();

        // Build Bind message: B + len + portal\0 + stmt\0 + format_count + params_count + param_len + param_data
        let mut msg = vec![MSG_BIND];

        // Length placeholder (will calculate)
        let len_pos = msg.len();
        msg.extend_from_slice(&[0, 0, 0, 0]);

        // Portal name (empty = unnamed)
        msg.push(0);

        // Statement name
        msg.extend_from_slice(b"stmt1");
        msg.push(0);

        // Number of format codes: 1
        msg.extend_from_slice(&1i16.to_be_bytes());
        // Format code: 0 (text)
        msg.extend_from_slice(&0i16.to_be_bytes());

        // Number of parameters: 1
        msg.extend_from_slice(&1i16.to_be_bytes());
        // Parameter length: 2
        msg.extend_from_slice(&2i32.to_be_bytes());
        // Parameter value: "42"
        msg.extend_from_slice(b"42");

        // Result format codes: 0
        msg.extend_from_slice(&0i16.to_be_bytes());

        // Fix length
        let len = (msg.len() - 1) as i32;
        msg[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        let result = extractor.extract_message(&msg);
        assert!(matches!(result, Some(Message::Bind { .. })));

        if let Some(Message::Bind { portal, statement, params_raw, .. }) = result {
            assert_eq!(portal, "");
            assert_eq!(statement, "stmt1");
            assert_eq!(params_raw.len(), 1);
            assert_eq!(params_raw[0], Some(b"42".to_vec()));
        }
    }

    #[test]
    fn test_extract_parse_message() {
        let extractor = MessageExtractor::new();

        // Build Parse message: P + len + name\0 + query\0 + param_count + oids
        let mut msg = vec![MSG_PARSE];
        let len_pos = msg.len();
        msg.extend_from_slice(&[0, 0, 0, 0]);

        // Statement name
        msg.extend_from_slice(b"stmt1");
        msg.push(0);

        // Query
        msg.extend_from_slice(b"SELECT * FROM users WHERE id = $1");
        msg.push(0);

        // Number of parameter types: 1
        msg.extend_from_slice(&1i16.to_be_bytes());
        // OID for int4: 23
        msg.extend_from_slice(&23u32.to_be_bytes());

        let len = (msg.len() - 1) as i32;
        msg[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        let result = extractor.extract_message(&msg);

        if let Some(Message::Parse { name, query, param_oids }) = result {
            assert_eq!(name, "stmt1");
            assert_eq!(query, "SELECT * FROM users WHERE id = $1");
            assert_eq!(param_oids, vec![23]);
        } else {
            panic!("Expected Parse message");
        }
    }

    #[test]
    fn test_extract_bind_with_null() {
        let extractor = MessageExtractor::new();

        let mut msg = vec![MSG_BIND];
        let len_pos = msg.len();
        msg.extend_from_slice(&[0, 0, 0, 0]);

        msg.push(0); // portal
        msg.extend_from_slice(b"stmt1");
        msg.push(0); // statement

        msg.extend_from_slice(&0i16.to_be_bytes()); // no format codes
        msg.extend_from_slice(&1i16.to_be_bytes()); // 1 param
        msg.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        msg.extend_from_slice(&0i16.to_be_bytes()); // no result formats

        let len = (msg.len() - 1) as i32;
        msg[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        let result = extractor.extract_message(&msg);

        if let Some(Message::Bind { params_raw, .. }) = result {
            assert_eq!(params_raw.len(), 1);
            assert_eq!(params_raw[0], None);
        } else {
            panic!("Expected Bind message");
        }
    }

    #[test]
    fn test_extract_ready_for_query_idle() {
        let extractor = MessageExtractor::new();
        // ReadyForQuery: 'Z' + length(5) + status('I')
        let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I'];

        let result = extractor.extract_ready_for_query(&msg);
        assert_eq!(result, Some(b'I'));
    }

    #[test]
    fn test_extract_ready_for_query_in_transaction() {
        let extractor = MessageExtractor::new();
        let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'T'];

        let result = extractor.extract_ready_for_query(&msg);
        assert_eq!(result, Some(b'T'));
    }

    #[test]
    fn test_extract_ready_for_query_error() {
        let extractor = MessageExtractor::new();
        let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'E'];

        let result = extractor.extract_ready_for_query(&msg);
        assert_eq!(result, Some(b'E'));
    }

    #[test]
    fn test_extract_ready_for_query_in_stream() {
        let extractor = MessageExtractor::new();
        // DataRow + CommandComplete + ReadyForQuery
        let mut msg = vec![];
        // DataRow: 'D' + length + data
        msg.extend_from_slice(&[MSG_DATA_ROW, 0, 0, 0, 11]);
        msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'1']); // 1 column, value "1"
                                                          // CommandComplete: 'C' + length + "SELECT 1\0"
        msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 13]);
        msg.extend_from_slice(b"SELECT 1\0");
        // ReadyForQuery
        msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I']);

        let result = extractor.extract_ready_for_query(&msg);
        assert_eq!(result, Some(b'I'));
    }

    #[test]
    fn test_no_ready_for_query() {
        let extractor = MessageExtractor::new();
        // Just a DataRow, no ReadyForQuery
        let mut msg = vec![MSG_DATA_ROW, 0, 0, 0, 11];
        msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'1']);

        let result = extractor.extract_ready_for_query(&msg);
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_ready_for_query_pipelined_returns_last() {
        let extractor = MessageExtractor::new();
        // Simulate pipelined response: BEGIN (T) + COMMIT (I)
        // First: ReadyForQuery with T (in transaction)
        // Second: ReadyForQuery with I (idle - after commit)
        let mut msg = vec![];
        // CommandComplete: BEGIN
        msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 10]);
        msg.extend_from_slice(b"BEGIN\0");
        // ReadyForQuery: T (in transaction)
        msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'T']);
        // CommandComplete: COMMIT
        msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 11]);
        msg.extend_from_slice(b"COMMIT\0");
        // ReadyForQuery: I (idle - final state)
        msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I']);

        let result = extractor.extract_ready_for_query(&msg);
        // Should return 'I' (the LAST ReadyForQuery), not 'T' (the first)
        assert_eq!(result, Some(b'I'));
    }

    #[test]
    fn test_extract_ready_for_query_multiple_in_error() {
        let extractor = MessageExtractor::new();
        // Simulate: command fails in transaction -> error state
        let mut msg = vec![];
        // ReadyForQuery: T (in transaction before error)
        msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'T']);
        // Some ErrorResponse would be here in real data
        // ReadyForQuery: E (in error state - final)
        msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'E']);

        let result = extractor.extract_ready_for_query(&msg);
        // Should return 'E' (the LAST), not 'T'
        assert_eq!(result, Some(b'E'));
    }
}

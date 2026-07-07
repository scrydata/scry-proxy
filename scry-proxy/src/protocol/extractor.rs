use super::*;
use bytes::{Buf, Bytes};
use std::sync::Mutex;
use tracing::{debug, trace, warn};

/// Parsed fields from a Postgres ErrorResponse.
#[derive(Debug, Default)]
struct ErrorFields {
    /// 'S' severity (e.g. "ERROR", "FATAL").
    severity: Option<String>,
    /// 'C' SQLSTATE code (e.g. "23505"). Safe to surface — carries no literals.
    sqlstate: Option<String>,
    /// 'M' human-readable message. May echo query literals; never surface on
    /// the anonymized path.
    message: Option<String>,
}

/// Practical upper bound on a single frontend (client->backend) message's
/// framed length field. `extract_messages` buffers incomplete trailing bytes
/// across calls so a message split across TCP reads can be reassembled; this
/// bound keeps that buffer from growing without limit when the length field is
/// garbled or adversarial. Legitimate `Parse`/`Bind` payloads (large SQL text,
/// bulk parameter data) are expected to stay well under this; anything beyond
/// it is treated as a framing error rather than buffered indefinitely.
const MAX_FRONTEND_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MiB

/// Extracts query information from Postgres wire protocol messages
pub struct MessageExtractor {
    buffer: Mutex<Vec<u8>>,
    /// Reassembly buffer for `extract_messages`, used ONLY on the client->backend
    /// (frontend) direction. Deliberately kept separate from `buffer` (used only
    /// by the legacy `extract_query`) so the two accumulation paths can never
    /// interfere with each other, even if a future caller invoked both on the
    /// same `MessageExtractor` instance. See extractor.rs module docs / WP-9
    /// Task 3 report for the verification that today nothing does.
    frontend_buffer: Mutex<Vec<u8>>,
}

impl MessageExtractor {
    pub fn new() -> Self {
        Self { buffer: Mutex::new(Vec::new()), frontend_buffer: Mutex::new(Vec::new()) }
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
    /// Looks for CommandComplete ('C') or ReadyForQuery ('Z') messages
    /// using proper PostgreSQL message framing. Only checks message type
    /// bytes at actual message boundaries, not raw bytes in the stream.
    ///
    /// This prevents false positives from binary data containing 0x43 ('C')
    /// or 0x5A ('Z') bytes in query results or error messages.
    pub fn is_query_complete(&self, data: &[u8]) -> bool {
        if data.is_empty() {
            return false;
        }

        let mut offset = 0;

        while offset + 5 <= data.len() {
            let msg_type = data[offset];

            // Check if this is a completion message
            if msg_type == MSG_COMMAND_COMPLETE || msg_type == MSG_READY_FOR_QUERY {
                trace!(msg_type = msg_type, "Found query completion marker");
                return true;
            }

            // Read the length field to skip to next message
            let length = i32::from_be_bytes([
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
            ]) as usize;

            // Validate length field
            if length < 4 || offset + 1 + length > data.len() {
                // Invalid or incomplete message, stop scanning
                break;
            }

            // Advance to next message
            offset += 1 + length;
        }

        false
    }

    /// Check if the data contains an error response and extract the error message
    ///
    /// Returns Some(error_message) if an ErrorResponse (E) message is found
    pub fn extract_error(&self, data: &[u8]) -> Option<String> {
        let fields = Self::find_error_fields(data)?;
        // Full form (used only on the non-anonymized path): severity + the
        // human-readable message, which can echo query literals. Callers that
        // anonymize must use `extract_error_scrubbed` instead.
        match (fields.severity, fields.message) {
            (Some(sev), Some(msg)) => Some(format!("{}: {}", sev, msg)),
            (None, Some(msg)) => Some(msg),
            (Some(sev), None) => Some(sev),
            (None, None) => None,
        }
    }

    /// Extract a *scrubbed* error suitable for anonymized events.
    ///
    /// Returns only the severity and SQLSTATE code (e.g. `"ERROR: 23505"`),
    /// never the free-text 'M' message — which routinely echoes the offending
    /// literal ("Key (email)=(bob@example.com) already exists") and would defeat
    /// anonymization (P1 §4.4). Returns `None` only when neither severity nor
    /// SQLSTATE is present.
    pub fn extract_error_scrubbed(&self, data: &[u8]) -> Option<String> {
        let fields = Self::find_error_fields(data)?;
        match (fields.severity, fields.sqlstate) {
            (Some(sev), Some(code)) => Some(format!("{}: {}", sev, code)),
            (Some(sev), None) => Some(sev),
            (None, Some(code)) => Some(code),
            (None, None) => None,
        }
    }

    /// Locate the first ErrorResponse in `data` and parse its fields.
    fn find_error_fields(data: &[u8]) -> Option<ErrorFields> {
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
                    return Some(Self::parse_error_fields(error_data));
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

    /// Parse the severity ('S'), SQLSTATE ('C') and message ('M') fields from an
    /// ErrorResponse message payload. The free-text message is captured but must
    /// only be surfaced on the non-anonymized path (see `extract_error_scrubbed`).
    fn parse_error_fields(data: &[u8]) -> ErrorFields {
        let mut fields = ErrorFields::default();
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
                    b'S' => fields.severity = Some(value.to_string()),
                    b'C' => fields.sqlstate = Some(value.to_string()),
                    b'M' => fields.message = Some(value.to_string()),
                    _ => {} // Ignore other fields (detail/hint may echo literals)
                }
            }

            i += 1; // Skip null terminator
        }

        fields
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
                            debug!(query = %crate::observability::loggable(&query_text), "Extracted query from Query message");
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
                                debug!(query = %crate::observability::loggable(&query_text), "Extracted query from Parse message");
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
    /// Returns a Vec of all parsed Message enums seen so far, INCLUDING any
    /// left over from a previous call whose trailing bytes were incomplete.
    /// Extended query protocol bundles multiple messages (Parse+Bind+Execute+Sync)
    /// in a single TCP packet, so we need to extract them all.
    ///
    /// This method is observational only — it never mutates or drops bytes from
    /// the forwarded stream; callers pass the original `data` slice through to
    /// the backend unchanged regardless of what this returns. It reassembles
    /// frontend messages that are split across multiple `read()` calls (or that
    /// exceed a single read buffer) by retaining any incomplete trailing bytes
    /// in an internal buffer and prepending them on the next call. This buffer
    /// is used ONLY for the client->backend direction (see `frontend_buffer`
    /// docs); it must never be shared with a backend->client scan.
    ///
    /// Malformed input (a framing length that is invalid or implausibly large)
    /// is treated as a framing error: any messages successfully parsed so far
    /// this call are still returned, but the retained buffer is discarded
    /// rather than grown without bound. Because downstream state-tracking is
    /// fail-closed (unknown state -> pinned connection), a dropped/garbled
    /// parse degrades to reduced pooling, never to stream corruption or a panic.
    pub fn extract_messages(&self, data: &[u8]) -> Vec<Message> {
        let mut messages = Vec::new();

        let Ok(mut buffer) = self.frontend_buffer.lock() else {
            // Poisoned lock: fail closed for this call rather than panic.
            // Observational path only — forwarding is unaffected.
            warn!("frontend reassembly buffer lock poisoned; dropping this chunk");
            return messages;
        };

        buffer.extend_from_slice(data);

        let mut offset = 0;
        let mut framing_error = false;

        while offset + 5 <= buffer.len() {
            let msg_type = buffer[offset];
            let length_i32 = i32::from_be_bytes([
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
            ]);

            // The length field includes itself (4 bytes) but not the type byte,
            // so a valid length is always >= 4. Reject negative/implausible
            // values before doing any arithmetic on them.
            if length_i32 < 4 || length_i32 as usize > MAX_FRONTEND_MESSAGE_SIZE {
                warn!(
                    msg_type = msg_type,
                    length = length_i32,
                    max = MAX_FRONTEND_MESSAGE_SIZE,
                    "Invalid or oversized frontend message length; discarding buffered frontend bytes"
                );
                framing_error = true;
                break;
            }
            let length = length_i32 as usize;

            // Check if we have the complete message.
            if offset + 1 + length > buffer.len() {
                break; // Incomplete message: retain from `offset` for next call.
            }

            let payload = &buffer[offset + 5..offset + 1 + length];

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

        if framing_error {
            // Fail-closed: drop everything currently buffered (including bytes
            // after the bad frame, which can no longer be reliably
            // resynchronized) rather than let the buffer grow unbounded or wedge.
            buffer.clear();
        } else if offset > 0 {
            // Retain only the unconsumed tail for the next call.
            buffer.drain(0..offset);
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

    /// Check if the data contains a properly-framed ReadyForQuery message
    ///
    /// Unlike raw byte search (e.g., `data.contains(&b'Z')`), this method
    /// correctly parses PostgreSQL message frames and only returns true
    /// when a valid ReadyForQuery message is found at a message boundary.
    ///
    /// This prevents false positives from:
    /// - Binary data in query results containing byte 0x5A
    /// - Error messages containing the letter 'Z'
    /// - Parameter data with the 'Z' byte
    pub fn contains_ready_for_query(&self, data: &[u8]) -> bool {
        self.extract_ready_for_query(data).is_some()
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
        let extractor = MessageExtractor::new();

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
        let extractor = MessageExtractor::new();

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

    /// Build an ErrorResponse with severity, SQLSTATE, and a literal-echoing
    /// message field.
    fn build_error_response_with_sqlstate() -> Vec<u8> {
        let mut msg = vec![MSG_ERROR_RESPONSE];
        let mut fields = Vec::new();
        fields.push(b'S');
        fields.extend_from_slice(b"ERROR");
        fields.push(0);
        fields.push(b'C');
        fields.extend_from_slice(b"23505"); // unique_violation
        fields.push(0);
        fields.push(b'M');
        // A real Postgres message frequently echoes the offending literal:
        fields.extend_from_slice(b"duplicate key value violates unique constraint; Key (email)=(bob@example.com) already exists.");
        fields.push(0);
        fields.push(0);
        let length = (fields.len() + 4) as i32;
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&fields);
        msg
    }

    #[test]
    fn test_extract_error_scrubbed_drops_freetext_message() {
        let extractor = MessageExtractor::new();
        let msg = build_error_response_with_sqlstate();

        let scrubbed = extractor.extract_error_scrubbed(&msg).expect("scrubbed error");
        // Must carry severity + SQLSTATE...
        assert!(scrubbed.contains("ERROR"), "scrubbed error should keep severity: {scrubbed}");
        assert!(scrubbed.contains("23505"), "scrubbed error should keep SQLSTATE: {scrubbed}");
        // ...and must NOT leak the free-text message or any literal it echoed.
        assert!(
            !scrubbed.contains("bob@example.com"),
            "scrubbed error must not leak the literal: {scrubbed}"
        );
        assert!(
            !scrubbed.contains("duplicate key"),
            "scrubbed error must not leak the free-text message: {scrubbed}"
        );
    }

    #[test]
    fn test_extract_error_still_captures_sqlstate() {
        let extractor = MessageExtractor::new();
        let msg = build_error_response_with_sqlstate();
        // The full (non-scrubbed) accessor keeps the human-readable message for
        // the non-anonymized path.
        let full = extractor.extract_error(&msg).expect("full error");
        assert!(full.contains("duplicate key"));
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

    #[test]
    fn test_contains_ready_for_query_true() {
        let extractor = MessageExtractor::new();
        // Valid ReadyForQuery message: 'Z' + length(5) + status('I')
        let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I'];
        assert!(extractor.contains_ready_for_query(&msg));
    }

    #[test]
    fn test_contains_ready_for_query_false_for_z_in_data() {
        let extractor = MessageExtractor::new();
        // DataRow containing 'Z' byte in payload - should NOT match
        // DataRow: 'D' + length + column_count + column_data
        let mut msg = vec![MSG_DATA_ROW, 0, 0, 0, 11]; // length = 11
        msg.extend_from_slice(&[0, 1]); // 1 column
        msg.extend_from_slice(&[0, 0, 0, 1]); // column length = 1
        msg.push(b'Z'); // 'Z' as data value, not message type
        assert!(!extractor.contains_ready_for_query(&msg));
    }

    #[test]
    fn test_contains_ready_for_query_in_stream() {
        let extractor = MessageExtractor::new();
        // DataRow + CommandComplete + ReadyForQuery
        let mut msg = vec![];
        // DataRow with 'Z' in data
        msg.extend_from_slice(&[MSG_DATA_ROW, 0, 0, 0, 11]);
        msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'Z']);
        // CommandComplete
        msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 13]);
        msg.extend_from_slice(b"SELECT 1\0");
        // ReadyForQuery
        msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I']);

        assert!(extractor.contains_ready_for_query(&msg));
    }

    #[test]
    fn test_contains_ready_for_query_incomplete_message() {
        let extractor = MessageExtractor::new();
        // Incomplete ReadyForQuery (missing status byte)
        let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5];
        assert!(!extractor.contains_ready_for_query(&msg));
    }

    #[test]
    fn test_is_query_complete_false_for_c_in_data() {
        let extractor = MessageExtractor::new();
        // DataRow containing 'C' byte in payload - should NOT match
        let mut msg = vec![MSG_DATA_ROW, 0, 0, 0, 11];
        msg.extend_from_slice(&[0, 1]); // 1 column
        msg.extend_from_slice(&[0, 0, 0, 1]); // column length = 1
        msg.push(b'C'); // 'C' as data value, not CommandComplete
        assert!(!extractor.is_query_complete(&msg));
    }

    #[test]
    fn test_is_query_complete_true_for_command_complete() {
        let extractor = MessageExtractor::new();
        // Valid CommandComplete message
        let mut msg = vec![MSG_COMMAND_COMPLETE, 0, 0, 0, 13];
        msg.extend_from_slice(b"SELECT 1\0");
        assert!(extractor.is_query_complete(&msg));
    }

    #[test]
    fn test_is_query_complete_true_for_ready_for_query() {
        let extractor = MessageExtractor::new();
        let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I'];
        assert!(extractor.is_query_complete(&msg));
    }

    /// CRIT (WP-9 Task 3): a `Parse` message carrying a `SET` statement, split
    /// across two `extract_messages` calls (simulating two TCP reads), must be
    /// reassembled into a single `Message::Parse` — not silently discarded.
    #[test]
    fn test_extract_messages_reassembles_split_parse() {
        let extractor = MessageExtractor::new();

        // Build a Parse message: 'P' + length + name\0 + query\0 + num_params(0)
        let mut msg = vec![MSG_PARSE];
        let len_pos = msg.len();
        msg.extend_from_slice(&[0, 0, 0, 0]);
        msg.extend_from_slice(b"stmt1");
        msg.push(0);
        msg.extend_from_slice(b"SET search_path TO public");
        msg.push(0);
        msg.extend_from_slice(&0i16.to_be_bytes()); // 0 param types
        let len = (msg.len() - 1) as i32;
        msg[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());

        // Split the message roughly in half, across the header/payload boundary.
        let split = msg.len() / 2;
        let (first, second) = msg.split_at(split);

        let msgs_first = extractor.extract_messages(first);
        assert!(msgs_first.is_empty(), "incomplete message must not be emitted yet");

        let msgs_second = extractor.extract_messages(second);
        assert_eq!(msgs_second.len(), 1, "split Parse must be reassembled on the next call");
        match &msgs_second[0] {
            Message::Parse { name, query, param_oids } => {
                assert_eq!(name, "stmt1");
                assert_eq!(query, "SET search_path TO public");
                assert!(param_oids.is_empty());
            }
            other => panic!("expected Parse, got {other:?}"),
        }
    }

    /// A simple `Query` message split across two `extract_messages` calls must
    /// also be reassembled correctly.
    #[test]
    fn test_extract_messages_reassembles_split_query() {
        let extractor = MessageExtractor::new();

        let query = b"SELECT 1";
        let length = (query.len() + 1 + 4) as i32;
        let mut msg = vec![MSG_QUERY];
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(query);
        msg.push(0);

        let split = 3; // cut inside the 5-byte header
        let (first, second) = msg.split_at(split);

        let msgs_first = extractor.extract_messages(first);
        assert!(msgs_first.is_empty());

        let msgs_second = extractor.extract_messages(second);
        assert_eq!(msgs_second.len(), 1);
        match &msgs_second[0] {
            Message::Query { query } => assert_eq!(query, "SELECT 1"),
            other => panic!("expected Query, got {other:?}"),
        }
    }

    /// A message larger than a typical read buffer, split across MANY calls,
    /// must still be reassembled — reassembly must accumulate across more than
    /// just two reads.
    #[test]
    fn test_extract_messages_reassembles_across_many_small_reads() {
        let extractor = MessageExtractor::new();

        let long_query = "x".repeat(20_000);
        let query_bytes = long_query.as_bytes();
        let length = (query_bytes.len() + 1 + 4) as i32;
        let mut msg = vec![MSG_QUERY];
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(query_bytes);
        msg.push(0);

        let mut all = Vec::new();
        for chunk in msg.chunks(37) {
            all.extend(extractor.extract_messages(chunk));
        }

        assert_eq!(all.len(), 1);
        match &all[0] {
            Message::Query { query } => assert_eq!(query, &long_query),
            other => panic!("expected Query, got {other:?}"),
        }
    }

    /// Two complete messages in one call are both extracted, and any following
    /// incomplete tail is retained for the next call (regression for the
    /// existing multi-message-per-read path).
    #[test]
    fn test_extract_messages_multiple_complete_plus_trailing_partial() {
        let extractor = MessageExtractor::new();

        let mut msg = vec![];
        // First: Sync (1 + 4 = 5 bytes, length=4)
        msg.extend_from_slice(&[MSG_SYNC, 0, 0, 0, 4]);
        // Second: Sync again
        msg.extend_from_slice(&[MSG_SYNC, 0, 0, 0, 4]);
        // Trailing partial: start of a third Sync, header only half written
        msg.extend_from_slice(&[MSG_SYNC, 0, 0]);

        let result = extractor.extract_messages(&msg);
        assert_eq!(result, vec![Message::Sync, Message::Sync]);

        // Complete the trailing partial in a second call.
        let rest = extractor.extract_messages(&[0, 4]);
        assert_eq!(rest, vec![Message::Sync]);
    }

    /// A malformed length field (< 4) must not panic and must not wedge the
    /// buffer — it is treated as a framing error and the buffered bytes are
    /// dropped (fail-closed: less pooling, never corruption of forwarded bytes).
    #[test]
    fn test_extract_messages_malformed_length_does_not_panic_or_wedge() {
        let extractor = MessageExtractor::new();

        // Type byte + a length field of 0 (invalid: must be >= 4).
        let bad = vec![MSG_SYNC, 0, 0, 0, 0];
        let result = extractor.extract_messages(&bad);
        assert!(result.is_empty());

        // A subsequent, well-formed message must parse normally — proving the
        // buffer didn't wedge on garbage.
        let good = extractor.extract_messages(&[MSG_SYNC, 0, 0, 0, 4]);
        assert_eq!(good, vec![Message::Sync]);
    }

    /// An implausibly large framed length must be rejected rather than causing
    /// the reassembly buffer to grow without bound.
    #[test]
    fn test_extract_messages_oversized_length_is_rejected() {
        let extractor = MessageExtractor::new();

        let mut msg = vec![MSG_QUERY];
        // Length far beyond any sane message size.
        msg.extend_from_slice(&(i32::MAX).to_be_bytes());
        msg.extend_from_slice(b"not actually this long");

        let result = extractor.extract_messages(&msg);
        assert!(result.is_empty());

        // Buffer must not have wedged: a subsequent well-formed message parses.
        let good = extractor.extract_messages(&[MSG_SYNC, 0, 0, 0, 4]);
        assert_eq!(good, vec![Message::Sync]);
    }

    #[test]
    fn test_is_query_complete_after_data_rows() {
        let extractor = MessageExtractor::new();
        // DataRow with 'C' in data + actual CommandComplete
        let mut msg = vec![];
        // DataRow containing 'C'
        msg.extend_from_slice(&[MSG_DATA_ROW, 0, 0, 0, 11]);
        msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'C']);
        // CommandComplete
        msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 13]);
        msg.extend_from_slice(b"SELECT 1\0");

        assert!(extractor.is_query_complete(&msg));
    }
}

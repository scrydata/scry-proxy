//! PostgreSQL authentication protocol messages
//!
//! This module provides builders and parsers for PostgreSQL wire protocol
//! authentication messages used during the startup handshake.

use std::collections::HashMap;

/// Parsed startup message from client
#[derive(Debug, Clone)]
pub struct StartupMessage {
    /// Protocol version (major << 16 | minor)
    pub protocol_version: i32,
    /// Connection parameters (user, database, application_name, etc.)
    pub parameters: HashMap<String, String>,
}

impl StartupMessage {
    /// Protocol version for PostgreSQL 3.0
    pub const PROTOCOL_VERSION_3: i32 = 196608; // 3 << 16 | 0

    /// Parse a startup message from raw bytes
    ///
    /// Format: length (4 bytes) + protocol_version (4 bytes) + parameters
    /// Parameters are null-terminated key-value pairs, ending with an extra null
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }

        let length = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < length {
            return None;
        }

        let protocol_version = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);

        // SSL request has protocol version 80877103, skip it
        if protocol_version == 80877103 {
            return None;
        }

        // Parse parameters (null-terminated key-value pairs)
        let mut parameters = HashMap::new();
        let mut offset = 8;

        while offset < length {
            // Read key
            let key_start = offset;
            while offset < length && data[offset] != 0 {
                offset += 1;
            }
            if offset >= length {
                break;
            }
            let key = String::from_utf8_lossy(&data[key_start..offset]).to_string();
            offset += 1; // Skip null terminator

            if key.is_empty() {
                break; // End of parameters
            }

            // Read value
            let value_start = offset;
            while offset < length && data[offset] != 0 {
                offset += 1;
            }
            let value = String::from_utf8_lossy(&data[value_start..offset]).to_string();
            offset += 1; // Skip null terminator

            parameters.insert(key, value);
        }

        Some(Self { protocol_version, parameters })
    }

    /// Get the username from the startup message
    pub fn user(&self) -> Option<&str> {
        self.parameters.get("user").map(|s| s.as_str())
    }

    /// Get the database from the startup message
    pub fn database(&self) -> Option<&str> {
        self.parameters.get("database").map(|s| s.as_str())
    }

    /// Build a new startup message with the given parameters
    pub fn build(user: &str, database: &str, extra_params: &[(&str, &str)]) -> Vec<u8> {
        let mut params = Vec::new();

        // Add user
        params.extend_from_slice(b"user\0");
        params.extend_from_slice(user.as_bytes());
        params.push(0);

        // Add database
        params.extend_from_slice(b"database\0");
        params.extend_from_slice(database.as_bytes());
        params.push(0);

        // Add extra parameters
        for (key, value) in extra_params {
            params.extend_from_slice(key.as_bytes());
            params.push(0);
            params.extend_from_slice(value.as_bytes());
            params.push(0);
        }

        // Final null terminator
        params.push(0);

        // Build message: length + protocol_version + params
        let length = (4 + 4 + params.len()) as i32;
        let mut msg = Vec::with_capacity(length as usize);
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&Self::PROTOCOL_VERSION_3.to_be_bytes());
        msg.extend_from_slice(&params);

        msg
    }
}

/// Parsed authentication request from backend
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthRequest {
    /// AuthenticationOk (auth_code = 0) - authentication successful
    Ok,
    /// AuthenticationCleartextPassword (auth_code = 3)
    CleartextPassword,
    /// AuthenticationMD5Password (auth_code = 5) with 4-byte salt
    Md5 { salt: [u8; 4] },
    /// AuthenticationSASL (auth_code = 10) - SCRAM initiation
    Sasl { mechanisms: Vec<String> },
    /// AuthenticationSASLContinue (auth_code = 11) - server challenge
    SaslContinue { data: Vec<u8> },
    /// AuthenticationSASLFinal (auth_code = 12) - server signature
    SaslFinal { data: Vec<u8> },
}

impl AuthRequest {
    /// Parse an authentication request message from the backend
    ///
    /// Format: 'R' + length (4 bytes) + auth_code (4 bytes) + [payload]
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }

        if data[0] != b'R' {
            return None;
        }

        let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        if data.len() < 1 + length {
            return None;
        }

        let auth_code = i32::from_be_bytes([data[5], data[6], data[7], data[8]]);

        match auth_code {
            0 => Some(AuthRequest::Ok),
            3 => Some(AuthRequest::CleartextPassword),
            5 => {
                if data.len() < 13 {
                    return None;
                }
                let salt = [data[9], data[10], data[11], data[12]];
                Some(AuthRequest::Md5 { salt })
            }
            10 => {
                // SASL: parse null-terminated mechanism names
                let mut mechanisms = Vec::new();
                let mut offset = 9;
                while offset < 1 + length {
                    let start = offset;
                    while offset < 1 + length && data[offset] != 0 {
                        offset += 1;
                    }
                    if start < offset {
                        if let Ok(mech) = std::str::from_utf8(&data[start..offset]) {
                            mechanisms.push(mech.to_string());
                        }
                    }
                    offset += 1; // Skip null
                    if offset < 1 + length && data[offset] == 0 {
                        break; // Double null = end
                    }
                }
                Some(AuthRequest::Sasl { mechanisms })
            }
            11 => {
                let payload = data[9..1 + length].to_vec();
                Some(AuthRequest::SaslContinue { data: payload })
            }
            12 => {
                let payload = data[9..1 + length].to_vec();
                Some(AuthRequest::SaslFinal { data: payload })
            }
            _ => None, // Unsupported auth type
        }
    }
}

/// Build an AuthenticationOk message
///
/// Format: 'R' + length(8) + auth_type(0)
pub fn build_auth_ok() -> Vec<u8> {
    let mut msg = Vec::with_capacity(9);
    msg.push(b'R'); // Authentication message type
    msg.extend_from_slice(&8i32.to_be_bytes()); // length
    msg.extend_from_slice(&0i32.to_be_bytes()); // AuthenticationOk
    msg
}

/// Build an AuthenticationCleartextPassword message
///
/// Format: 'R' + length(8) + auth_type(3)
pub fn build_auth_cleartext_password() -> Vec<u8> {
    let mut msg = Vec::with_capacity(9);
    msg.push(b'R');
    msg.extend_from_slice(&8i32.to_be_bytes());
    msg.extend_from_slice(&3i32.to_be_bytes()); // CleartextPassword
    msg
}

/// Build an AuthenticationMD5Password message
///
/// Format: 'R' + length(12) + auth_type(5) + salt(4 bytes)
pub fn build_auth_md5_password(salt: [u8; 4]) -> Vec<u8> {
    let mut msg = Vec::with_capacity(13);
    msg.push(b'R');
    msg.extend_from_slice(&12i32.to_be_bytes());
    msg.extend_from_slice(&5i32.to_be_bytes()); // MD5Password
    msg.extend_from_slice(&salt);
    msg
}

/// Parse a PasswordMessage from client
///
/// Format: 'p' + length + password (null-terminated)
/// Returns the password string if valid
pub fn parse_password_message(data: &[u8]) -> Option<String> {
    if data.len() < 5 {
        return None;
    }

    if data[0] != b'p' {
        return None;
    }

    let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    if data.len() < 1 + length {
        return None;
    }

    // Password is null-terminated
    let password_data = &data[5..1 + length - 1]; // -1 for null terminator
    String::from_utf8(password_data.to_vec()).ok()
}

/// Build a PasswordMessage to send to the backend
///
/// Format: 'p' + length (4 bytes) + password + null terminator
pub fn build_password_message(password: &str) -> Vec<u8> {
    let length = (4 + password.len() + 1) as i32;
    let mut msg = Vec::with_capacity(1 + length as usize);
    msg.push(b'p');
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(password.as_bytes());
    msg.push(0);
    msg
}

/// Build a SASLInitialResponse message
///
/// Format: 'p' + length + mechanism (null-terminated) + response_length (4 bytes) + response
pub fn build_sasl_initial_response(mechanism: &str, client_first: &[u8]) -> Vec<u8> {
    let mech_bytes = mechanism.as_bytes();
    let content_len = mech_bytes.len() + 1 + 4 + client_first.len();
    let length = (4 + content_len) as i32;

    let mut msg = Vec::with_capacity(1 + length as usize);
    msg.push(b'p');
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(mech_bytes);
    msg.push(0); // Null terminate mechanism
    msg.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    msg.extend_from_slice(client_first);
    msg
}

/// Build a SASLResponse message (for subsequent SCRAM messages)
///
/// Format: 'p' + length + response_data
pub fn build_sasl_response(data: &[u8]) -> Vec<u8> {
    let length = (4 + data.len()) as i32;
    let mut msg = Vec::with_capacity(1 + length as usize);
    msg.push(b'p');
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(data);
    msg
}

/// Build an ErrorResponse message
///
/// Format: 'E' + length + fields + terminator
/// Fields: severity ('S'), code ('C'), message ('M'), each null-terminated
pub fn build_error_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
    let mut fields = Vec::new();

    // Severity field
    fields.push(b'S');
    fields.extend_from_slice(severity.as_bytes());
    fields.push(0);

    // Code field
    fields.push(b'C');
    fields.extend_from_slice(code.as_bytes());
    fields.push(0);

    // Message field
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);

    // Terminator
    fields.push(0);

    let length = (4 + fields.len()) as i32;
    let mut msg = Vec::with_capacity(1 + length as usize);
    msg.push(b'E');
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(&fields);

    msg
}

/// Compute MD5 password hash for PostgreSQL authentication
///
/// PostgreSQL MD5 auth uses: "md5" + md5(md5(password + username) + salt)
pub fn compute_md5_response(password: &str, username: &str, salt: &[u8; 4]) -> String {
    // First hash: md5(password + username)
    let inner = format!("{}{}", password, username);
    let inner_hash = format!("{:x}", md5::compute(inner.as_bytes()));

    // Second hash: md5(inner_hash + salt)
    let mut outer_input = inner_hash.into_bytes();
    outer_input.extend_from_slice(salt);
    let outer_hash = format!("{:x}", md5::compute(&outer_input));

    format!("md5{}", outer_hash)
}

/// Check if an MD5 password response matches
///
/// client_response is the full "md5<hash>" string from the client
pub fn verify_md5_response(
    client_response: &str,
    password: &str,
    username: &str,
    salt: &[u8; 4],
) -> bool {
    let expected = compute_md5_response(password, username, salt);
    client_response == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_startup_message() {
        // Build a startup message
        let msg = StartupMessage::build("testuser", "testdb", &[]);

        let parsed = StartupMessage::parse(&msg).unwrap();
        assert_eq!(parsed.protocol_version, StartupMessage::PROTOCOL_VERSION_3);
        assert_eq!(parsed.user(), Some("testuser"));
        assert_eq!(parsed.database(), Some("testdb"));
    }

    #[test]
    fn test_parse_startup_message_with_extra_params() {
        let msg = StartupMessage::build("testuser", "testdb", &[("application_name", "myapp")]);

        let parsed = StartupMessage::parse(&msg).unwrap();
        assert_eq!(parsed.user(), Some("testuser"));
        assert_eq!(parsed.database(), Some("testdb"));
        assert_eq!(
            parsed.parameters.get("application_name").map(|s| s.as_str()),
            Some("myapp")
        );
    }

    #[test]
    fn test_build_auth_ok() {
        let msg = build_auth_ok();
        assert_eq!(msg.len(), 9);
        assert_eq!(msg[0], b'R');
        let length = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(length, 8);
        let auth_type = i32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]);
        assert_eq!(auth_type, 0);
    }

    #[test]
    fn test_build_auth_md5_password() {
        let salt = [0x01, 0x02, 0x03, 0x04];
        let msg = build_auth_md5_password(salt);
        assert_eq!(msg.len(), 13);
        assert_eq!(msg[0], b'R');
        let length = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(length, 12);
        let auth_type = i32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]);
        assert_eq!(auth_type, 5);
        assert_eq!(&msg[9..13], &salt);
    }

    #[test]
    fn test_parse_password_message() {
        // Build password message: 'p' + length + password + null
        let password = "mypassword";
        let length = (4 + password.len() + 1) as i32;
        let mut msg = Vec::new();
        msg.push(b'p');
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(password.as_bytes());
        msg.push(0);

        let parsed = parse_password_message(&msg).unwrap();
        assert_eq!(parsed, "mypassword");
    }

    #[test]
    fn test_compute_md5_response() {
        // Test vector: user "postgres", password "password", salt [1,2,3,4]
        let salt = [0x01, 0x02, 0x03, 0x04];
        let response = compute_md5_response("password", "postgres", &salt);

        // Verify it starts with "md5" and is the right length
        assert!(response.starts_with("md5"));
        assert_eq!(response.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn test_verify_md5_response() {
        let salt = [0x01, 0x02, 0x03, 0x04];
        let response = compute_md5_response("password", "postgres", &salt);

        assert!(verify_md5_response(&response, "password", "postgres", &salt));
        assert!(!verify_md5_response(&response, "wrong", "postgres", &salt));
        assert!(!verify_md5_response(&response, "password", "other", &salt));
    }

    #[test]
    fn test_build_error_response() {
        let msg = build_error_response("FATAL", "28P01", "password authentication failed");
        assert_eq!(msg[0], b'E');

        // Verify it contains the error message
        let msg_str = String::from_utf8_lossy(&msg);
        assert!(msg_str.contains("FATAL"));
        assert!(msg_str.contains("28P01"));
        assert!(msg_str.contains("password authentication failed"));
    }

    #[test]
    fn test_parse_auth_request_md5() {
        let salt = [0x01, 0x02, 0x03, 0x04];
        let msg = build_auth_md5_password(salt);

        let parsed = AuthRequest::parse(&msg).unwrap();
        assert_eq!(parsed, AuthRequest::Md5 { salt });
    }

    #[test]
    fn test_parse_auth_request_ok() {
        let msg = build_auth_ok();

        let parsed = AuthRequest::parse(&msg).unwrap();
        assert_eq!(parsed, AuthRequest::Ok);
    }

    #[test]
    fn test_parse_auth_request_cleartext() {
        let msg = build_auth_cleartext_password();

        let parsed = AuthRequest::parse(&msg).unwrap();
        assert_eq!(parsed, AuthRequest::CleartextPassword);
    }

    #[test]
    fn test_parse_auth_request_scram() {
        // AuthenticationSASL: 'R' + length + 10 + "SCRAM-SHA-256\0" + "\0"
        let mut msg = vec![b'R'];
        let mechanisms = b"SCRAM-SHA-256\0\0";
        let length = (4 + 4 + mechanisms.len()) as i32;
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&10i32.to_be_bytes()); // AuthenticationSASL
        msg.extend_from_slice(mechanisms);

        let parsed = AuthRequest::parse(&msg).unwrap();
        assert!(matches!(parsed, AuthRequest::Sasl { .. }));
    }

    #[test]
    fn test_build_password_message() {
        let msg = build_password_message("md5abc123");

        assert_eq!(msg[0], b'p');
        let length = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(length as usize, 4 + "md5abc123".len() + 1); // length field + password + null

        // Verify we can parse it back
        let parsed = parse_password_message(&msg).unwrap();
        assert_eq!(parsed, "md5abc123");
    }

    #[test]
    fn test_build_sasl_initial_response() {
        let msg = build_sasl_initial_response("SCRAM-SHA-256", b"n,,n=user,r=nonce123");

        assert_eq!(msg[0], b'p');
        // Should contain mechanism name and client-first-message
        let msg_str = String::from_utf8_lossy(&msg);
        assert!(msg_str.contains("SCRAM-SHA-256"));
    }

    #[test]
    fn test_build_sasl_response() {
        let client_final = b"c=biws,r=nonce,p=proof";
        let msg = build_sasl_response(client_final);

        assert_eq!(msg[0], b'p');
        let length = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(length as usize, 4 + client_final.len());
    }
}

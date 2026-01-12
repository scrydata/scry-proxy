//! SCRAM-SHA-256 client implementation for backend authentication
//!
//! Implements RFC 5802 (SCRAM) and RFC 7677 (SCRAM-SHA-256) for authenticating
//! the proxy to PostgreSQL backends.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256};

/// SCRAM-SHA-256 client state machine
pub struct ScramClient {
    username: String,
    password: String,
    client_nonce: String,
    client_first_bare: String,
    server_first: String,
    salted_password: Option<[u8; 32]>,
}

impl ScramClient {
    /// Create a new SCRAM client for the given credentials
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: generate_nonce(),
            client_first_bare: String::new(),
            server_first: String::new(),
            salted_password: None,
        }
    }

    /// Create client for testing with a fixed nonce
    #[cfg(test)]
    pub fn new_with_nonce(username: &str, password: &str, nonce: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: nonce.to_string(),
            client_first_bare: String::new(),
            server_first: String::new(),
            salted_password: None,
        }
    }

    /// Generate the client-first-message
    pub fn client_first(&mut self) -> Vec<u8> {
        self.client_first_bare = format!("n={},r={}", self.username, self.client_nonce);
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    /// Process server-first-message and generate client-final-message
    pub fn client_final(&mut self, server_first: &[u8]) -> Result<Vec<u8>, ScramError> {
        let server_first_str = std::str::from_utf8(server_first)
            .map_err(|_| ScramError::InvalidServerMessage)?;
        self.server_first = server_first_str.to_string();

        // Parse server-first: r=<nonce>,s=<salt>,i=<iterations>
        let mut server_nonce = None;
        let mut salt_b64 = None;
        let mut iterations = None;

        for part in server_first_str.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                server_nonce = Some(value);
            } else if let Some(value) = part.strip_prefix("s=") {
                salt_b64 = Some(value);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = value.parse().ok();
            }
        }

        let server_nonce = server_nonce.ok_or(ScramError::MissingNonce)?;
        let salt_b64 = salt_b64.ok_or(ScramError::MissingSalt)?;
        let iterations = iterations.ok_or(ScramError::MissingIterations)?;

        // Verify server nonce starts with our client nonce
        if !server_nonce.starts_with(&self.client_nonce) {
            return Err(ScramError::InvalidNonce);
        }

        // Decode salt
        let salt = BASE64.decode(salt_b64).map_err(|_| ScramError::InvalidSalt)?;

        // Compute SaltedPassword
        let mut salted_password = [0u8; 32];
        pbkdf2_hmac::<Sha256>(
            self.password.as_bytes(),
            &salt,
            iterations,
            &mut salted_password,
        );
        self.salted_password = Some(salted_password);

        // Build client-final-message-without-proof
        let client_final_without_proof = format!("c=biws,r={}", server_nonce);

        // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof
        );

        // Compute proof
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof = xor_bytes(&client_key, &client_signature);

        let proof_b64 = BASE64.encode(&client_proof);
        let client_final = format!("{},p={}", client_final_without_proof, proof_b64);

        Ok(client_final.into_bytes())
    }

    /// Verify the server-final-message
    pub fn verify_server_final(&self, server_final: &[u8]) -> Result<(), ScramError> {
        let server_final_str = std::str::from_utf8(server_final)
            .map_err(|_| ScramError::InvalidServerMessage)?;

        let server_sig_b64 = server_final_str
            .strip_prefix("v=")
            .ok_or(ScramError::InvalidServerSignature)?;

        let server_sig = BASE64.decode(server_sig_b64)
            .map_err(|_| ScramError::InvalidServerSignature)?;

        let salted_password = self.salted_password.ok_or(ScramError::NotInitialized)?;

        // Compute expected server signature
        let server_key = hmac_sha256(&salted_password, b"Server Key");

        // Rebuild full nonce from server_first
        let server_nonce = self.server_first
            .split(',')
            .find(|p| p.starts_with("r="))
            .and_then(|p| p.strip_prefix("r="))
            .ok_or(ScramError::MissingNonce)?;

        let client_final_without_proof = format!("c=biws,r={}", server_nonce);

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof
        );

        let expected_sig = hmac_sha256(&server_key, auth_message.as_bytes());

        if server_sig != expected_sig {
            return Err(ScramError::ServerSignatureMismatch);
        }

        Ok(())
    }
}

/// SCRAM authentication errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramError {
    InvalidServerMessage,
    MissingNonce,
    MissingSalt,
    MissingIterations,
    InvalidNonce,
    InvalidSalt,
    NotInitialized,
    InvalidServerSignature,
    ServerSignatureMismatch,
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScramError::InvalidServerMessage => write!(f, "Invalid server message"),
            ScramError::MissingNonce => write!(f, "Missing nonce in server message"),
            ScramError::MissingSalt => write!(f, "Missing salt in server message"),
            ScramError::MissingIterations => write!(f, "Missing iterations in server message"),
            ScramError::InvalidNonce => write!(f, "Server nonce doesn't match client nonce"),
            ScramError::InvalidSalt => write!(f, "Invalid salt encoding"),
            ScramError::NotInitialized => write!(f, "SCRAM client not initialized"),
            ScramError::InvalidServerSignature => write!(f, "Invalid server signature format"),
            ScramError::ServerSignatureMismatch => write!(f, "Server signature verification failed"),
        }
    }
}

impl std::error::Error for ScramError {}

// Helper functions

fn generate_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 18] = rng.gen();
    BASE64.encode(bytes)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC key size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

fn xor_bytes(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut result = [0u8; 32];
    for i in 0..32 {
        result[i] = a[i] ^ b[i];
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_first_message_format() {
        let mut client = ScramClient::new_with_nonce("user", "password", "rOprNGfwEbeRWgbNEkqO");
        let msg = client.client_first();
        let msg_str = String::from_utf8(msg).unwrap();

        assert!(msg_str.starts_with("n,,"));
        assert!(msg_str.contains("n=user"));
        assert!(msg_str.contains("r=rOprNGfwEbeRWgbNEkqO"));
    }

    #[test]
    fn test_client_final_message() {
        let mut client = ScramClient::new_with_nonce("user", "pencil", "rOprNGfwEbeRWgbNEkqO");
        client.client_first();

        // RFC 5802 test vector (adapted)
        let server_first = b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

        let result = client.client_final(server_first);
        assert!(result.is_ok());

        let client_final = String::from_utf8(result.unwrap()).unwrap();
        assert!(client_final.starts_with("c=biws,r="));
        assert!(client_final.contains(",p="));
    }

    #[test]
    fn test_invalid_server_nonce() {
        let mut client = ScramClient::new_with_nonce("user", "password", "clientnonce");
        client.client_first();

        // Server nonce doesn't start with client nonce
        let server_first = b"r=differentnonce,s=c2FsdA==,i=4096";

        let result = client.client_final(server_first);
        assert_eq!(result.unwrap_err(), ScramError::InvalidNonce);
    }

    #[test]
    fn test_missing_iterations() {
        let mut client = ScramClient::new_with_nonce("user", "password", "nonce");
        client.client_first();

        let server_first = b"r=nonce123,s=c2FsdA==";

        let result = client.client_final(server_first);
        assert_eq!(result.unwrap_err(), ScramError::MissingIterations);
    }
}

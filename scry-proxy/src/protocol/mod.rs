mod extractor;
mod anonymize;
mod command_detector;
pub mod bind;
pub mod traits;
pub mod postgres;

pub use bind::decode_params;
pub use extractor::MessageExtractor;
pub use anonymize::{QueryAnonymizer, AnonymizedQuery};
pub use command_detector::{CommandDetector, DetectedCommand};
pub use traits::{Protocol, ProtocolConfig, ProtocolRegistry};
// Message enum is defined below and doesn't need re-export

// Postgres wire protocol message types
pub const MSG_QUERY: u8 = b'Q';
pub const MSG_PARSE: u8 = b'P';
pub const MSG_BIND: u8 = b'B';
pub const MSG_EXECUTE: u8 = b'E';
pub const MSG_DESCRIBE: u8 = b'D';
pub const MSG_SYNC: u8 = b'S';
pub const MSG_TERMINATE: u8 = b'X';
pub const MSG_CLOSE: u8 = b'C';

// Backend message types
pub const MSG_COMMAND_COMPLETE: u8 = b'C';
pub const MSG_READY_FOR_QUERY: u8 = b'Z';
pub const MSG_ERROR_RESPONSE: u8 = b'E';
pub const MSG_ROW_DESCRIPTION: u8 = b'T';
pub const MSG_DATA_ROW: u8 = b'D';

/// Parsed PostgreSQL wire protocol message
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// Simple query protocol (Q)
    Query { query: String },

    /// Parse message - extended query protocol (P)
    Parse {
        name: String,
        query: String,
        param_oids: Vec<u32>,
    },

    /// Bind message - extended query protocol (B)
    Bind {
        portal: String,
        statement: String,
        format_codes: Vec<i16>,
        params_raw: Vec<Option<Vec<u8>>>,
    },

    /// Execute message (E)
    Execute { portal: String },

    /// Close message (C for frontend)
    Close {
        kind: char, // 'S' for statement, 'P' for portal
        name: String,
    },

    /// Sync message (S)
    Sync,

    /// Terminate message (X)
    Terminate,
}

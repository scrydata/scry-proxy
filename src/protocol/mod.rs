mod extractor;
mod anonymize;
pub mod traits;
pub mod postgres;

pub use extractor::MessageExtractor;
pub use anonymize::{QueryAnonymizer, AnonymizedQuery};
pub use traits::{Protocol, ProtocolConfig, ProtocolRegistry};

// Postgres wire protocol message types
pub const MSG_QUERY: u8 = b'Q';
pub const MSG_PARSE: u8 = b'P';
pub const MSG_BIND: u8 = b'B';
pub const MSG_EXECUTE: u8 = b'E';
pub const MSG_DESCRIBE: u8 = b'D';
pub const MSG_SYNC: u8 = b'S';
pub const MSG_TERMINATE: u8 = b'X';

// Backend message types
pub const MSG_COMMAND_COMPLETE: u8 = b'C';
pub const MSG_READY_FOR_QUERY: u8 = b'Z';
pub const MSG_ERROR_RESPONSE: u8 = b'E';
pub const MSG_ROW_DESCRIPTION: u8 = b'T';
pub const MSG_DATA_ROW: u8 = b'D';

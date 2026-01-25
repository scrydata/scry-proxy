//! Admin console response formatting
//!
//! Converts admin command results to PostgreSQL wire protocol messages.

/// Admin command response
#[derive(Debug, Clone)]
pub enum AdminResponse {
    /// Row set result (for SHOW commands)
    RowSet { columns: Vec<String>, rows: Vec<Vec<String>> },
    /// Command completion (for PAUSE, RESUME, etc.)
    CommandComplete { tag: String },
    /// Error response
    Error { message: String },
}

impl AdminResponse {
    /// Convert the response to PostgreSQL wire protocol bytes
    pub fn to_wire(&self) -> Vec<u8> {
        match self {
            AdminResponse::RowSet { columns, rows } => {
                let mut result = Vec::new();

                // RowDescription message
                result.extend_from_slice(&build_row_description(columns));

                // DataRow messages
                for row in rows {
                    result.extend_from_slice(&build_data_row(row));
                }

                // CommandComplete message
                let tag = format!("SELECT {}", rows.len());
                result.extend_from_slice(&build_command_complete(&tag));

                // ReadyForQuery message
                result.extend_from_slice(&build_ready_for_query());

                result
            }
            AdminResponse::CommandComplete { tag } => {
                let mut result = Vec::new();
                result.extend_from_slice(&build_command_complete(tag));
                result.extend_from_slice(&build_ready_for_query());
                result
            }
            AdminResponse::Error { message } => {
                let mut result = Vec::new();
                result.extend_from_slice(&build_error_response(message));
                result.extend_from_slice(&build_ready_for_query());
                result
            }
        }
    }
}

/// Build a RowDescription message
///
/// Format: 'T' + length + field_count + fields
/// Each field: name (null-terminated) + table_oid(4) + column_attr(2) + type_oid(4) + type_size(2) + type_modifier(4) + format(2)
fn build_row_description(columns: &[String]) -> Vec<u8> {
    let mut fields = Vec::new();

    for col in columns {
        // Column name (null-terminated)
        fields.extend_from_slice(col.as_bytes());
        fields.push(0);

        // table_oid (0 = not from a table)
        fields.extend_from_slice(&0i32.to_be_bytes());
        // column_attr (0 = not from a table)
        fields.extend_from_slice(&0i16.to_be_bytes());
        // type_oid (25 = text)
        fields.extend_from_slice(&25i32.to_be_bytes());
        // type_size (-1 = variable length)
        fields.extend_from_slice(&(-1i16).to_be_bytes());
        // type_modifier (-1 = no modifier)
        fields.extend_from_slice(&(-1i32).to_be_bytes());
        // format (0 = text)
        fields.extend_from_slice(&0i16.to_be_bytes());
    }

    let field_count = columns.len() as i16;
    let length = (4 + 2 + fields.len()) as i32;

    let mut msg = Vec::with_capacity(1 + length as usize);
    msg.push(b'T'); // RowDescription
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(&field_count.to_be_bytes());
    msg.extend_from_slice(&fields);

    msg
}

/// Build a DataRow message
///
/// Format: 'D' + length + column_count + columns
/// Each column: length(4) + data (or -1 for NULL)
fn build_data_row(values: &[String]) -> Vec<u8> {
    let mut columns = Vec::new();

    for val in values {
        let bytes = val.as_bytes();
        columns.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        columns.extend_from_slice(bytes);
    }

    let column_count = values.len() as i16;
    let length = (4 + 2 + columns.len()) as i32;

    let mut msg = Vec::with_capacity(1 + length as usize);
    msg.push(b'D'); // DataRow
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(&column_count.to_be_bytes());
    msg.extend_from_slice(&columns);

    msg
}

/// Build a CommandComplete message
///
/// Format: 'C' + length + tag (null-terminated)
fn build_command_complete(tag: &str) -> Vec<u8> {
    let length = (4 + tag.len() + 1) as i32;

    let mut msg = Vec::with_capacity(1 + length as usize);
    msg.push(b'C'); // CommandComplete
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(tag.as_bytes());
    msg.push(0);

    msg
}

/// Build a ReadyForQuery message
///
/// Format: 'Z' + length(5) + status
fn build_ready_for_query() -> Vec<u8> {
    vec![b'Z', 0, 0, 0, 5, b'I'] // I = idle
}

/// Build an ErrorResponse message
///
/// Format: 'E' + length + fields + terminator
fn build_error_response(message: &str) -> Vec<u8> {
    let mut fields = Vec::new();

    // Severity field
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR");
    fields.push(0);

    // SQLSTATE code
    fields.push(b'C');
    fields.extend_from_slice(b"42000"); // Syntax error or access rule violation
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_row_description() {
        let columns = vec!["name".to_string(), "value".to_string()];
        let msg = build_row_description(&columns);

        assert_eq!(msg[0], b'T');

        // Parse the message to verify structure
        let length = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert!(length > 0);

        let field_count = i16::from_be_bytes([msg[5], msg[6]]);
        assert_eq!(field_count, 2);
    }

    #[test]
    fn test_build_data_row() {
        let values = vec!["hello".to_string(), "world".to_string()];
        let msg = build_data_row(&values);

        assert_eq!(msg[0], b'D');

        let length = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert!(length > 0);

        let column_count = i16::from_be_bytes([msg[5], msg[6]]);
        assert_eq!(column_count, 2);
    }

    #[test]
    fn test_build_command_complete() {
        let msg = build_command_complete("SELECT 5");

        assert_eq!(msg[0], b'C');

        // Check it contains the tag
        let content = String::from_utf8_lossy(&msg[5..]);
        assert!(content.contains("SELECT 5"));
    }

    #[test]
    fn test_build_ready_for_query() {
        let msg = build_ready_for_query();

        assert_eq!(msg.len(), 6);
        assert_eq!(msg[0], b'Z');
        assert_eq!(msg[5], b'I'); // Idle status
    }

    #[test]
    fn test_admin_response_to_wire_rowset() {
        let response = AdminResponse::RowSet {
            columns: vec!["col1".to_string()],
            rows: vec![vec!["value1".to_string()]],
        };

        let wire = response.to_wire();

        // Should contain: RowDescription + DataRow + CommandComplete + ReadyForQuery
        assert!(wire.iter().any(|&b| b == b'T')); // RowDescription
        assert!(wire.iter().any(|&b| b == b'D')); // DataRow
        assert!(wire.iter().any(|&b| b == b'C')); // CommandComplete
        assert!(wire.iter().any(|&b| b == b'Z')); // ReadyForQuery
    }

    #[test]
    fn test_admin_response_to_wire_command_complete() {
        let response = AdminResponse::CommandComplete { tag: "PAUSE".to_string() };

        let wire = response.to_wire();

        // Should contain: CommandComplete + ReadyForQuery
        assert!(wire.iter().any(|&b| b == b'C')); // CommandComplete
        assert!(wire.iter().any(|&b| b == b'Z')); // ReadyForQuery

        let content = String::from_utf8_lossy(&wire);
        assert!(content.contains("PAUSE"));
    }

    #[test]
    fn test_admin_response_to_wire_error() {
        let response = AdminResponse::Error { message: "Unknown command".to_string() };

        let wire = response.to_wire();

        // Should contain: ErrorResponse + ReadyForQuery
        assert!(wire.iter().any(|&b| b == b'E')); // ErrorResponse
        assert!(wire.iter().any(|&b| b == b'Z')); // ReadyForQuery
    }
}

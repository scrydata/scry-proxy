//! Parameter decoding from PostgreSQL Bind messages.

use scry_protocol::ParamValue;
use tracing::warn;

/// Well-known PostgreSQL type OIDs
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSON: u32 = 114;
    pub const JSONB: u32 = 3802;
}

/// Decode raw parameter bytes into typed ParamValue.
///
/// # Arguments
/// * `params_raw` - Raw parameter bytes from Bind message (None = NULL)
/// * `format_codes` - Format codes (0 = text, 1 = binary)
/// * `param_oids` - Type OIDs from Parse message
///
/// Returns decoded parameters. Uses `ParamValue::Unknown` for unrecognized types.
pub fn decode_params(
    params_raw: &[Option<Vec<u8>>],
    format_codes: &[i16],
    param_oids: &[u32],
) -> Vec<ParamValue> {
    params_raw
        .iter()
        .enumerate()
        .map(|(i, raw)| {
            match raw {
                None => ParamValue::Null,
                Some(data) => {
                    let oid = param_oids.get(i).copied().unwrap_or(0);
                    let format = get_format_code(format_codes, i);
                    decode_param(data, oid, format)
                }
            }
        })
        .collect()
}

/// Get format code for parameter index.
/// If 0 format codes: all text
/// If 1 format code: all same
/// Otherwise: per-parameter
fn get_format_code(format_codes: &[i16], index: usize) -> i16 {
    match format_codes.len() {
        0 => 0, // All text
        1 => format_codes[0],
        _ => format_codes.get(index).copied().unwrap_or(0),
    }
}

/// Decode a single parameter value.
fn decode_param(data: &[u8], oid: u32, format: i16) -> ParamValue {
    if format == 0 {
        // Text format
        decode_text_param(data, oid)
    } else {
        // Binary format
        decode_binary_param(data, oid)
    }
}

fn decode_text_param(data: &[u8], oid: u32) -> ParamValue {
    let text = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return ParamValue::Bytes(data.to_vec()),
    };

    match oid {
        oid::BOOL => {
            match text {
                "t" | "true" | "TRUE" | "1" => ParamValue::Bool(true),
                "f" | "false" | "FALSE" | "0" => ParamValue::Bool(false),
                _ => ParamValue::Text(text.to_string()),
            }
        }
        oid::INT2 => {
            text.parse::<i16>()
                .map(ParamValue::Int16)
                .unwrap_or_else(|_| ParamValue::Text(text.to_string()))
        }
        oid::INT4 | oid::OID => {
            text.parse::<i32>()
                .map(ParamValue::Int32)
                .unwrap_or_else(|_| ParamValue::Text(text.to_string()))
        }
        oid::INT8 => {
            text.parse::<i64>()
                .map(ParamValue::Int64)
                .unwrap_or_else(|_| ParamValue::Text(text.to_string()))
        }
        oid::FLOAT4 => {
            text.parse::<f32>()
                .map(ParamValue::Float32)
                .unwrap_or_else(|_| ParamValue::Text(text.to_string()))
        }
        oid::FLOAT8 => {
            text.parse::<f64>()
                .map(ParamValue::Float64)
                .unwrap_or_else(|_| ParamValue::Text(text.to_string()))
        }
        oid::NUMERIC => ParamValue::Numeric(text.to_string()),
        oid::TEXT | oid::VARCHAR => ParamValue::Text(text.to_string()),
        oid::JSON | oid::JSONB => ParamValue::Json(text.to_string()),
        oid::UUID => {
            // Parse UUID from text format
            let hex: String = text.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            if hex.len() == 32 {
                let mut bytes = [0u8; 16];
                for i in 0..16 {
                    if let Ok(b) = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16) {
                        bytes[i] = b;
                    }
                }
                ParamValue::Uuid(bytes)
            } else {
                ParamValue::Text(text.to_string())
            }
        }
        oid::BYTEA => {
            // Text format bytea is hex encoded with \x prefix
            if text.starts_with("\\x") {
                let hex = &text[2..];
                let bytes: Result<Vec<u8>, _> = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
                    .collect();
                bytes.map(ParamValue::Bytes).unwrap_or_else(|_| ParamValue::Text(text.to_string()))
            } else {
                ParamValue::Text(text.to_string())
            }
        }
        _ => {
            // Unknown OID - try to preserve as text
            ParamValue::Text(text.to_string())
        }
    }
}

fn decode_binary_param(data: &[u8], oid: u32) -> ParamValue {
    match oid {
        oid::BOOL => {
            if data.len() == 1 {
                ParamValue::Bool(data[0] != 0)
            } else {
                ParamValue::Unknown { oid, data: data.to_vec() }
            }
        }
        oid::INT2 => {
            if data.len() == 2 {
                ParamValue::Int16(i16::from_be_bytes([data[0], data[1]]))
            } else {
                ParamValue::Unknown { oid, data: data.to_vec() }
            }
        }
        oid::INT4 | oid::OID => {
            if data.len() == 4 {
                ParamValue::Int32(i32::from_be_bytes([data[0], data[1], data[2], data[3]]))
            } else {
                ParamValue::Unknown { oid, data: data.to_vec() }
            }
        }
        oid::INT8 => {
            if data.len() == 8 {
                ParamValue::Int64(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                    data[4], data[5], data[6], data[7],
                ]))
            } else {
                ParamValue::Unknown { oid, data: data.to_vec() }
            }
        }
        oid::FLOAT4 => {
            if data.len() == 4 {
                ParamValue::Float32(f32::from_be_bytes([data[0], data[1], data[2], data[3]]))
            } else {
                ParamValue::Unknown { oid, data: data.to_vec() }
            }
        }
        oid::FLOAT8 => {
            if data.len() == 8 {
                ParamValue::Float64(f64::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                    data[4], data[5], data[6], data[7],
                ]))
            } else {
                ParamValue::Unknown { oid, data: data.to_vec() }
            }
        }
        oid::UUID => {
            if data.len() == 16 {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(data);
                ParamValue::Uuid(bytes)
            } else {
                ParamValue::Unknown { oid, data: data.to_vec() }
            }
        }
        oid::TEXT | oid::VARCHAR => {
            String::from_utf8(data.to_vec())
                .map(ParamValue::Text)
                .unwrap_or_else(|_| ParamValue::Bytes(data.to_vec()))
        }
        oid::BYTEA => ParamValue::Bytes(data.to_vec()),
        oid::JSON | oid::JSONB => {
            // JSONB has 1-byte version prefix in binary
            let json_data = if oid == oid::JSONB && !data.is_empty() {
                &data[1..]
            } else {
                data
            };
            String::from_utf8(json_data.to_vec())
                .map(ParamValue::Json)
                .unwrap_or_else(|_| ParamValue::Unknown { oid, data: data.to_vec() })
        }
        _ => {
            warn!(oid = oid, len = data.len(), "Unknown binary parameter type");
            ParamValue::Unknown { oid, data: data.to_vec() }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_text_int32() {
        let params = decode_params(
            &[Some(b"42".to_vec())],
            &[0], // text format
            &[oid::INT4],
        );
        assert_eq!(params, vec![ParamValue::Int32(42)]);
    }

    #[test]
    fn test_decode_text_bool() {
        let params = decode_params(
            &[Some(b"t".to_vec()), Some(b"f".to_vec())],
            &[0],
            &[oid::BOOL, oid::BOOL],
        );
        assert_eq!(params, vec![ParamValue::Bool(true), ParamValue::Bool(false)]);
    }

    #[test]
    fn test_decode_null() {
        let params = decode_params(
            &[None],
            &[0],
            &[oid::INT4],
        );
        assert_eq!(params, vec![ParamValue::Null]);
    }

    #[test]
    fn test_decode_binary_int32() {
        let params = decode_params(
            &[Some(42i32.to_be_bytes().to_vec())],
            &[1], // binary format
            &[oid::INT4],
        );
        assert_eq!(params, vec![ParamValue::Int32(42)]);
    }

    #[test]
    fn test_decode_text_uuid() {
        let params = decode_params(
            &[Some(b"550e8400-e29b-41d4-a716-446655440000".to_vec())],
            &[0],
            &[oid::UUID],
        );
        if let ParamValue::Uuid(bytes) = &params[0] {
            assert_eq!(bytes[0], 0x55);
            assert_eq!(bytes[15], 0x00);
        } else {
            panic!("Expected UUID");
        }
    }

    #[test]
    fn test_decode_unknown_oid() {
        let params = decode_params(
            &[Some(b"some data".to_vec())],
            &[0],
            &[99999], // Unknown OID
        );
        // Should fall back to Text
        assert_eq!(params, vec![ParamValue::Text("some data".to_string())]);
    }

    #[test]
    fn test_format_code_handling() {
        // 0 format codes = all text
        assert_eq!(get_format_code(&[], 0), 0);
        assert_eq!(get_format_code(&[], 5), 0);

        // 1 format code = all same
        assert_eq!(get_format_code(&[1], 0), 1);
        assert_eq!(get_format_code(&[1], 5), 1);

        // Multiple = per-param
        assert_eq!(get_format_code(&[0, 1, 0], 0), 0);
        assert_eq!(get_format_code(&[0, 1, 0], 1), 1);
        assert_eq!(get_format_code(&[0, 1, 0], 2), 0);
    }
}

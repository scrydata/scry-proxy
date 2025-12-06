use super::QueryEvent;
use flexbuffers::FlexbufferSerializer;
use serde::Serialize;
use std::time::SystemTime;

/// Serializes a batch of QueryEvents to FlexBuffers format
///
/// FlexBuffers is a schema-less binary format from the FlatBuffers project
/// that provides efficient serialization without requiring code generation.
///
/// It's ideal for our use case: high performance, compact, and works with serde.
pub struct FlatBuffersSerializer;

#[derive(Serialize)]
struct QueryEventBatch<'a> {
    events: &'a [SerializableEvent<'a>],
    proxy_id: &'a str,
    batch_seq: u64,
}

#[derive(Serialize)]
struct SerializableEvent<'a> {
    event_id: &'a str,
    timestamp_us: u64,
    query: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    normalized_query: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_fingerprints: Option<&'a [String]>,
    duration_us: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<u64>,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    database: &'a str,
    connection_id: &'a str,
}

impl FlatBuffersSerializer {
    /// Serialize a batch of events to FlexBuffers binary format
    ///
    /// Returns the serialized bytes ready to send over the wire
    pub fn serialize_batch(events: &[QueryEvent], proxy_id: &str, batch_seq: u64) -> Vec<u8> {
        // Convert QueryEvent to SerializableEvent
        let serializable_events: Vec<SerializableEvent> = events
            .iter()
            .map(|event| Self::to_serializable(event))
            .collect();

        let batch = QueryEventBatch {
            events: &serializable_events,
            proxy_id,
            batch_seq,
        };

        // Serialize to FlexBuffers
        let mut serializer = FlexbufferSerializer::new();
        batch.serialize(&mut serializer).expect("FlexBuffers serialization should not fail");
        serializer.view().to_vec()
    }

    fn to_serializable(event: &QueryEvent) -> SerializableEvent<'_> {
        // Convert timestamp to microseconds
        let timestamp_us = event
            .timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Convert duration to microseconds
        let duration_us = event.duration.as_micros() as u64;

        SerializableEvent {
            event_id: &event.event_id,
            timestamp_us,
            query: &event.query,
            normalized_query: event.normalized_query.as_deref(),
            value_fingerprints: event.value_fingerprints.as_deref(),
            duration_us,
            rows: event.rows,
            success: event.success,
            error: event.error.as_deref(),
            database: &event.database,
            connection_id: &event.connection_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::QueryEventBuilder;
    use std::time::Duration;

    #[test]
    fn test_serialize_single_event() {
        let event = QueryEventBuilder::new("SELECT 1")
            .connection_id("conn-123")
            .database("testdb")
            .duration(Duration::from_millis(5))
            .build();

        let bytes = FlatBuffersSerializer::serialize_batch(&[event], "proxy-1", 42);

        // Verify we got some bytes
        assert!(!bytes.is_empty());

        // Basic sanity check - FlatBuffers has a file identifier at the start
        assert!(bytes.len() > 4);
    }

    #[test]
    fn test_serialize_batch() {
        let events = vec![
            QueryEventBuilder::new("SELECT 1")
                .connection_id("conn-1")
                .database("db1")
                .duration(Duration::from_millis(5))
                .build(),
            QueryEventBuilder::new("SELECT 2")
                .connection_id("conn-2")
                .database("db2")
                .duration(Duration::from_millis(10))
                .build(),
        ];

        let bytes = FlatBuffersSerializer::serialize_batch(&events, "proxy-1", 1);

        assert!(!bytes.is_empty());
        // Batch should be larger than single event
        assert!(bytes.len() > 100);
    }

    #[test]
    fn test_serialize_with_anonymization() {
        let event = QueryEventBuilder::new("SELECT * FROM users WHERE id = ?")
            .normalized_query("SELECT * FROM users WHERE id = ?")
            .value_fingerprints(vec!["abc123hash".to_string()])
            .connection_id("conn-1")
            .database("db1")
            .duration(Duration::from_millis(5))
            .build();

        let bytes = FlatBuffersSerializer::serialize_batch(&[event], "proxy-1", 1);

        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_serialize_with_error() {
        let event = QueryEventBuilder::new("INVALID SQL")
            .connection_id("conn-1")
            .database("db1")
            .duration(Duration::from_millis(1))
            .success(false)
            .error("syntax error")
            .build();

        let bytes = FlatBuffersSerializer::serialize_batch(&[event], "proxy-1", 1);

        assert!(!bytes.is_empty());
    }
}

# HTTP Event Publisher

## Overview

The HTTP Event Publisher sends batches of query events to a central analytics service using **FlexBuffers** serialization format over HTTP.

## Features

- ✅ **FlexBuffers Serialization**: Compact, efficient binary format (part of FlatBuffers project)
- ✅ **Non-blocking**: Best-effort delivery, never blocks the proxy
- ✅ **Retry Logic**: Exponential backoff with configurable max retries
- ✅ **Timeout Protection**: Configurable request timeout
- ✅ **Optional Compression**: gzip compression support
- ✅ **API Key Authentication**: Optional X-API-Key header support
- ✅ **Metrics**: Tracks successful/failed publishes, bytes sent, etc.
- ✅ **Proxy ID**: Automatic unique instance identification

## Configuration

### Using TOML Config File

```toml
[publisher]
enabled = true
batch_size = 100
flush_interval_ms = 1000
anonymize = true

# Publisher type: "debug" or "http"
publisher_type = "http"

# Memory safety: max events to queue before dropping oldest
# Prevents unbounded memory growth if publisher is slow/down
max_queue_size = 10000  # ~1MB of events (100 bytes/event avg)

# HTTP publisher settings
http_endpoint = "https://analytics.example.com/events"
http_timeout_ms = 500
http_max_retries = 2
http_api_key = "your-secret-key-here"  # Optional
http_compression = true
```

### Using Environment Variables

```bash
# Publisher settings
export SCRY_PUBLISHER_ENABLED=true
export SCRY_PUBLISHER_BATCH_SIZE=100
export SCRY_PUBLISHER_FLUSH_INTERVAL_MS=1000
export SCRY_PUBLISHER_ANONYMIZE=true

# HTTP publisher settings
export SCRY_PUBLISHER_TYPE=http
export SCRY_PUBLISHER_HTTP_ENDPOINT=https://analytics.example.com/events
export SCRY_PUBLISHER_HTTP_TIMEOUT_MS=500
export SCRY_PUBLISHER_HTTP_MAX_RETRIES=2
export SCRY_PUBLISHER_HTTP_API_KEY=your-secret-key-here
export SCRY_PUBLISHER_HTTP_COMPRESSION=true
```

## FlexBuffers Wire Format

### What is FlexBuffers?

FlexBuffers is a schema-less binary format from the FlatBuffers project that:
- Works with serde (no code generation needed)
- Provides compact, efficient serialization
- Supports zero-copy deserialization
- Much faster and smaller than JSON

### Event Batch Structure

```rust
struct QueryEventBatch {
    events: Vec<QueryEvent>,
    proxy_id: String,      // Unique proxy instance ID
    batch_seq: u64,        // Batch sequence number
}

struct QueryEvent {
    event_id: String,
    timestamp_us: u64,
    query: String,
    normalized_query: Option<String>,
    value_fingerprints: Option<Vec<String>>,
    duration_us: u64,
    rows: Option<u64>,
    success: bool,
    error: Option<String>,
    database: String,
    connection_id: String,
}
```

### HTTP Request Format

```http
POST /events HTTP/1.1
Host: analytics.example.com
Content-Type: application/x-flexbuffer
X-API-Key: your-secret-key-here
Content-Length: <size>

<FlexBuffers binary data>
```

## Implementing the Analytics Server

### Rust Example (using flexbuffers crate)

```rust
use axum::{extract::Bytes, http::StatusCode, routing::post, Router};
use flexbuffers::Reader;
use serde::Deserialize;

#[derive(Deserialize)]
struct QueryEventBatch {
    events: Vec<QueryEvent>,
    proxy_id: String,
    batch_seq: u64,
}

#[derive(Deserialize)]
struct QueryEvent {
    event_id: String,
    timestamp_us: u64,
    query: String,
    normalized_query: Option<String>,
    value_fingerprints: Option<Vec<String>>,
    duration_us: u64,
    rows: Option<u64>,
    success: bool,
    error: Option<String>,
    database: String,
    connection_id: String,
}

async fn handle_events(body: Bytes) -> StatusCode {
    // Deserialize FlexBuffers
    let reader = Reader::get_root(&body).unwrap();
    let batch: QueryEventBatch = QueryEventBatch::deserialize(reader).unwrap();

    println!("Received {} events from {}", batch.events.len(), batch.proxy_id);

    // Process events...
    for event in batch.events {
        println!("Query: {}", event.query);
        if let Some(normalized) = event.normalized_query {
            println!("  Normalized: {}", normalized);
        }
        if let Some(fingerprints) = event.value_fingerprints {
            println!("  Value fingerprints: {:?}", fingerprints);
        }
    }

    StatusCode::OK
}

#[tokio::main]
async fn main() {
    let app = Router::new().route("/events", post(handle_events));

    axum::Server::bind(&"0.0.0.0:8080".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
```

### Python Example (using flexbuffers)

```python
from flask import Flask, request
import flexbuffers

app = Flask(__name__)

@app.route('/events', methods=['POST'])
def handle_events():
    # Deserialize FlexBuffers
    data = flexbuffers.Loads(request.data)

    print(f"Received {len(data['events'])} events from {data['proxy_id']}")

    # Process events
    for event in data['events']:
        print(f"Query: {event['query']}")
        if 'normalized_query' in event:
            print(f"  Normalized: {event['normalized_query']}")
        if 'value_fingerprints' in event:
            print(f"  Value fingerprints: {event['value_fingerprints']}")

    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
```

## Performance Characteristics

### Serialization Overhead

FlexBuffers provides excellent performance:
- **~2-5x faster** than JSON serialization
- **~40-60% smaller** payload size than JSON
- **Zero-copy deserialization** on the receiver side

### Retry Behavior

- **Initial backoff**: 50ms
- **Exponential backoff**: 2x each retry (capped at 5 seconds)
- **Max retries**: Configurable (default: 2)
- **Timeout**: Configurable (default: 500ms)

Example retry sequence:
1. Initial attempt fails
2. Wait 50ms, retry (attempt 2)
3. Wait 100ms, retry (attempt 3)
4. Give up and log error

## Monitoring & Metrics

The HTTP publisher tracks:
- `total_events`: Total events sent
- `total_batches`: Total batches sent
- `total_bytes`: Total bytes transmitted
- `successful_publishes`: Successful HTTP requests
- `failed_publishes`: Failed HTTP requests (after all retries)

These metrics are logged on shutdown and can be exposed via your observability system.

## Memory Safety & Back-Pressure Handling

### Bounded Queue

The event batcher uses a **bounded channel** to prevent unbounded memory growth:

- **Queue Size**: Configurable via `max_queue_size` (default: 10,000 events)
- **Memory Budget**: ~1MB for default config (assuming 100 bytes/event)
- **Drop Policy**: When queue is full, **oldest events are dropped** (ring buffer semantics)
- **Metrics**: Tracks `events_sent` and `events_dropped` for monitoring

### What Happens When Publisher is Slow?

1. **Queue fills up** as events arrive faster than they can be published
2. **Warnings logged** every 100 dropped events
3. **Newest events prioritized** - oldest events dropped to make room
4. **Proxy continues operating** - never blocks query processing
5. **Metrics track drops** - visible in logs and monitoring

### Test Coverage

Two dedicated tests verify bounded memory behavior:

1. **`test_batcher_bounded_queue`**: Verifies events are dropped when queue is full
2. **`test_no_memory_leak_with_slow_publisher`**: Floods queue with 1000 events while publisher is slow

Both tests confirm:
- ✅ Memory stays bounded by `max_queue_size`
- ✅ Events are dropped rather than accumulated
- ✅ No unbounded memory growth

### Monitoring Drops

Check batcher metrics:
```rust
let (sent, dropped) = batcher.get_metrics();
println!("Sent: {}, Dropped: {}", sent, dropped);
```

High drop rates indicate:
- Publisher endpoint is slow or down
- Network connectivity issues
- Need to increase `max_queue_size`
- Consider reducing `batch_size` for more frequent small batches

## Security Considerations

1. **API Key**: Use `http_api_key` for authentication
2. **HTTPS**: Always use HTTPS in production
3. **Rate Limiting**: Implement rate limiting on the analytics server
4. **Anonymization**: Enable `anonymize: true` to protect PII

## Comparison: Debug vs HTTP Publisher

| Feature | DebugLoggerPublisher | HttpPublisher |
|---------|---------------------|---------------|
| Serialization | JSON (serde) | FlexBuffers |
| Output | Logs | HTTP POST |
| Performance | Low overhead | Low overhead |
| Production Ready | ❌ Dev only | ✅ Yes |
| Retries | N/A | ✅ Yes |
| Compression | ❌ | ✅ Optional |
| Authentication | N/A | ✅ API Key |

## Troubleshooting

### Events Not Arriving

1. Check endpoint URL is correct
2. Verify API key if using authentication
3. Check firewall rules
4. Review proxy logs for error messages
5. Test with `publisher_type = "debug"` first

### High Failure Rate

1. Increase `http_timeout_ms` if server is slow
2. Increase `http_max_retries` for flaky networks
3. Check server capacity
4. Monitor server error logs

### Large Payload Sizes

1. Reduce `batch_size` for smaller batches
2. Ensure `http_compression = true`
3. Consider query length limits

## Future Enhancements

- [ ] gRPC support (in addition to HTTP)
- [ ] Additional compression formats (zstd, brotli)
- [ ] Circuit breaker pattern
- [ ] Local buffering on disk for resilience
- [ ] Multiple endpoint failover

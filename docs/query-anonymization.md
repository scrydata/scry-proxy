# Query Anonymization

Scry provides privacy-preserving query logging through SQL-aware anonymization that replaces literal values with placeholders while generating cryptographic fingerprints for hot data detection.

## Table of Contents

- [Why Anonymize Queries?](#why-anonymize-queries)
- [How It Works](#how-it-works)
- [Value Fingerprinting](#value-fingerprinting)
- [Hot Data Detection](#hot-data-detection)
- [Supported SQL](#supported-sql)
- [Configuration](#configuration)
- [Examples](#examples)
- [Limitations](#limitations)

## Why Anonymize Queries?

### Privacy & Compliance

Raw query logging exposes sensitive data:

```sql
-- Raw query (PII exposed!)
SELECT * FROM users WHERE email = 'alice@example.com' AND ssn = '123-45-6789'
```

This violates:
- GDPR (personal data)
- HIPAA (protected health information)
- PCI DSS (payment card data)
- SOC 2 (customer data protection)

### Solution: Anonymization

```sql
-- Anonymized query (safe to log)
SELECT * FROM users WHERE email = ? AND ssn = ?
```

**Benefits**:
- No PII in logs
- Compliance-friendly
- Still useful for query analysis
- Preserves query structure

## How It Works

Scry uses a SQL parser-based approach to intelligently replace literal values:

```
Original Query
      ↓
Parse SQL (sqlparser crate)
      ↓
Extract literal values
      ↓
Generate fingerprints (Blake3 hash)
      ↓
Replace literals with placeholders
      ↓
Anonymized Query + Fingerprints
```

### Algorithm

1. **Parse**: Parse SQL using PostgreSQL dialect
2. **Extract**: Visit AST and collect all literal values
3. **Fingerprint**: Hash each value with Blake3
4. **Normalize**: Replace literals with `?` placeholders
5. **Output**: Return normalized query + fingerprints

### Example Processing

**Input**:
```sql
SELECT * FROM orders WHERE user_id = 12345 AND status = 'completed'
```

**Step 1: Parse**:
```
SelectStatement {
  from: Table("orders"),
  where: BinaryOp(
    BinaryOp(
      Column("user_id"),
      Eq,
      Number(12345)  ← Literal 1
    ),
    And,
    BinaryOp(
      Column("status"),
      Eq,
      String("completed")  ← Literal 2
    )
  )
}
```

**Step 2: Extract Literals**:
```
values = ["12345", "completed"]
```

**Step 3: Fingerprint**:
```
fingerprints = [
  blake3("12345" + salt) = "abc123...",
  blake3("completed" + salt) = "def456..."
]
```

**Step 4: Normalize**:
```sql
SELECT * FROM orders WHERE user_id = ? AND status = ?
```

**Output**:
```rust
AnonymizedQuery {
    normalized_query: "SELECT * FROM orders WHERE user_id = ? AND status = ?",
    value_fingerprints: ["blake3:abc123...", "blake3:def456..."]
}
```

## Value Fingerprinting

### Blake3 Hashing

Scry uses **Blake3** for fast, secure hashing:

- **Fast**: ~1GB/s hashing throughput
- **Secure**: Cryptographically secure (prevents rainbow tables)
- **Deterministic**: Same value → same fingerprint
- **Collision-resistant**: Different values → different fingerprints

### Salt

A salt is mixed with each value before hashing:

```rust
fingerprint = blake3::hash(value + salt)
```

**Default salt**: `b"scry-default-salt"`

**Why salt?**:
- Prevents rainbow table attacks
- Makes fingerprints unique to your deployment
- Even if attacker has fingerprints, can't reverse without salt

**Custom salt** (recommended for production):
```rust
let anonymizer = QueryAnonymizer::with_salt(b"my-secret-salt-12345");
```

### Fingerprint Format

```
blake3:0123456789abcdef...
│      └─ Hex-encoded hash (64 chars)
└─ Algorithm prefix
```

**Example**:
```
blake3:a1b2c3d4e5f6...
```

### Why Fingerprints?

Fingerprints enable privacy-preserving analytics:

```
Original value:  "alice@example.com"
Fingerprint:     "blake3:abc123..."

Query 1: SELECT * FROM users WHERE email = 'alice@example.com'
         → email = ? [blake3:abc123...]

Query 2: SELECT * FROM users WHERE email = 'alice@example.com'
         → email = ? [blake3:abc123...]

Query 3: SELECT * FROM users WHERE email = 'bob@example.com'
         → email = ? [blake3:def456...]
```

**Analysis**:
- Fingerprint `blake3:abc123...` accessed 2 times (hot data!)
- Fingerprint `blake3:def456...` accessed 1 time
- No PII exposed, but access patterns visible

## Hot Data Detection

Scry tracks frequently accessed value fingerprints using Count-Min Sketch + Top-K heap.

### Architecture

```
Value Fingerprint
      ↓
Count-Min Sketch (track access frequency)
      ↓
Top-K Heap (maintain K most frequent)
      ↓
Hot Data Tracker
```

### Count-Min Sketch

Probabilistic data structure for frequency counting:

- **Width**: 2048 buckets
- **Depth**: 4 hash functions
- **Error**: <1% with high probability
- **Memory**: ~64KB
- **Operations**: Lock-free atomic increments

**Example**:
```
Record: blake3:abc123... (access 1)
Record: blake3:abc123... (access 2)
Record: blake3:def456... (access 1)
Record: blake3:abc123... (access 3)

Frequency(abc123...) ≈ 3
Frequency(def456...) ≈ 1
```

### Top-K Heap

Maintains K most frequently accessed fingerprints:

- **Size**: 100 (default)
- **Data**: Min-heap (smallest at root)
- **Update**: Only if count > min count

**Example** (K=3):
```
Heap: [
  {fingerprint: "abc123...", count: 100},
  {fingerprint: "def456...", count: 50},
  {fingerprint: "ghi789...", count: 25}
]

New access: {fingerprint: "jkl012...", count: 30}
  → Replace "ghi789..." (smallest)

New heap: [
  {fingerprint: "abc123...", count: 100},
  {fingerprint: "def456...", count: 50},
  {fingerprint: "jkl012...", count: 30}
]
```

### Accessing Hot Data

```bash
curl http://localhost:9090/debug/hot_data
```

```json
{
  "top_k": [
    {
      "fingerprint": "blake3:a1b2c3d4e5f6...",
      "access_count": 15234
    },
    {
      "fingerprint": "blake3:f6e5d4c3b2a1...",
      "access_count": 8901
    },
    {
      "fingerprint": "blake3:123abc456def...",
      "access_count": 5678
    }
  ]
}
```

### Use Cases

1. **Cache Optimization**: Cache hot fingerprint results
2. **Anomaly Detection**: Sudden spike in specific fingerprint access
3. **Performance Tuning**: Optimize queries accessing hot data
4. **Security**: Detect unusual access patterns (e.g., credential stuffing)

**Example**: User ID `12345` (fingerprint `blake3:abc123...`) accessed 10,000 times in 1 hour:
- Could be a popular user
- Could be an attack (account enumeration)
- Investigate based on baseline patterns

## Supported SQL

### Supported Statements

- ✓ SELECT
- ✓ INSERT
- ✓ UPDATE
- ✓ DELETE
- ✓ WHERE clauses
- ✓ JOIN conditions
- ✓ HAVING clauses
- ✓ VALUES lists
- ✓ Function arguments
- ✓ Subqueries

### Supported Literal Types

- ✓ Numbers (integers, decimals)
- ✓ Strings (single-quoted)
- ✓ Booleans (TRUE, FALSE)
- ✓ NULL

### Preserved Elements

- ✓ Table names
- ✓ Column names
- ✓ Function names
- ✓ Operators
- ✓ SQL keywords

### Not Anonymized

- Column names (needed for query analysis)
- Table names (needed for query analysis)
- SQL structure

**Why?**: Query structure is needed for performance analysis, query plan optimization, and identifying slow queries.

## Configuration

### Enable/Disable Anonymization

```toml
[publisher]
anonymize = true  # Enable (default)
```

```bash
export SCRY_PUBLISHER__ANONYMIZE=true
```

### Custom Salt (Recommended for Production)

Currently set in code:

```rust
// src/protocol/anonymize.rs
const DEFAULT_SALT: &[u8] = b"scry-default-salt";

// For production, modify:
const DEFAULT_SALT: &[u8] = b"your-production-salt-here-make-it-random";
```

**Future**: Configuration option for custom salt.

### Hot Data Tracker Configuration

Currently hardcoded:

```rust
// src/observability/hot_data.rs
const DEFAULT_K: usize = 100;  // Top-100 fingerprints
const DEFAULT_DECAY: f64 = 0.99;  // 1% decay per update
```

## Examples

### SELECT Query

**Original**:
```sql
SELECT name, email FROM users WHERE id = 12345
```

**Anonymized**:
```sql
SELECT name, email FROM users WHERE id = ?
```

**Fingerprints**: `["blake3:abc123..."]`

### INSERT Query

**Original**:
```sql
INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)
```

**Anonymized**:
```sql
INSERT INTO users (name, email, age) VALUES (?, ?, ?)
```

**Fingerprints**: `["blake3:aaa111...", "blake3:bbb222...", "blake3:ccc333..."]`

### UPDATE Query

**Original**:
```sql
UPDATE orders SET status = 'shipped', shipped_at = '2025-12-06' WHERE id = 67890
```

**Anonymized**:
```sql
UPDATE orders SET status = ?, shipped_at = ? WHERE id = ?
```

**Fingerprints**: `["blake3:ddd444...", "blake3:eee555...", "blake3:fff666..."]`

### Complex Query

**Original**:
```sql
SELECT o.id, u.name
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE o.status IN ('pending', 'processing')
  AND o.total > 100.00
  AND u.created_at > '2025-01-01'
```

**Anonymized**:
```sql
SELECT o.id, u.name
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE o.status IN (?, ?)
  AND o.total > ?
  AND u.created_at > ?
```

**Fingerprints**:
```
[
  "blake3:111aaa...",  # 'pending'
  "blake3:222bbb...",  # 'processing'
  "blake3:333ccc...",  # 100.00
  "blake3:444ddd..."   # '2025-01-01'
]
```

## Limitations

### 1. Parser Limitations

Scry uses `sqlparser` crate with PostgreSQL dialect:

- Supports most standard SQL
- May not support all PostgreSQL-specific syntax
- May not support proprietary database extensions

**Fallback**: If parsing fails, original query logged with warning.

### 2. Query Structure Visible

Table names, column names, and SQL structure are **not anonymized**:

```sql
SELECT password_hash FROM users WHERE username = ?
```

An attacker can still see:
- Table name: `users`
- Column names: `password_hash`, `username`
- Query pattern: Looking up user by username

**Mitigation**: Don't rely on query anonymization alone for security. Use proper access controls.

### 3. Fingerprint Reversibility

If attacker has:
- Query fingerprints
- Access to same salt
- Small value space (e.g., true/false)

They can brute-force reverse fingerprints:

```
Fingerprint: blake3:abc123...
Salt: scry-default-salt

Try: blake3("true" + salt) = def456... (no match)
Try: blake3("false" + salt) = abc123... (match!)
```

**Mitigation**:
- Use strong custom salt
- Rotate salt periodically
- Don't publish salt

### 4. Performance Overhead

Anonymization adds latency:
- Parsing: ~100-500μs per query
- Hashing: ~10μs per value
- Total: ~100-1000μs (0.1-1ms)

**Mitigation**:
- Disable if not needed
- Performed asynchronously during event publishing (doesn't block query)

## See Also

- [Observability](observability.md) - Event publishing with anonymized queries
- [Configuration](configuration.md) - Anonymization configuration
- [Architecture](architecture.md) - Where anonymization fits in the system

use blake3::Hasher;
use sqlparser::ast::{Expr, Statement, Value, Visit, VisitMut, Visitor, VisitorMut};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;
use tracing::debug;

/// Result of anonymizing a query
#[derive(Debug, Clone, PartialEq)]
pub struct AnonymizedQuery {
    /// The normalized query with all literals replaced by placeholders
    pub normalized_query: String,
    /// Consistent hash fingerprints of each literal value (in order of appearance)
    pub value_fingerprints: Vec<String>,
}

/// Anonymizes SQL queries by normalizing them and fingerprinting literal values
///
/// This allows detecting hot data patterns (frequently accessed IDs, etc.) while
/// protecting PII by hashing all literal values.
pub struct QueryAnonymizer {
    /// Optional salt for hashing (prevents rainbow table attacks)
    salt: Vec<u8>,
}

impl QueryAnonymizer {
    /// Create a new query anonymizer with default settings
    pub fn new() -> Self {
        Self { salt: b"scry-default-salt".to_vec() }
    }

    /// Create a new query anonymizer with a custom salt
    pub fn with_salt(salt: impl Into<Vec<u8>>) -> Self {
        Self { salt: salt.into() }
    }

    /// Anonymize a SQL query
    ///
    /// Returns the normalized query and value fingerprints, or None if parsing fails
    pub fn anonymize(&self, query: &str) -> Option<AnonymizedQuery> {
        let dialect = PostgreSqlDialect {};

        // Parse the query
        let statements = match Parser::parse_sql(&dialect, query) {
            Ok(stmts) => stmts,
            Err(e) => {
                // Never log the raw query here: on parse failure the caller
                // fails closed (redact/drop), and echoing the unparsed text
                // would leak the very literals anonymization exists to protect
                // (P1 §4.4). Log only the parser error and the query length.
                debug!(
                    error = %e,
                    query_len = query.len(),
                    "Failed to parse query for anonymization; failing closed"
                );
                return None;
            }
        };

        if statements.is_empty() {
            return None;
        }

        // Collect value fingerprints using a visitor
        let mut collector = ValueCollector::new(&self.salt);
        for stmt in &statements {
            let _ = stmt.visit(&mut collector);
        }

        // Generate the normalized query by replacing every literal with a
        // placeholder. `normalize_statements` fails closed (returns None) if any
        // literal survives the replacement, so a partially-anonymized statement
        // is never emitted (P1 §4.4, §5.3).
        let normalized = self.normalize_statements(&statements)?;

        debug!(
            query_len = query.len(),
            fingerprint_count = collector.fingerprints.len(),
            "Anonymized query"
        );

        Some(AnonymizedQuery {
            normalized_query: normalized,
            value_fingerprints: collector.fingerprints,
        })
    }

    /// Normalize statements by converting them to SQL with placeholders.
    ///
    /// Returns `None` if any statement could not be fully normalized (a literal
    /// survived), so the caller fails closed rather than shipping raw values.
    fn normalize_statements(&self, statements: &[Statement]) -> Option<String> {
        let mut normalized = String::new();
        for (i, stmt) in statements.iter().enumerate() {
            if i > 0 {
                normalized.push_str("; ");
            }
            normalized.push_str(&self.normalize_statement(stmt)?);
        }
        Some(normalized)
    }

    /// Normalize a single statement by replacing every literal `Expr::Value`
    /// with a `?` placeholder, using sqlparser's comprehensive `VisitorMut` so
    /// all statement kinds (DDL included, e.g. `CREATE ROLE ... PASSWORD 'x'`)
    /// are covered — not just SELECT/INSERT/UPDATE/DELETE.
    ///
    /// After replacement it re-scans the statement for any surviving literal;
    /// if one remains, normalization is considered incomplete and `None` is
    /// returned so the caller fails closed.
    fn normalize_statement(&self, stmt: &Statement) -> Option<String> {
        let mut normalized_stmt = stmt.clone();
        let mut replacer = ValueReplacer;
        let _ = VisitMut::visit(&mut normalized_stmt, &mut replacer);

        // Defense in depth: verify no literal survived the replacement.
        let mut residual = ResidualLiteralDetector::default();
        let _ = Visit::visit(&normalized_stmt, &mut residual);
        if residual.found_literal {
            return None;
        }

        Some(normalized_stmt.to_string())
    }
}

impl Default for QueryAnonymizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Visitor that collects and fingerprints all literal values in a query
struct ValueCollector {
    fingerprints: Vec<String>,
    salt: Vec<u8>,
}

impl ValueCollector {
    fn new(salt: &[u8]) -> Self {
        Self { fingerprints: Vec::new(), salt: salt.to_vec() }
    }

    fn hash_value(&self, value: &str) -> String {
        let mut hasher = Hasher::new();
        hasher.update(&self.salt);
        hasher.update(value.as_bytes());
        let hash = hasher.finalize();
        format!("{}", hash.to_hex())
    }
}

impl Visitor for ValueCollector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<Self::Break> {
        if let Expr::Value(value) = expr {
            let value_str = match value {
                Value::Number(n, _) => n.clone(),
                Value::SingleQuotedString(s) => s.clone(),
                Value::DoubleQuotedString(s) => s.clone(),
                Value::Boolean(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                _ => return std::ops::ControlFlow::Continue(()),
            };
            let fingerprint = self.hash_value(&value_str);
            self.fingerprints.push(fingerprint);
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// Returns true if a `Value` is a literal that must be redacted (as opposed to
/// a placeholder or other non-sensitive value). Kept in sync with
/// [`ValueCollector`]'s fingerprinting arms.
fn is_sensitive_literal(value: &Value) -> bool {
    matches!(
        value,
        Value::Number(_, _)
            | Value::SingleQuotedString(_)
            | Value::DoubleQuotedString(_)
            | Value::Boolean(_)
            | Value::Null
    )
}

/// `VisitorMut` that replaces every literal value in a statement with a `?`
/// placeholder.
///
/// Uses sqlparser's comprehensive AST walk, so it covers *all* statement kinds
/// (including DDL such as `CREATE ROLE ... PASSWORD 'secret'`), not just the
/// DML the previous hand-rolled traversal knew about — that gap leaked literals
/// into the "normalized" output.
struct ValueReplacer;

impl VisitorMut for ValueReplacer {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value) = expr {
            if is_sensitive_literal(value) {
                *expr = Expr::Value(Value::Placeholder("?".to_string()));
            }
        }
        ControlFlow::Continue(())
    }
}

/// `Visitor` that reports whether any sensitive literal survived replacement.
/// Used as a fail-closed check: if a literal remains after normalization, the
/// statement is not emitted at all.
#[derive(Default)]
struct ResidualLiteralDetector {
    found_literal: bool,
}

impl Visitor for ResidualLiteralDetector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value) = expr {
            if is_sensitive_literal(value) {
                self.found_literal = true;
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_with_number() {
        let anonymizer = QueryAnonymizer::new();
        let result = anonymizer.anonymize("SELECT * FROM users WHERE user_id = 12345");

        assert!(result.is_some());
        let anon = result.unwrap();
        assert!(anon.normalized_query.contains("= ?"));
        assert_eq!(anon.value_fingerprints.len(), 1);
    }

    #[test]
    fn test_select_with_string() {
        let anonymizer = QueryAnonymizer::new();
        let result = anonymizer.anonymize("SELECT * FROM users WHERE email = 'bob@example.com'");

        assert!(result.is_some());
        let anon = result.unwrap();
        assert!(anon.normalized_query.contains("= ?"));
        assert_eq!(anon.value_fingerprints.len(), 1);
    }

    #[test]
    fn test_select_with_multiple_values() {
        let anonymizer = QueryAnonymizer::new();
        let result = anonymizer
            .anonymize("SELECT * FROM users WHERE user_id = 12345 AND email = 'bob@example.com'");

        assert!(result.is_some());
        let anon = result.unwrap();
        assert_eq!(anon.value_fingerprints.len(), 2);
        // Ensure same value produces same fingerprint
        let result2 = anonymizer.anonymize("SELECT * FROM users WHERE user_id = 12345");
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().value_fingerprints[0], anon.value_fingerprints[0]);
    }

    #[test]
    fn test_insert_statement() {
        let anonymizer = QueryAnonymizer::new();
        let result = anonymizer.anonymize("INSERT INTO users (name, age) VALUES ('Alice', 30)");

        assert!(result.is_some());
        let anon = result.unwrap();
        assert_eq!(anon.value_fingerprints.len(), 2);
        assert!(anon.normalized_query.contains("?"));
    }

    #[test]
    fn test_update_statement() {
        let anonymizer = QueryAnonymizer::new();
        let result = anonymizer.anonymize("UPDATE users SET age = 31 WHERE user_id = 12345");

        assert!(result.is_some());
        let anon = result.unwrap();
        assert_eq!(anon.value_fingerprints.len(), 2);
    }

    #[test]
    fn test_invalid_sql() {
        let anonymizer = QueryAnonymizer::new();
        let result = anonymizer.anonymize("INVALID SQL QUERY");

        assert!(result.is_none());
    }

    #[test]
    fn test_hot_data_detection() {
        let anonymizer = QueryAnonymizer::new();

        // Same user_id should produce same fingerprint
        let result1 = anonymizer.anonymize("SELECT * FROM orders WHERE user_id = 999");
        let result2 = anonymizer.anonymize("SELECT * FROM purchases WHERE buyer_id = 999");

        assert!(result1.is_some() && result2.is_some());
        let anon1 = result1.unwrap();
        let anon2 = result2.unwrap();

        // Same value (999) should have same fingerprint
        assert_eq!(anon1.value_fingerprints[0], anon2.value_fingerprints[0]);
    }

    #[test]
    fn test_different_salts_produce_different_fingerprints() {
        let anonymizer1 = QueryAnonymizer::new();
        let anonymizer2 = QueryAnonymizer::with_salt(b"different-salt");

        let result1 = anonymizer1.anonymize("SELECT * FROM users WHERE user_id = 12345");
        let result2 = anonymizer2.anonymize("SELECT * FROM users WHERE user_id = 12345");

        assert!(result1.is_some() && result2.is_some());
        let anon1 = result1.unwrap();
        let anon2 = result2.unwrap();

        // Different salts should produce different fingerprints
        assert_ne!(anon1.value_fingerprints[0], anon2.value_fingerprints[0]);
    }

    #[test]
    fn test_select_star() {
        let anonymizer = QueryAnonymizer::new();
        let result = anonymizer.anonymize("SELECT * FROM users");

        assert!(result.is_some());
        let anon = result.unwrap();
        assert_eq!(anon.value_fingerprints.len(), 0);
        assert!(anon.normalized_query.contains("SELECT"));
    }
}

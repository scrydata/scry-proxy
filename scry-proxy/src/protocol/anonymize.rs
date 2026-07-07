use blake3::Hasher;
use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value, Visit, Visitor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
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

        // Generate normalized query by replacing values with placeholders
        let normalized = self.normalize_statements(&statements);

        debug!(
            query = %query,
            normalized = %normalized,
            fingerprint_count = collector.fingerprints.len(),
            "Anonymized query"
        );

        Some(AnonymizedQuery {
            normalized_query: normalized,
            value_fingerprints: collector.fingerprints,
        })
    }

    /// Normalize statements by converting them to SQL with placeholders
    fn normalize_statements(&self, statements: &[Statement]) -> String {
        let mut normalized = String::new();
        for (i, stmt) in statements.iter().enumerate() {
            if i > 0 {
                normalized.push_str("; ");
            }
            let stmt_normalized = self.normalize_statement(stmt);
            normalized.push_str(&stmt_normalized);
        }
        normalized
    }

    /// Normalize a single statement
    fn normalize_statement(&self, stmt: &Statement) -> String {
        // Use sqlparser's Display trait but replace values
        // For now, we'll use a simple approach: convert to string and use visitor
        let mut normalized_stmt = stmt.clone();
        let mut replacer = ValueReplacer;
        replacer.visit_statement(&mut normalized_stmt);
        normalized_stmt.to_string()
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

/// Visitor that replaces all literal values with placeholders
struct ValueReplacer;

impl ValueReplacer {
    fn visit_statement(&mut self, stmt: &mut Statement) {
        match stmt {
            Statement::Query(query) => self.visit_query(query),
            Statement::Insert(insert) => {
                if let Some(source) = &mut insert.source {
                    self.visit_query(source);
                }
            }
            Statement::Update { selection, assignments, .. } => {
                if let Some(sel) = selection {
                    self.visit_expr(sel);
                }
                for assignment in assignments {
                    self.visit_expr(&mut assignment.value);
                }
            }
            Statement::Delete(delete) => {
                if let Some(selection) = &mut delete.selection {
                    self.visit_expr(selection);
                }
            }
            _ => {}
        }
    }

    fn visit_query(&mut self, query: &mut Box<Query>) {
        match query.body.as_mut() {
            SetExpr::Select(select) => {
                // Replace values in WHERE clause
                if let Some(selection) = &mut select.selection {
                    self.visit_expr(selection);
                }
                // Replace values in SELECT list
                for projection in &mut select.projection {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(expr) = projection {
                        self.visit_expr(expr);
                    } else if let sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } =
                        projection
                    {
                        self.visit_expr(expr);
                    }
                }
            }
            SetExpr::Values(values) => {
                for row in &mut values.rows {
                    for expr in row {
                        self.visit_expr(expr);
                    }
                }
            }
            SetExpr::Query(inner_query) => {
                self.visit_query(inner_query);
            }
            _ => {}
        }
    }

    fn visit_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::Value(_) => {
                // Replace with placeholder
                *expr = Expr::Value(Value::Placeholder("?".to_string()));
            }
            Expr::BinaryOp { left, right, .. } => {
                self.visit_expr(left);
                self.visit_expr(right);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                self.visit_expr(inner);
            }
            Expr::Cast { expr: inner, .. } => {
                self.visit_expr(inner);
            }
            Expr::Between { expr: inner, low, high, .. } => {
                self.visit_expr(inner);
                self.visit_expr(low);
                self.visit_expr(high);
            }
            Expr::InList { expr: inner, list, .. } => {
                self.visit_expr(inner);
                for item in list {
                    self.visit_expr(item);
                }
            }
            Expr::Function(func) => match &mut func.args {
                sqlparser::ast::FunctionArguments::None => {}
                sqlparser::ast::FunctionArguments::Subquery(_) => {}
                sqlparser::ast::FunctionArguments::List(arg_list) => {
                    for arg in &mut arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) = arg
                        {
                            self.visit_expr(e);
                        }
                    }
                }
            },
            Expr::Nested(inner) => {
                self.visit_expr(inner);
            }
            Expr::Subquery(query) => {
                self.visit_query(query);
            }
            _ => {}
        }
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

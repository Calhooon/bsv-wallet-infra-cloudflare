//! D1 database helpers.
//!
//! Provides `Query` builder for parameterized queries and row deserialization
//! helpers that bridge worker::D1 to our entity types.

pub mod batch;

use crate::error::{Error, Result};
use serde::de::DeserializeOwned;
use serde_wasm_bindgen;
use wasm_bindgen::JsValue;
use worker::{D1Database, D1PreparedStatement};

// =============================================================================
// Query Value (replaces sqlx .bind())
// =============================================================================

/// A value that can be bound to a D1 prepared statement.
///
/// D1 uses JsValue bindings. We support the types needed by our schema:
/// - i64 → f64 (JS number, safe for integers up to 2^53)
/// - i32 → f64
/// - String/&str → JsValue::from_str
/// - bool → JsValue::from_bool (stored as 0/1 in SQLite)
/// - Vec<u8>/&[u8] → serde_wasm_bindgen (→ Array of numbers for BLOB)
/// - Option<T> → JsValue::null() or inner value
/// - DateTime<Utc> → ISO 8601 string
pub enum QVal {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Blob(Vec<u8>),
    Float(f64),
}

impl QVal {
    pub fn to_js(&self) -> JsValue {
        match self {
            QVal::Null => JsValue::null(),
            QVal::Int(i) => JsValue::from_f64(*i as f64),
            QVal::Text(s) => JsValue::from_str(s),
            QVal::Bool(b) => JsValue::from_f64(if *b { 1.0 } else { 0.0 }),
            QVal::Blob(b) => serde_wasm_bindgen::to_value(b).unwrap_or(JsValue::null()),
            QVal::Float(f) => JsValue::from_f64(*f),
        }
    }
}

// Conversion traits for ergonomic binding
impl From<i64> for QVal {
    fn from(v: i64) -> Self {
        QVal::Int(v)
    }
}
impl From<i32> for QVal {
    fn from(v: i32) -> Self {
        QVal::Int(v as i64)
    }
}
impl From<u32> for QVal {
    fn from(v: u32) -> Self {
        QVal::Int(v as i64)
    }
}
impl From<String> for QVal {
    fn from(v: String) -> Self {
        QVal::Text(v)
    }
}
impl From<&str> for QVal {
    fn from(v: &str) -> Self {
        QVal::Text(v.to_string())
    }
}
impl From<bool> for QVal {
    fn from(v: bool) -> Self {
        QVal::Bool(v)
    }
}
impl From<Vec<u8>> for QVal {
    fn from(v: Vec<u8>) -> Self {
        QVal::Blob(v)
    }
}
impl From<&[u8]> for QVal {
    fn from(v: &[u8]) -> Self {
        QVal::Blob(v.to_vec())
    }
}
impl From<f64> for QVal {
    fn from(v: f64) -> Self {
        QVal::Float(v)
    }
}
impl From<chrono::DateTime<chrono::Utc>> for QVal {
    fn from(v: chrono::DateTime<chrono::Utc>) -> Self {
        QVal::Text(v.to_rfc3339())
    }
}

// Option conversions
impl<T: Into<QVal>> From<Option<T>> for QVal {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => QVal::Null,
        }
    }
}

// =============================================================================
// Query Builder
// =============================================================================

/// Builds a parameterized D1 query with bind values.
pub struct Query {
    sql: String,
    params: Vec<QVal>,
}

impl Query {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// Add a bind parameter.
    pub fn bind(mut self, val: impl Into<QVal>) -> Self {
        self.params.push(val.into());
        self
    }

    /// Prepare a D1 statement with all bound parameters.
    pub fn prepare(self, db: &D1Database) -> Result<D1PreparedStatement> {
        let stmt = db.prepare(&self.sql);
        if self.params.is_empty() {
            return Ok(stmt);
        }
        let js_values: Vec<JsValue> = self.params.iter().map(|v| v.to_js()).collect();
        stmt.bind(&js_values)
            .map_err(|e| Error::DatabaseError(e.to_string()))
    }

    /// Execute and return all rows deserialized as T.
    pub async fn fetch_all<T: DeserializeOwned>(self, db: &D1Database) -> Result<Vec<T>> {
        let stmt = self.prepare(db)?;
        let result = stmt
            .all()
            .await
            .map_err(|e| Error::DatabaseError(e.to_string()))?;
        result
            .results::<T>()
            .map_err(|e| Error::DatabaseError(e.to_string()))
    }

    /// Execute and return the first row (or None).
    pub async fn fetch_optional<T: DeserializeOwned>(self, db: &D1Database) -> Result<Option<T>> {
        let stmt = self.prepare(db)?;
        stmt.first::<T>(None)
            .await
            .map_err(|e| Error::DatabaseError(e.to_string()))
    }

    /// Execute and return the first row (error if none).
    pub async fn fetch_one<T: DeserializeOwned>(self, db: &D1Database) -> Result<T> {
        self.fetch_optional(db)
            .await?
            .ok_or_else(|| Error::NotFound("No rows returned".to_string()))
    }

    /// Execute (INSERT/UPDATE/DELETE) and return metadata.
    pub async fn execute(self, db: &D1Database) -> Result<ExecMeta> {
        let stmt = self.prepare(db)?;
        let result = stmt
            .run()
            .await
            .map_err(|e| Error::DatabaseError(e.to_string()))?;
        let meta = result
            .meta()
            .map_err(|e| Error::DatabaseError(e.to_string()))?;
        Ok(ExecMeta {
            last_row_id: meta.as_ref().and_then(|m| m.last_row_id).unwrap_or(0),
            changes: meta.as_ref().and_then(|m| m.changes).unwrap_or(0),
        })
    }

    /// Execute and return a scalar value from the first column of the first row.
    pub async fn fetch_scalar<T: DeserializeOwned>(self, db: &D1Database) -> Result<T> {
        // Use raw() to get array-of-arrays, then extract [0][0]
        let stmt = self.prepare(db)?;
        let rows: Vec<Vec<serde_json::Value>> = stmt
            .raw()
            .await
            .map_err(|e| Error::DatabaseError(e.to_string()))?;
        let val = rows
            .first()
            .and_then(|row| row.first())
            .ok_or_else(|| Error::NotFound("No scalar value returned".to_string()))?;
        serde_json::from_value(val.clone()).map_err(|e| Error::DatabaseError(e.to_string()))
    }

    /// Get the SQL string (for batch building).
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Consume and return (sql, params) for batch use.
    pub fn into_parts(self) -> (String, Vec<QVal>) {
        (self.sql, self.params)
    }
}

// =============================================================================
// Execution Metadata
// =============================================================================

pub struct ExecMeta {
    pub last_row_id: i64,
    pub changes: usize,
}

// =============================================================================
// Dynamic WHERE clause builder
// =============================================================================

/// Helps build dynamic SQL WHERE clauses with parameters.
pub struct WhereBuilder {
    clauses: Vec<String>,
    params: Vec<QVal>,
}

impl Default for WhereBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WhereBuilder {
    pub fn new() -> Self {
        Self {
            clauses: Vec::new(),
            params: Vec::new(),
        }
    }

    /// Add `column = ?` condition.
    pub fn eq(mut self, column: &str, val: impl Into<QVal>) -> Self {
        self.clauses.push(format!("{} = ?", column));
        self.params.push(val.into());
        self
    }

    /// Add `column IN (?, ?, ...)` condition from a list of values.
    pub fn in_list(mut self, column: &str, vals: Vec<QVal>) -> Self {
        if vals.is_empty() {
            // Impossible condition - nothing matches
            self.clauses.push("1 = 0".to_string());
        } else {
            let placeholders: Vec<&str> = vals.iter().map(|_| "?").collect();
            self.clauses
                .push(format!("{} IN ({})", column, placeholders.join(", ")));
            self.params.extend(vals);
        }
        self
    }

    /// Add `column >= ?` condition.
    pub fn gte(mut self, column: &str, val: impl Into<QVal>) -> Self {
        self.clauses.push(format!("{} >= ?", column));
        self.params.push(val.into());
        self
    }

    /// Add a raw clause with parameters.
    pub fn raw(mut self, clause: &str, params: Vec<QVal>) -> Self {
        self.clauses.push(clause.to_string());
        self.params.extend(params);
        self
    }

    /// Build the WHERE clause string and parameters.
    /// Returns ("", empty) if no conditions, or (" WHERE ...", params).
    pub fn build(self) -> (String, Vec<QVal>) {
        if self.clauses.is_empty() {
            (String::new(), Vec::new())
        } else {
            let clause = format!(" WHERE {}", self.clauses.join(" AND "));
            (clause, self.params)
        }
    }
}

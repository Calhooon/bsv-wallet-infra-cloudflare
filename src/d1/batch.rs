//! D1 batch collector for atomic multi-statement execution.
//!
//! D1 has no BEGIN/COMMIT. Instead, `db.batch(stmts)` executes all statements
//! atomically (up to 100 per batch, all-or-nothing).
//!
//! Usage pattern:
//! 1. Read phase — individual queries to gather state
//! 2. Compute phase — pure Rust logic
//! 3. Write phase — collect all writes into BatchCollector, then execute

use crate::d1::QVal;
use crate::error::{Error, Result};
use wasm_bindgen::JsValue;
use worker::{D1Database, D1PreparedStatement, D1Result};

/// Collects prepared statements for atomic batch execution.
pub struct BatchCollector<'a> {
    db: &'a D1Database,
    statements: Vec<D1PreparedStatement>,
}

impl<'a> BatchCollector<'a> {
    pub fn new(db: &'a D1Database) -> Self {
        Self {
            db,
            statements: Vec::new(),
        }
    }

    /// Add a parameterized statement to the batch.
    pub fn add(&mut self, sql: &str, params: Vec<QVal>) -> Result<()> {
        let stmt = self.db.prepare(sql);
        let bound = if params.is_empty() {
            stmt
        } else {
            let js_values: Vec<JsValue> = params.iter().map(|v| v.to_js()).collect();
            stmt.bind(&js_values)
                .map_err(|e| Error::DatabaseError(e.to_string()))?
        };
        self.statements.push(bound);
        Ok(())
    }

    /// Number of statements in the batch.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    /// Execute all statements atomically.
    /// Returns results in the same order as statements were added.
    ///
    /// If the batch exceeds 100 statements, it is split into sequential
    /// batches of 100. Each sub-batch is atomic internally, but failures
    /// in later batches won't roll back earlier ones.
    pub async fn execute(self) -> Result<Vec<D1Result>> {
        if self.statements.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_results = Vec::new();

        // D1 limit: 100 statements per batch
        for chunk in self.statements.chunks(100) {
            let batch: Vec<D1PreparedStatement> = chunk.to_vec();
            let results = self
                .db
                .batch(batch)
                .await
                .map_err(|e| Error::DatabaseError(format!("Batch execution failed: {}", e)))?;
            all_results.extend(results);
        }

        Ok(all_results)
    }

    /// Execute and return the last_row_id from the last INSERT in the batch.
    /// Useful when the batch contains a single INSERT and you need the ID.
    pub async fn execute_returning_last_id(self) -> Result<i64> {
        let results = self.execute().await?;
        let last = results
            .last()
            .ok_or_else(|| Error::DatabaseError("Empty batch result".to_string()))?;
        let meta = last
            .meta()
            .map_err(|e| Error::DatabaseError(e.to_string()))?;
        Ok(meta.as_ref().and_then(|m| m.last_row_id).unwrap_or(0))
    }
}

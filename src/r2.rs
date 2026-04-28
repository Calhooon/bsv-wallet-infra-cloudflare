//! R2 blob storage with size threshold.
//!
//! Blobs larger than THRESHOLD bytes are stored in R2 instead of D1.
//! The D1 column is set to NULL when the blob is in R2.
//!
//! Key scheme: `{table}/{id}/{column}` (e.g., `transactions/42/raw_tx`)

use crate::error::{Error, Result};
use worker::*;

/// Blobs larger than this go to R2; smaller ones stay in D1.
const THRESHOLD: usize = 4096;

pub struct BlobStore<'a> {
    bucket: &'a Bucket,
}

impl<'a> BlobStore<'a> {
    pub fn new(bucket: &'a Bucket) -> Self {
        Self { bucket }
    }

    /// Store a blob. Returns (d1_value, stored_in_r2).
    /// If the blob is small enough, returns (Some(blob), false) for D1 inline storage.
    /// If large, stores in R2 and returns (None, true) — D1 column should be set to NULL.
    pub async fn put(
        &self,
        table: &str,
        id: i64,
        column: &str,
        data: &[u8],
    ) -> Result<(Option<Vec<u8>>, bool)> {
        if data.len() <= THRESHOLD {
            Ok((Some(data.to_vec()), false))
        } else {
            let key = format!("{}/{}/{}", table, id, column);
            self.bucket
                .put(&key, data.to_vec())
                .execute()
                .await
                .map_err(|e| Error::InternalError(format!("R2 put failed: {}", e)))?;
            Ok((None, true))
        }
    }

    /// Read a blob. Check D1 column first; if None, try R2.
    pub async fn get(
        &self,
        table: &str,
        id: i64,
        column: &str,
        d1_value: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(data) = d1_value {
            return Ok(Some(data));
        }
        // Try R2
        let key = format!("{}/{}/{}", table, id, column);
        let obj = self
            .bucket
            .get(&key)
            .execute()
            .await
            .map_err(|e| Error::InternalError(format!("R2 get failed: {}", e)))?;
        match obj {
            Some(obj) => {
                let body = obj
                    .body()
                    .ok_or_else(|| Error::InternalError("R2 object has no body".to_string()))?;
                let bytes = body
                    .bytes()
                    .await
                    .map_err(|e| Error::InternalError(format!("R2 read failed: {}", e)))?;
                Ok(Some(bytes))
            }
            None => Ok(None),
        }
    }

    /// Delete a blob from R2 (cleanup).
    pub async fn delete(&self, table: &str, id: i64, column: &str) -> Result<()> {
        let key = format!("{}/{}/{}", table, id, column);
        self.bucket
            .delete(&key)
            .await
            .map_err(|e| Error::InternalError(format!("R2 delete failed: {}", e)))?;
        Ok(())
    }
}

/// Helper: determine if a blob should go to R2 based on size.
pub fn should_use_r2(data: &[u8]) -> bool {
    data.len() > THRESHOLD
}

/// Generate an R2 key from table, id, and column.
/// Key format: `{table}/{id}/{column}`.
pub fn r2_key(table: &str, id: i64, column: &str) -> String {
    format!("{}/{}/{}", table, id, column)
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // should_use_r2 — threshold logic
    // =========================================================================

    #[test]
    fn test_empty_blob_is_inline() {
        assert!(!should_use_r2(&[]));
    }

    #[test]
    fn test_one_byte_is_inline() {
        assert!(!should_use_r2(&[0x42]));
    }

    #[test]
    fn test_exactly_threshold_is_inline() {
        // At exactly 4096 bytes, should NOT use R2 (data.len() > THRESHOLD, not >=)
        let data = vec![0u8; THRESHOLD];
        assert!(!should_use_r2(&data));
    }

    #[test]
    fn test_one_above_threshold_uses_r2() {
        let data = vec![0u8; THRESHOLD + 1];
        assert!(should_use_r2(&data));
    }

    #[test]
    fn test_well_below_threshold_is_inline() {
        let data = vec![0u8; 100];
        assert!(!should_use_r2(&data));
    }

    #[test]
    fn test_well_above_threshold_uses_r2() {
        let data = vec![0u8; 1_000_000];
        assert!(should_use_r2(&data));
    }

    #[test]
    fn test_threshold_value_is_4096() {
        // Verify the constant hasn't been accidentally changed
        assert_eq!(THRESHOLD, 4096);
    }

    // =========================================================================
    // r2_key — key format
    // =========================================================================

    #[test]
    fn test_key_format_basic() {
        assert_eq!(
            r2_key("transactions", 42, "raw_tx"),
            "transactions/42/raw_tx"
        );
    }

    #[test]
    fn test_key_format_large_id() {
        assert_eq!(
            r2_key("outputs", 999999999, "locking_script"),
            "outputs/999999999/locking_script"
        );
    }

    #[test]
    fn test_key_format_zero_id() {
        assert_eq!(
            r2_key("proven_txs", 0, "merkle_path"),
            "proven_txs/0/merkle_path"
        );
    }

    #[test]
    fn test_key_format_negative_id() {
        // Negative IDs shouldn't happen in practice but shouldn't panic
        assert_eq!(r2_key("test", -1, "col"), "test/-1/col");
    }

    #[test]
    fn test_key_format_various_tables() {
        // Verify several table/column combinations used in the codebase
        assert_eq!(r2_key("transactions", 1, "raw_tx"), "transactions/1/raw_tx");
        assert_eq!(
            r2_key("transactions", 1, "input_beef"),
            "transactions/1/input_beef"
        );
        assert_eq!(
            r2_key("outputs", 5, "locking_script"),
            "outputs/5/locking_script"
        );
        assert_eq!(r2_key("proven_txs", 10, "raw_tx"), "proven_txs/10/raw_tx");
    }
}

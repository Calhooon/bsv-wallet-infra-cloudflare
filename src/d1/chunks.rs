//! Statements over a list of keys, within D1's bound-parameter limit.
//!
//! D1 binds at most [`MAX_BOUND_PARAMS`] parameters a statement and answers
//! `too many SQL variables` past it. A statement whose `IN (...)` list grows
//! with a page (the labels, tags, outputs or certificate fields of up to
//! 1,000 rows) runs once a slice of the keys here, the rows of every slice in
//! turn; a list that filters a paged or counted statement binds as one JSON
//! array instead ([`json_in`]), so the paging and the count stay one statement.

use serde::de::DeserializeOwned;
use worker::D1Database;

use super::{QVal, Query};
use crate::error::Result;

/// The most parameters D1 binds in one statement.
pub const MAX_BOUND_PARAMS: usize = 100;

/// `n` placeholders, `?,?,...`.
pub fn placeholders(n: usize) -> String {
    vec!["?"; n].join(",")
}

/// The keys without repeats, in the order each first appears: a key repeated
/// across two slices would otherwise fetch its rows twice.
pub fn distinct<V: Clone + PartialEq>(keys: &[V]) -> Vec<V> {
    let mut out: Vec<V> = Vec::with_capacity(keys.len());
    for k in keys {
        if !out.contains(k) {
            out.push(k.clone());
        }
    }
    out
}

/// The keys in slices small enough that each, with the `fixed` other
/// parameters of its statement, binds at most [`MAX_BOUND_PARAMS`].
pub fn bind_chunks<V>(keys: &[V], fixed: usize) -> std::slice::Chunks<'_, V> {
    keys.chunks(MAX_BOUND_PARAMS.saturating_sub(fixed).max(1))
}

/// The statements `sql(n)` makes over `keys`, one a slice: the keys repeated
/// nowhere, each slice binding its keys in order.
pub fn chunked_queries<V: Clone + PartialEq + Into<QVal>>(
    keys: &[V],
    sql: impl Fn(usize) -> String,
) -> Vec<Query> {
    let keys = distinct(keys);
    bind_chunks(&keys, 0)
        .map(|chunk| {
            chunk
                .iter()
                .cloned()
                .fold(Query::new(sql(chunk.len())), |q, k| q.bind(k))
        })
        .collect()
}

/// The rows of `sql(n)` over every key, a statement a slice, the slices' rows
/// in turn.
pub async fn fetch_all_chunked<T: DeserializeOwned, V: Clone + PartialEq + Into<QVal>>(
    db: &D1Database,
    keys: &[V],
    sql: impl Fn(usize) -> String,
) -> Result<Vec<T>> {
    let mut rows = Vec::new();
    for query in chunked_queries(keys, sql) {
        rows.extend(query.fetch_all::<T>(db).await?);
    }
    Ok(rows)
}

/// `column IN (...)` over `values` as one bound JSON array: the clause and its
/// one parameter.
pub fn json_in(column: &str, values: &[QVal]) -> (String, QVal) {
    let array: Vec<serde_json::Value> = values
        .iter()
        .map(|v| match v {
            QVal::Int(i) => serde_json::Value::from(*i),
            QVal::Text(s) => serde_json::Value::from(s.as_str()),
            QVal::Bool(b) => serde_json::Value::from(i64::from(*b)),
            QVal::Float(f) => serde_json::Value::from(*f),
            QVal::Null | QVal::Blob(_) => serde_json::Value::Null,
        })
        .collect();
    (
        format!("{column} IN (SELECT value FROM json_each(?))"),
        QVal::Text(serde_json::Value::Array(array).to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_counts() {
        assert_eq!(placeholders(0), "");
        assert_eq!(placeholders(1), "?");
        assert_eq!(placeholders(3), "?,?,?");
    }

    #[test]
    fn chunks_stay_within_the_limit() {
        let keys: Vec<i64> = (0..1000).collect();
        let sizes: Vec<usize> = bind_chunks(&keys, 0).map(<[i64]>::len).collect();
        assert_eq!(sizes, vec![100; 10]);
        let sizes: Vec<usize> = bind_chunks(&keys[..250], 1).map(<[i64]>::len).collect();
        assert_eq!(sizes, vec![99, 99, 52]);
        assert_eq!(bind_chunks(&keys[..100], 0).count(), 1);
        assert_eq!(bind_chunks(&keys[..0], 0).count(), 0);
    }

    #[test]
    fn distinct_keeps_first_order() {
        assert_eq!(distinct(&[3, 1, 3, 2, 1]), vec![3, 1, 2]);
    }

    #[test]
    fn chunked_queries_repeat_no_key() {
        let mut keys: Vec<i64> = (0..150).collect();
        keys.extend(0..150);
        let parts: Vec<(String, Vec<QVal>)> = chunked_queries(&keys, placeholders)
            .into_iter()
            .map(Query::into_parts)
            .collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].1.len(), 100);
        assert_eq!(parts[1].1.len(), 50);
        assert_eq!(parts[1].0.split(',').count(), 50);
    }

    #[test]
    fn json_in_binds_one_array() {
        let (clause, value) = json_in("label", &[QVal::Text("a\"b".into()), QVal::Int(7)]);
        assert_eq!(clause, "label IN (SELECT value FROM json_each(?))");
        match value {
            QVal::Text(s) => assert_eq!(s, r#"["a\"b",7]"#),
            _ => panic!("a JSON array as text"),
        }
    }
}

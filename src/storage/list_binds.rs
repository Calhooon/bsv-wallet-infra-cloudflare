//! The statements that bind a list, on SQLite with D1's limit of 100 bound
//! parameters a statement set on the connection: the actual migrations, the
//! actual statement builders and the actual slicing (`crate::d1::chunks`),
//! over 250 rows. Each test also runs the one-statement form (every key bound
//! at once) and shows SQLite refuse it at 100, as D1 does; and each compares
//! the sliced rows, grouped by key, with that one statement's rows on a
//! connection without the limit: the same rows, each once, in the same order
//! within a key.

use std::collections::BTreeMap;

use rusqlite::limits::Limit;
use rusqlite::types::Value;

use crate::d1::chunks::{chunked_queries, MAX_BOUND_PARAMS};
use crate::d1::{QVal, Query, WhereBuilder};

const N: i64 = 250;

fn db() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in [
        include_str!("../../migrations/0001_initial.sql"),
        include_str!("../../migrations/0002_add_indexes.sql"),
        include_str!("../../migrations/0003_add_output_reservations.sql"),
        include_str!("../../migrations/0004_proven_txs_height_pair_index.sql"),
    ] {
        conn.execute_batch(sql).unwrap();
    }
    seed(&conn);
    let _ = conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, MAX_BOUND_PARAMS as i32);
    assert_eq!(
        conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER),
        MAX_BOUND_PARAMS as i32
    );
    conn
}

/// `sql` on the same rows with SQLite's own limit (32,766), the limit put
/// back to D1's after: what the one statement answers where it is admitted.
fn unlimited(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[QVal],
) -> rusqlite::Result<Vec<Vec<Value>>> {
    let _ = conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 32_766);
    let rows = run(conn, sql, params);
    let _ = conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, MAX_BOUND_PARAMS as i32);
    rows
}

/// User 1: 250 transactions, each with its own label and the shared one;
/// one output each and a second on every fifth, each output with its own tag
/// and the shared one; 250 certificates of two fields; 250 proven txs and
/// their requests. User 2 holds a label and a tag of the same names as
/// user 1's, which no statement of user 1 may return.
fn seed(conn: &rusqlite::Connection) {
    conn.execute_batch(
        "INSERT INTO users (user_id, identity_key) VALUES (1, '02aa'), (2, '02bb');
         INSERT INTO output_baskets (basket_id, user_id, name) VALUES (1, 1, 'default'), (2, 2, 'default');
         INSERT INTO tx_labels (tx_label_id, user_id, label) VALUES (100000, 1, 'shared'), (100001, 2, 'label-1');
         INSERT INTO output_tags (output_tag_id, user_id, tag) VALUES (100000, 1, 'shared'), (100001, 2, 'tag-1');",
    )
    .unwrap();
    let mut output_id = 0;
    for i in 1..=N {
        conn.execute(
            "INSERT INTO transactions (transaction_id, user_id, status, reference, is_outgoing, description, txid)
             VALUES (?1, 1, 'completed', 'r' || ?1, 0, 'd' || ?1, printf('tx%04d', ?1))",
            [i],
        )
        .unwrap();
        // The shared label first on even transactions, last on odd: the order
        // within a key is the map's, and must survive the slicing.
        conn.execute(
            "INSERT INTO tx_labels (tx_label_id, user_id, label) VALUES (?1, 1, 'label-' || ?1)",
            [i],
        )
        .unwrap();
        let (first, second) = if i % 2 == 0 { (100000, i) } else { (i, 100000) };
        for label in [first, second] {
            conn.execute(
                "INSERT INTO tx_labels_map (tx_label_id, transaction_id) VALUES (?1, ?2)",
                [label, i],
            )
            .unwrap();
        }
        for vout in 0..(if i % 5 == 0 { 2 } else { 1 }) {
            output_id += 1;
            conn.execute(
                "INSERT INTO outputs (output_id, user_id, transaction_id, basket_id, spendable, change, vout, satoshis,
                                      provided_by, purpose, type, txid, locking_script, custom_instructions)
                 VALUES (?1, 1, ?2, 1, 1, 0, ?3, 1000 + ?1, 'you', '', 'custom', printf('tx%04d', ?2), X'51', 'ci-' || ?1)",
                [output_id, i, vout],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO output_tags (output_tag_id, user_id, tag) VALUES (?1, 1, 'tag-' || ?1)",
                [output_id],
            )
            .unwrap();
            let (first, second) = if output_id % 2 == 0 {
                (100000, output_id)
            } else {
                (output_id, 100000)
            };
            for tag in [first, second] {
                conn.execute(
                    "INSERT INTO output_tags_map (output_tag_id, output_id) VALUES (?1, ?2)",
                    [tag, output_id],
                )
                .unwrap();
            }
        }
        conn.execute(
            "INSERT INTO certificates (certificate_id, user_id, serial_number, type, certifier, subject, revocation_outpoint, signature)
             VALUES (?1, 1, 's' || ?1, 'type-' || (?1 % 3), 'certifier-' || ?1, 'subject', 'outpoint', 'sig')",
            [i],
        )
        .unwrap();
        for name in ["name", "email"] {
            conn.execute(
                "INSERT INTO certificate_fields (user_id, certificate_id, field_name, field_value, master_key)
                 VALUES (1, ?1, ?2, 'v-' || ?1, 'k')",
                rusqlite::params![i, name],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO proven_txs (proven_tx_id, txid, height, idx, block_hash, merkle_root, merkle_path, raw_tx)
             VALUES (?1, printf('tx%04d', ?1), 800000 + ?1, 0, 'h', 'r', X'00', X'00')",
            [i],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO proven_tx_reqs (txid, raw_tx, status) VALUES (printf('tx%04d', ?1), X'00', CASE WHEN ?1 % 2 = 0 THEN 'completed' ELSE 'unmined' END)",
            [i],
        )
        .unwrap();
    }
}

fn to_value(v: &QVal) -> Value {
    match v {
        QVal::Null => Value::Null,
        QVal::Int(i) => Value::Integer(*i),
        QVal::Text(s) => Value::Text(s.clone()),
        QVal::Bool(b) => Value::Integer(i64::from(*b)),
        QVal::Blob(b) => Value::Blob(b.clone()),
        QVal::Float(f) => Value::Real(*f),
    }
}

fn run(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[QVal],
) -> rusqlite::Result<Vec<Vec<Value>>> {
    let mut stmt = conn.prepare(sql)?;
    let columns = stmt.column_count();
    let params: Vec<Value> = params.iter().map(to_value).collect();
    let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
        (0..columns).map(|i| row.get::<_, Value>(i)).collect()
    })?;
    rows.collect()
}

fn run_query(conn: &rusqlite::Connection, query: Query) -> rusqlite::Result<Vec<Vec<Value>>> {
    let (sql, params) = query.into_parts();
    assert!(
        params.len() <= MAX_BOUND_PARAMS,
        "{} parameters",
        params.len()
    );
    run(conn, &sql, &params)
}

/// The rows of `sql(n)` over `keys`, sliced as the Worker slices them.
fn chunked(
    conn: &rusqlite::Connection,
    keys: &[i64],
    sql: impl Fn(usize) -> String,
) -> Vec<Vec<Value>> {
    chunked_queries(keys, sql)
        .into_iter()
        .flat_map(|q| run_query(conn, q).unwrap())
        .collect()
}

/// Every key bound at once, the form before the slicing.
fn one_statement(keys: &[i64], sql: impl Fn(usize) -> String) -> (String, Vec<QVal>) {
    let keys = crate::d1::chunks::distinct(keys);
    (sql(keys.len()), keys.into_iter().map(QVal::Int).collect())
}

fn refused(result: rusqlite::Result<Vec<Vec<Value>>>) {
    let err = result.expect_err("SQLite at 100 bound parameters must refuse the one statement");
    assert!(err.to_string().contains("too many SQL variables"), "{err}");
}

/// The rows grouped by their first column, each key's rows in order.
fn grouped(rows: Vec<Vec<Value>>) -> BTreeMap<i64, Vec<Vec<Value>>> {
    grouped_by(rows, 0)
}

/// The rows grouped by column `key`, each key's rows in order.
fn grouped_by(rows: Vec<Vec<Value>>, key: usize) -> BTreeMap<i64, Vec<Vec<Value>>> {
    let mut map: BTreeMap<i64, Vec<Vec<Value>>> = BTreeMap::new();
    for mut row in rows {
        let Value::Integer(k) = row.remove(key) else {
            panic!("an integer key")
        };
        map.entry(k).or_default().push(row);
    }
    map
}

/// A page's keys as the readers pass them: repeats where two rows share a
/// key (two outputs of one transaction), and 250 distinct.
fn page_keys() -> Vec<i64> {
    let mut keys: Vec<i64> = (1..=N).rev().collect();
    keys.extend((1..=N).step_by(5));
    keys
}

fn check_page_statement(
    sql: impl Fn(usize) -> String + Copy,
    key: usize,
    per_key: usize,
    want_keys: usize,
) {
    let limited = db();
    let keys = page_keys();
    let (one, params) = one_statement(&keys, sql);
    refused(run(&limited, &one, &params));
    let want = grouped_by(unlimited(&limited, &one, &params).unwrap(), key);
    let got_rows = chunked(&limited, &keys, sql);
    let got = grouped_by(got_rows.clone(), key);
    assert_eq!(got.len(), want_keys);
    assert_eq!(got, want);
    assert!(got.values().all(|rows| rows.len() >= per_key));
    // No row twice, though keys repeat across the slices.
    let mut seen = got_rows.clone();
    seen.sort_by_key(|r| format!("{r:?}"));
    seen.dedup();
    assert_eq!(seen.len(), got_rows.len());
    // A page of 100 or fewer keys is one statement, the same as before.
    assert_eq!(chunked_queries(&keys[..100], sql).len(), 1);
}

#[test]
fn transaction_labels_slice_past_100() {
    check_page_statement(super::readers::transaction_labels_sql, 0, 2, N as usize);
    // Each transaction's labels its own: label-i and the shared one.
    let got = grouped(chunked(
        &db(),
        &page_keys(),
        super::readers::transaction_labels_sql,
    ));
    for (tid, rows) in got {
        let mut labels: Vec<String> = rows
            .into_iter()
            .map(|r| match &r[0] {
                Value::Text(s) => s.clone(),
                v => panic!("{v:?}"),
            })
            .collect();
        labels.sort();
        assert_eq!(labels, vec![format!("label-{tid}"), "shared".to_string()]);
    }
}

#[test]
fn output_tags_slice_past_100() {
    let outputs = 300; // 250 + every fifth transaction's second
    let keys: Vec<i64> = (1..=outputs).collect();
    let limited = db();
    let (one, params) = one_statement(&keys, super::readers::output_tags_sql);
    refused(run(&limited, &one, &params));
    let want = grouped(unlimited(&limited, &one, &params).unwrap());
    let got = grouped(chunked(&limited, &keys, super::readers::output_tags_sql));
    assert_eq!(got.len(), outputs as usize);
    assert_eq!(got, want);
    check_page_statement(super::readers::output_tags_sql, 0, 2, N as usize);
}

#[test]
fn action_outputs_slice_past_100_and_match_the_per_action_statement() {
    let limited = db();
    let keys = page_keys();
    for include_locking in [false, true] {
        let sql = |n| super::readers::action_outputs_sql(n, include_locking);
        let (one, params) = one_statement(&keys, sql);
        refused(run(&limited, &one, &params));
        let got = grouped(chunked(&limited, &keys, sql));
        assert_eq!(got.len(), N as usize);
        // The statement the readers ran a row each before: the same outputs
        // in the same order, column for column.
        let locking_col = if include_locking {
            "hex(o.locking_script) as locking_script"
        } else {
            "NULL as locking_script"
        };
        let per_action = format!(
            "SELECT o.output_id, o.vout, o.satoshis, o.spendable, {locking_col}, \
             o.custom_instructions, o.output_description, ob.name as basket_name \
             FROM outputs o \
             LEFT JOIN output_baskets ob ON o.basket_id = ob.basket_id \
             WHERE o.transaction_id = ? \
             ORDER BY o.vout ASC"
        );
        for (tid, rows) in &got {
            let want = unlimited(&limited, &per_action, &[QVal::Int(*tid)]).unwrap();
            assert_eq!(rows, &want, "transaction {tid}");
            assert_eq!(rows.len(), if tid % 5 == 0 { 2 } else { 1 });
        }
    }
    // Their tags, fetched for the page, in the order the per-output statement gave.
    let output_ids: Vec<i64> = (1..=300).collect();
    let tags = grouped(chunked(
        &limited,
        &output_ids,
        super::readers::output_tags_sql,
    ));
    let per_output = "SELECT t.tag FROM output_tags t \
         JOIN output_tags_map m ON t.output_tag_id = m.output_tag_id \
         WHERE m.output_id = ? AND m.is_deleted = 0 AND t.is_deleted = 0";
    for (oid, rows) in tags {
        assert_eq!(
            rows,
            unlimited(&limited, per_output, &[QVal::Int(oid)]).unwrap(),
            "output {oid}"
        );
    }
}

#[test]
fn certificate_fields_slice_past_100() {
    check_page_statement(
        super::certificates::certificate_fields_sql,
        2,
        2,
        N as usize,
    );
}

#[test]
fn proven_proofs_slice_past_100() {
    let limited = db();
    let txids: Vec<String> = (1..=N + 50).map(|i| format!("tx{i:04}")).collect();
    let one: Vec<QVal> = txids.iter().map(|t| QVal::Text(t.clone())).collect();
    refused(run(
        &limited,
        &super::create_action::proven_proofs_sql(one.len()),
        &one,
    ));
    let rows: Vec<Vec<Value>> = chunked_queries(&txids, super::create_action::proven_proofs_sql)
        .into_iter()
        .flat_map(|q| run_query(&limited, q).unwrap())
        .collect();
    assert_eq!(rows.len(), N as usize);
}

#[test]
fn label_and_tag_names_bind_as_one_parameter() {
    let limited = db();
    for (names, build, old) in [
        (
            (1..=N + 20)
                .map(|i| format!("label-{i}"))
                .collect::<Vec<_>>(),
            super::readers::resolve_label_ids_sql as fn(&[String]) -> (String, QVal),
            "SELECT tx_label_id FROM tx_labels WHERE user_id = ? AND is_deleted = 0 AND label IN",
        ),
        (
            (1..=N + 20).map(|i| format!("tag-{i}")).collect::<Vec<_>>(),
            super::readers::resolve_tag_ids_sql as fn(&[String]) -> (String, QVal),
            "SELECT output_tag_id FROM output_tags WHERE user_id = ? AND is_deleted = 0 AND tag IN",
        ),
    ] {
        let mut names = names;
        names.push(names[0].clone()); // a name twice
        let mut old_params = vec![QVal::Int(1)];
        old_params.extend(names.iter().map(|n| QVal::Text(n.clone())));
        let old_sql = format!("{old} ({})", crate::d1::chunks::placeholders(names.len()));
        refused(run(&limited, &old_sql, &old_params));
        let mut want = unlimited(&limited, &old_sql, &old_params).unwrap();
        let (sql, param) = build(&names);
        let mut got = run(&limited, &sql, &[QVal::Int(1), param]).unwrap();
        want.sort_by_key(|r| format!("{r:?}"));
        got.sort_by_key(|r| format!("{r:?}"));
        assert_eq!(got, want);
        assert!(got.len() >= N as usize, "{} ids", got.len());
        assert!(
            !got.contains(&vec![Value::Integer(100001)]),
            "another user's id"
        );
    }
}

#[test]
fn certificate_filters_bind_as_one_parameter() {
    let limited = db();
    let certifiers: Vec<QVal> = (1..=N + 30)
        .map(|i| QVal::Text(format!("certifier-{i}")))
        .collect();
    let (where_clause, params) = WhereBuilder::new()
        .eq("user_id", 1i64)
        .eq("is_deleted", 0i32)
        .in_list("certifier", certifiers)
        .in_list(
            "type",
            vec![QVal::Text("type-0".into()), QVal::Text("type-1".into())],
        )
        .build();
    assert_eq!(params.len(), 4);
    let count = run(
        &limited,
        &format!("SELECT COUNT(*) FROM certificates{where_clause}"),
        &params,
    )
    .unwrap();
    // certificate i has type-(i % 3): two in three of the 250.
    let want = (1..=N).filter(|i| i % 3 != 2).count() as i64;
    assert_eq!(count, vec![vec![Value::Integer(want)]]);
    let rows = run(
        &limited,
        &format!("SELECT certificate_id FROM certificates{where_clause} ORDER BY created_at DESC LIMIT 1000 OFFSET 0"),
        &params,
    )
    .unwrap();
    assert_eq!(rows.len() as i64, want);
}

#[test]
fn proven_tx_req_filters_bind_as_one_parameter() {
    let limited = db();
    let args: crate::types::FindProvenTxReqsArgs = serde_json::from_value(serde_json::json!({
        "txids": (1..=N + 30).map(|i| format!("tx{i:04}")).collect::<Vec<_>>(),
        "status": ["completed"],
        "paged": {"limit": 1000},
    }))
    .unwrap();
    let (sql, params) = super::proven_tx_reqs::find_sql(1, &args);
    assert_eq!(params.len(), 5);
    let rows = run(&limited, &sql, &params).unwrap();
    assert_eq!(rows.len(), N as usize / 2);
    let unfiltered: crate::types::FindProvenTxReqsArgs =
        serde_json::from_value(serde_json::json!({})).unwrap();
    let (sql, params) = super::proven_tx_reqs::find_sql(1, &unfiltered);
    assert_eq!(run(&limited, &sql, &params).unwrap().len(), N as usize);
}

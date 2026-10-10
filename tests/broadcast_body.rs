//! bsv-stack-lean #63 (F1), the poster's side of the broadcast body: what the
//! inline door of `internalizeAction` hands ARC for an AtomicBEEF.
//!
//! The door posts `broadcast_beef(&hex::encode(bytes))` with the caller's
//! AtomicBEEF as it came (`src/storage/internalize_action.rs`), and ARC's
//! provider builds the request from that hex (`arc_beef_request`). ARC at
//! `e7efc5b` reads the body by its content type (`[SRC] bitcoin-sv/arc@e7efc5b
//! internal/api/handler/parsers.go:20-61`) and tells a BEEF by bytes 2 and 3,
//! `BE EF` (`internal/beef/beef.go:24-34`, `internal/validator/helpers.go:36-46`);
//! the prefix's are `01 01`, so an AtomicBEEF is read as a raw transaction and
//! answered 400 before any validation.
//!
//! The six rows are bsv-stack-lean's replay rows (`corpus/runners/broadcast-body/`,
//! the bytes of `corpus/beef-of-any-size/bytes/` at 805c1c1, their sha256 as its
//! `results.txt` records them): three AtomicBEEFs and the same three BEEFs
//! without the prefix. The replay answers 400 for each AtomicBEEF and parses
//! each plain BEEF at ARC's pin; here the request this crate builds for each
//! row must put a plain BEEF in front of ARC, the plain rows unchanged, as
//! `application/octet-stream`, the same bytes the at-rest stream posts
//! (`src/broadcast_at_rest.rs`).

use bsv_sdk::primitives::sha256;
use bsv_sdk::transaction::Beef;
use rust_wallet_infra::broadcast_at_rest::atomic_subject;
use rust_wallet_infra::services::arc::arc_beef_request;

/// (row, sha256 at the replay, the plain row it carries behind its prefix).
const ROWS: [(&str, &str, Option<&str>); 6] = [
    (
        "example_atomic_payment",
        "b6b08e4c5f5252224bd89cc0c0c0147e3db390bd189e1d70d48a5218a2a224c9",
        Some("example"),
    ),
    (
        "example",
        "530b9a600ac45fa14ceeda12e42a57922fb1d70f94c2b61f30d3ccf92987a4d8",
        None,
    ),
    (
        "small_chain_atomic_true",
        "2d7b847053f7cbd7af3c17072130ad5faacba56fc3aac9c7b1fce7f6bd4e8c48",
        Some("small_chain_true"),
    ),
    (
        "small_chain_true",
        "6b5eb2237845c56bad1761dc4ab4061142868956b5ff96428ded7285aac5ea4f",
        None,
    ),
    (
        "lone_atomic",
        "4fc97f61e5c2598b1971d3724d358cf5216fd64be4e67a770ff30cbd188ce3bc",
        Some("lone"),
    ),
    (
        "lone",
        "38656f4c184e754b42b8a30cfc09c959b971a971744db34456d3da3a741fbb50",
        None,
    ),
];

fn row(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/tests/fixtures/broadcast-body/{name}.bin",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// The bytes ARC dispatches on for a body of a content type
/// (`[SRC] bitcoin-sv/arc@e7efc5b internal/api/handler/parsers.go:34-61`).
fn bytes_arc_reads(body: &[u8], content_type: &str) -> Result<Vec<u8>, String> {
    if content_type.contains("text/plain") {
        let text = std::str::from_utf8(body).map_err(|e| e.to_string())?;
        hex::decode(text).map_err(|e| e.to_string())
    } else if content_type.contains("application/json") {
        let json: serde_json::Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
        let raw = json["rawTx"].as_str().ok_or("no rawTx")?;
        hex::decode(raw).map_err(|e| e.to_string())
    } else if content_type.contains("application/octet-stream") {
        Ok(body.to_vec())
    } else {
        Err(format!("HTTP 400: content type {content_type}"))
    }
}

#[test]
fn the_rows_are_the_replays() {
    for (name, sha, _) in ROWS {
        assert_eq!(hex::encode(sha256(&row(name))), sha, "{name}");
    }
}

/// For every row, the bytes ARC reads from the request are a BEEF by ARC's
/// test (bytes 2 and 3 `BE EF`), a version `0100BEEF` or `0200BEEF`, never the
/// prefix `01010101`; an AtomicBEEF's are its plain row byte for byte (the
/// row the replay parses), and they still hold the subject the prefix named;
/// a plain row's are itself.
#[test]
fn arc_is_handed_a_plain_beef_for_every_row() {
    let mut failures = Vec::new();
    for (name, _, plain) in ROWS {
        let bytes = row(name);
        let (body, content_type) = arc_beef_request(&hex::encode(&bytes)).unwrap();
        let read = bytes_arc_reads(&body, content_type).unwrap();
        let lead = hex::encode(&read[..4]);
        if lead != "0100beef" && lead != "0200beef" {
            failures.push(format!(
                "{name}: ARC reads leading bytes {lead}, not a BEEF"
            ));
            continue;
        }
        let expected = plain.map(row).unwrap_or_else(|| bytes.clone());
        if read != expected {
            failures.push(format!(
                "{name}: ARC reads {} bytes, not the plain row",
                read.len()
            ));
            continue;
        }
        let beef = Beef::from_binary(&read).unwrap_or_else(|e| panic!("{name}: {e}"));
        if plain.is_some() {
            let subject = atomic_subject(&bytes[..36]).unwrap();
            if beef.find_txid(&subject).is_none() {
                failures.push(format!("{name}: the subject {subject} is not in the BEEF"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The BEEF goes as bytes, `application/octet-stream`, the form the at-rest
/// stream posts; never hex in a JSON body.
#[test]
fn the_beef_goes_as_octet_stream() {
    for (name, _, _) in ROWS {
        let bytes = row(name);
        let (body, content_type) = arc_beef_request(&hex::encode(&bytes)).unwrap();
        assert_eq!(content_type, "application/octet-stream", "{name}");
        assert!(
            body.len() <= bytes.len(),
            "{name}: {} bytes for {}",
            body.len(),
            bytes.len()
        );
    }
}

/// Bytes that are not hex never reach ARC.
#[test]
fn a_body_that_is_not_hex_is_not_posted() {
    assert!(arc_beef_request("zz").is_err());
}

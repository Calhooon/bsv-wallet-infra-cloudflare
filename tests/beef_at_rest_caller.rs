//! NL-7c, the witness of the caller on the object: a reference to bytes at
//! rest is honoured only for the caller the object names. The relay writes
//! the recipient's identity key on every object it spools as the custom
//! metadata `recipient-identity-key` (NL-7b); wallet-infra reads it from the
//! object's head, before the body, and refuses a missing or different key by
//! reason, with no body read and no copy. The checks are held in order: the
//! caller first, so a refusal never says another caller's object's etag or
//! size, and never says whose it is.

use std::collections::HashMap;

use rust_wallet_infra::beef_at_rest::{hold_to, BeefAtRest, RECIPIENT_IDENTITY_KEY};

const CALLER: &str = "02aa6b1e3c7d0f4b8a9c2d5e7f1a3b5c7d9e0f2a4b6c8d0e1f3a5b7c9d1e3f5a7b";
const OTHER: &str = "03bb0c2d4e6f8a1b3c5d7e9f0a2b4c6d8e0f1a3b5c7d9e1f2a4b6c8d0e2f4a6b8c";

fn reference() -> BeefAtRest {
    BeefAtRest {
        r2_key: "02bb/0b5e.beef".to_string(),
        size: 6_200_098,
        etag: "e-1".to_string(),
        bucket: "bsv-messagebox-beefs".to_string(),
    }
}

fn naming(key: Option<&str>) -> HashMap<String, String> {
    key.map(|k| HashMap::from([(RECIPIENT_IDENTITY_KEY.to_string(), k.to_string())]))
        .unwrap_or_default()
}

#[test]
fn the_metadata_key_is_the_one_the_relay_writes() {
    assert_eq!(RECIPIENT_IDENTITY_KEY, "recipient-identity-key");
}

#[test]
fn the_caller_the_object_names_is_honoured() {
    let r = reference();
    hold_to(&r, "e-1", r.size, &naming(Some(CALLER)), CALLER).expect("the caller's own object");
    // The quoted etag of the HTTP form, and the key in capitals: the same key.
    hold_to(
        &r,
        "\"e-1\"",
        r.size,
        &naming(Some(&CALLER.to_uppercase())),
        CALLER,
    )
    .expect("the same key in capitals");
}

#[test]
fn a_caller_naming_another_callers_object_is_refused_by_reason() {
    let r = reference();
    let e = hold_to(&r, "e-1", r.size, &naming(Some(OTHER)), CALLER).unwrap_err();
    let words = e.to_string();
    println!("another's: {words}");
    assert!(words.contains("names another recipient"), "{words}");
    assert!(
        !words.contains(OTHER),
        "the refusal says whose the object is: {words}"
    );
}

#[test]
fn an_object_that_names_no_recipient_is_refused_by_reason() {
    let r = reference();
    for metadata in [
        naming(None),
        naming(Some("")),
        HashMap::from([("recipient".to_string(), CALLER.to_string())]),
    ] {
        let e = hold_to(&r, "e-1", r.size, &metadata, CALLER).unwrap_err();
        let words = e.to_string();
        println!("none: {words}");
        assert!(words.contains("names no recipient"), "{words}");
    }
}

#[test]
fn the_caller_is_held_before_the_etag_and_the_size() {
    // Another's object at another etag and size: the refusal is the caller's,
    // and says nothing of the object's etag or size.
    let r = reference();
    let e = hold_to(&r, "e-2", r.size + 7, &naming(Some(OTHER)), CALLER).unwrap_err();
    let words = e.to_string();
    assert!(words.contains("names another recipient"), "{words}");
    assert!(
        !words.contains("e-2") && !words.contains(&(r.size + 7).to_string()),
        "{words}"
    );
    // The caller's own object is then held to the etag and the size as before.
    let e = hold_to(&r, "e-2", r.size, &naming(Some(CALLER)), CALLER).unwrap_err();
    assert!(e.to_string().contains("another upload is at"), "{e}");
    let e = hold_to(&r, "e-1", r.size + 1, &naming(Some(CALLER)), CALLER).unwrap_err();
    assert!(e.to_string().contains("bytes, the reference names"), "{e}");
}

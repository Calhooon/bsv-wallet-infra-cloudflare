//! The digest this Worker signs with (bsv-rs 0.7.0, bsv-stack-lean wave W23).
//!
//! The Worker signs no transaction: the client's wallet signs every input,
//! so bsv-rs 0.7.0's signing-digest change (`SigningContext::compute_sighash`
//! dispatching to the node's original digest for a type without FORKID or
//! with CHRONICLE) reaches no code here. Its signatures are BRC-103 auth
//! messages, all made by core's `sign_message` over `signing_data()`; the
//! one on every response is the one `sign_json_response` makes: the adapter builds the
//! response payload and core's `sign_http_response` signs it through
//! `ProtoWallet::create_signature` with `data`, so the digest is ONE SHA-256
//! of the payload under the BRC-42 key for (`auth message signature`,
//! counterparty level, `"{nonce} {peer nonce}"`). `worker::Response` needs the
//! JS runtime, so this drives the core function the adapter calls.

use bsv_middleware_cloudflare::core::auth::{sign_http_response, SessionBinding};
use bsv_middleware_cloudflare::core::HttpResponseData;
use bsv_sdk::auth::types::AUTH_PROTOCOL_ID;
use bsv_sdk::primitives::{sha256, PrivateKey};
use bsv_sdk::wallet::{
    Counterparty, CreateSignatureArgs, ProtoWallet, Protocol, SecurityLevel, VerifySignatureArgs,
};

const SERVER_KEY_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const CLIENT_KEY_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000002";
const PEER_NONCE: &str = "cGVlci1oYW5kc2hha2Utbm9uY2U=";

/// The payload of a fixed response: request id 0x11 x 32, status 200, no
/// signable headers, the body `{"jsonrpc":"2.0","result":true,"id":1}`.
fn fixed_response() -> HttpResponseData {
    HttpResponseData {
        request_id: [0x11; 32],
        status: 200,
        headers: vec![],
        body: br#"{"jsonrpc":"2.0","result":true,"id":1}"#.to_vec(),
    }
}

/// SHA-256 of the fixed response's BRC-104 payload (`11` x 32, `c8`, `00`,
/// `26`, the body), recomputed outside the crate from the bytes by hand.
const FIXED_PAYLOAD_SHA256: &str =
    "b31efaabff79fa621801c82095cc391c7bbe12332ea3d835afd75a1ee628249a";

fn wallet(hex: &str) -> ProtoWallet {
    ProtoWallet::new(Some(PrivateKey::from_hex(hex).unwrap()))
}

fn auth_protocol() -> Protocol {
    Protocol::new(SecurityLevel::Counterparty, AUTH_PROTOCOL_ID)
}

#[test]
fn the_response_signature_is_over_one_sha256_of_the_payload() {
    let server = wallet(SERVER_KEY_HEX);
    let client_key = PrivateKey::from_hex(CLIENT_KEY_HEX).unwrap().public_key();
    let server_key = PrivateKey::from_hex(SERVER_KEY_HEX).unwrap().public_key();
    let session = SessionBinding::new(
        "c2Vzc2lvbi1ub25jZQ==",
        client_key.to_hex(),
        Some(PEER_NONCE.to_string()),
    );
    let response = fixed_response();
    let payload = response.to_payload();
    assert_eq!(hex::encode(sha256(&payload)), FIXED_PAYLOAD_SHA256);

    let msg = sign_http_response(&server, &session, &response).unwrap();
    assert_eq!(msg.payload.as_deref(), Some(payload.as_slice()));
    let signature = msg.signature.clone().unwrap();
    let key_id = msg.get_key_id(Some(PEER_NONCE));

    // The same key signing the digest directly: RFC 6979 makes the two
    // signatures byte-equal exactly when the digest is sha256(payload).
    let sign_digest = |digest: [u8; 32]| {
        server
            .create_signature(CreateSignatureArgs {
                data: None,
                hash_to_directly_sign: Some(digest),
                protocol_id: auth_protocol(),
                key_id: key_id.clone(),
                counterparty: Some(Counterparty::Other(client_key.clone())),
            })
            .unwrap()
            .signature
    };
    assert_eq!(signature, sign_digest(sha256(&payload)));
    assert_ne!(signature, sign_digest(sha256(&sha256(&payload))));

    // The client verifies it over the payload with the server as counterparty.
    let verified = wallet(CLIENT_KEY_HEX)
        .verify_signature(VerifySignatureArgs {
            data: Some(payload),
            hash_to_directly_verify: None,
            signature,
            protocol_id: auth_protocol(),
            key_id,
            counterparty: Some(Counterparty::Other(server_key)),
            for_self: None,
        })
        .unwrap();
    assert!(verified.valid);
}

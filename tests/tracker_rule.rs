//! Pure local-script witnesses: policy and missing context are never verdicts.
use bsv_sdk::script::{LockingScript, UnlockingScript};
use bsv_sdk::transaction::{Transaction, TransactionInput, TransactionOutput};
use rust_wallet_infra::storage::tracker_rule::script_refusal;

fn subject(unlock: &str) -> Transaction {
    Transaction::with_params(
        1,
        vec![TransactionInput {
            source_transaction: None,
            source_txid: Some("ab".repeat(32)),
            source_output_index: 0,
            unlocking_script: Some(UnlockingScript::from_hex(unlock).unwrap()),
            unlocking_script_template: None,
            sequence: u32::MAX,
        }],
        vec![output("51")],
        0,
    )
}
fn output(script: &str) -> TransactionOutput {
    TransactionOutput {
        satoshis: Some(1000),
        locking_script: LockingScript::from_hex(script).unwrap(),
        change: false,
    }
}
#[test]
fn false_source_script_is_a_local_refusal() {
    assert!(script_refusal(&subject(""), &[output("00")]).is_some());
}
#[test]
fn true_script_is_not_a_refusal() {
    assert!(script_refusal(&subject(""), &[output("51")]).is_none());
}
#[test]
fn policy_extra_stack_is_not_a_consensus_refusal() {
    assert!(script_refusal(&subject("51"), &[output("51")]).is_none());
}
#[test]
fn missing_sources_are_no_verdict() {
    assert!(script_refusal(&subject(""), &[]).is_none());
}

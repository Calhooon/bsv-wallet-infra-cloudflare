-- Initial migration for wallet storage
-- Creates all 18 tables matching the TypeScript schema
-- Compatible with Cloudflare D1 (SQLite)

-- ============================================================================
-- proven_txs - Transactions with merkle proofs
-- ============================================================================
CREATE TABLE IF NOT EXISTS proven_txs (
    proven_tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txid TEXT NOT NULL UNIQUE,
    height INTEGER NOT NULL,
    idx INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    merkle_root TEXT NOT NULL,
    merkle_path BLOB NOT NULL,
    raw_tx BLOB NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_proven_txs_block_hash ON proven_txs(block_hash);

-- ============================================================================
-- proven_tx_reqs - Requests for merkle proofs
-- ============================================================================
CREATE TABLE IF NOT EXISTS proven_tx_reqs (
    proven_tx_req_id INTEGER PRIMARY KEY AUTOINCREMENT,
    proven_tx_id INTEGER REFERENCES proven_txs(proven_tx_id),
    status TEXT NOT NULL DEFAULT 'unknown',
    attempts INTEGER NOT NULL DEFAULT 0,
    notified INTEGER NOT NULL DEFAULT 0,
    txid TEXT NOT NULL UNIQUE,
    batch TEXT,
    history TEXT NOT NULL DEFAULT '{}',
    notify TEXT NOT NULL DEFAULT '{}',
    raw_tx BLOB NOT NULL,
    input_beef BLOB,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_proven_tx_reqs_status ON proven_tx_reqs(status);
CREATE INDEX IF NOT EXISTS idx_proven_tx_reqs_batch ON proven_tx_reqs(batch);
CREATE INDEX IF NOT EXISTS idx_proven_tx_reqs_txid ON proven_tx_reqs(txid);

-- ============================================================================
-- users - Wallet users
-- ============================================================================
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_key TEXT NOT NULL UNIQUE,
    active_storage TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- certificates - Identity certificates
-- ============================================================================
CREATE TABLE IF NOT EXISTS certificates (
    certificate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    serial_number TEXT NOT NULL,
    type TEXT NOT NULL,
    certifier TEXT NOT NULL,
    subject TEXT NOT NULL,
    verifier TEXT,
    revocation_outpoint TEXT NOT NULL,
    signature TEXT NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, type, certifier, serial_number)
);

-- ============================================================================
-- certificate_fields - Certificate field values
-- ============================================================================
CREATE TABLE IF NOT EXISTS certificate_fields (
    certificate_field_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    certificate_id INTEGER NOT NULL REFERENCES certificates(certificate_id),
    field_name TEXT NOT NULL,
    field_value TEXT NOT NULL,
    master_key TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(field_name, certificate_id)
);

-- ============================================================================
-- output_baskets - Organizes outputs into baskets
-- ============================================================================
CREATE TABLE IF NOT EXISTS output_baskets (
    basket_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    name TEXT NOT NULL,
    number_of_desired_utxos INTEGER NOT NULL DEFAULT 6,
    minimum_desired_utxo_value INTEGER NOT NULL DEFAULT 10000,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, user_id)
);

-- ============================================================================
-- transactions - Transaction records
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    proven_tx_id INTEGER REFERENCES proven_txs(proven_tx_id),
    status TEXT NOT NULL,
    reference TEXT NOT NULL UNIQUE,
    is_outgoing INTEGER NOT NULL,
    satoshis INTEGER NOT NULL DEFAULT 0,
    version INTEGER,
    lock_time INTEGER,
    description TEXT NOT NULL,
    txid TEXT,
    input_beef BLOB,
    raw_tx BLOB,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_txid ON transactions(txid);

-- ============================================================================
-- commissions - Commission records for transactions
-- ============================================================================
CREATE TABLE IF NOT EXISTS commissions (
    commission_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    transaction_id INTEGER NOT NULL UNIQUE REFERENCES transactions(transaction_id),
    satoshis INTEGER NOT NULL,
    key_offset TEXT NOT NULL,
    is_redeemed INTEGER NOT NULL DEFAULT 0,
    locking_script BLOB NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_commissions_transaction_id ON commissions(transaction_id);

-- ============================================================================
-- outputs - UTXOs and spent outputs
-- ============================================================================
CREATE TABLE IF NOT EXISTS outputs (
    output_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    transaction_id INTEGER NOT NULL REFERENCES transactions(transaction_id),
    basket_id INTEGER REFERENCES output_baskets(basket_id),
    spendable INTEGER NOT NULL DEFAULT 0,
    change INTEGER NOT NULL DEFAULT 0,
    vout INTEGER NOT NULL,
    satoshis INTEGER NOT NULL,
    provided_by TEXT NOT NULL,
    purpose TEXT NOT NULL,
    type TEXT NOT NULL,
    output_description TEXT,
    txid TEXT,
    sender_identity_key TEXT,
    derivation_prefix TEXT,
    derivation_suffix TEXT,
    custom_instructions TEXT,
    spent_by INTEGER REFERENCES transactions(transaction_id),
    sequence_number INTEGER,
    spending_description TEXT,
    script_length INTEGER,
    script_offset INTEGER,
    locking_script BLOB,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(transaction_id, vout, user_id)
);

CREATE INDEX IF NOT EXISTS idx_outputs_spendable ON outputs(spendable);

-- ============================================================================
-- output_tags - Tags for labeling outputs
-- ============================================================================
CREATE TABLE IF NOT EXISTS output_tags (
    output_tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    tag TEXT NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tag, user_id)
);

-- ============================================================================
-- output_tags_map - Many-to-many mapping between outputs and tags
-- ============================================================================
CREATE TABLE IF NOT EXISTS output_tags_map (
    output_tag_map_id INTEGER PRIMARY KEY AUTOINCREMENT,
    output_tag_id INTEGER NOT NULL REFERENCES output_tags(output_tag_id),
    output_id INTEGER NOT NULL REFERENCES outputs(output_id),
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(output_tag_id, output_id)
);

CREATE INDEX IF NOT EXISTS idx_output_tags_map_output_id ON output_tags_map(output_id);

-- ============================================================================
-- tx_labels - Transaction labels
-- ============================================================================
CREATE TABLE IF NOT EXISTS tx_labels (
    tx_label_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    label TEXT NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(label, user_id)
);

-- ============================================================================
-- tx_labels_map - Many-to-many mapping between transactions and labels
-- ============================================================================
CREATE TABLE IF NOT EXISTS tx_labels_map (
    tx_label_map_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_label_id INTEGER NOT NULL REFERENCES tx_labels(tx_label_id),
    transaction_id INTEGER NOT NULL REFERENCES transactions(transaction_id),
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tx_label_id, transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_tx_labels_map_transaction_id ON tx_labels_map(transaction_id);

-- ============================================================================
-- monitor_events - System monitoring events
-- ============================================================================
CREATE TABLE IF NOT EXISTS monitor_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    details TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_monitor_events_event ON monitor_events(event);

-- ============================================================================
-- settings - Singleton settings table
-- ============================================================================
CREATE TABLE IF NOT EXISTS settings (
    settings_id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_identity_key TEXT NOT NULL,
    storage_name TEXT NOT NULL,
    chain TEXT NOT NULL,
    dbtype TEXT NOT NULL,
    max_output_script INTEGER NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- sync_states - Synchronization state between storages
-- ============================================================================
CREATE TABLE IF NOT EXISTS sync_states (
    sync_state_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    storage_identity_key TEXT NOT NULL DEFAULT '',
    storage_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unknown',
    init INTEGER NOT NULL DEFAULT 0,
    ref_num TEXT NOT NULL UNIQUE,
    sync_map TEXT NOT NULL,
    when_last_sync_started DATETIME,
    satoshis INTEGER,
    error_local TEXT,
    error_other TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_states_status ON sync_states(status);
CREATE INDEX IF NOT EXISTS idx_sync_states_ref_num ON sync_states(ref_num);

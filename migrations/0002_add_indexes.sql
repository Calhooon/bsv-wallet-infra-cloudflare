-- Add composite indexes for common query patterns.
-- All viewer and agent queries filter by user_id first — without these,
-- every listOutputs/listActions does a full table scan.

-- outputs: listOutputs filters (user_id, spendable) on every call
CREATE INDEX IF NOT EXISTS idx_outputs_user_spendable ON outputs(user_id, spendable);

-- transactions: listActions filters (user_id, status) on every call
CREATE INDEX IF NOT EXISTS idx_transactions_user_status ON transactions(user_id, status);

-- output_baskets: basket lookup by (user_id, name) on every listOutputs
CREATE INDEX IF NOT EXISTS idx_output_baskets_user_name ON output_baskets(user_id, name, is_deleted);

-- tx_labels: resolve_label_ids filters by (user_id, is_deleted) on every listActions with labels
CREATE INDEX IF NOT EXISTS idx_tx_labels_user ON tx_labels(user_id, is_deleted);

-- output_tags: resolve_tag_ids filters by (user_id, is_deleted) on every listOutputs with tags
CREATE INDEX IF NOT EXISTS idx_output_tags_user ON output_tags(user_id, is_deleted);

-- 0004: covering index for the deep reorg sweep (monitor task 11).
--
-- deep_reorg_sweep pages DISTINCT (height, block_hash, merkle_root) groups in
-- ascending height above a cursor every cron cycle. Without this index each
-- run scans all of proven_txs; with it the GROUP BY ... ORDER BY height LIMIT
-- walks the index and stops after the page. The sweep is correct either way.
--
-- Safe on the LIVE database: additive, IF NOT EXISTS, no table rewrite.
CREATE INDEX IF NOT EXISTS idx_proven_txs_height_pair
    ON proven_txs(height, block_hash, merkle_root);

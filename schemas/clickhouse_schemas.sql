
CREATE TABLE IF NOT EXISTS `7700_logs`
(
    `log_index` Int32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` Int32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` Int32,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, address, block_number)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS 7700_receipts
                     (
                         `transaction_hash` String CODEC(ZSTD(1)),
                         `transaction_index` Int32,
                         `block_hash` String CODEC(ZSTD(1)),
                         `block_number` Int32,
                         `cumulative_gas_used` Int64,
                         `gas_used` Int64,
                         `contract_address` String CODEC(ZSTD(1)),
                         `root` String CODEC(ZSTD(1)),
                         `status` Int32,
                         `effective_gas_price` Int64
                     )
                          ENGINE = ReplacingMergeTree
                          ORDER BY (block_number, transaction_hash, contract_address)
                          SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS 7700_transactions
                     (
                         `hash` String CODEC(ZSTD(1)),
                         `nonce` Int64,
                         `block_hash` String CODEC(ZSTD(1)),
                         `block_number` Int32,
                         `transaction_index` Int32,
                         `from_address` String CODEC(ZSTD(1)),
                         `to_address` Nullable(String) CODEC(ZSTD(1)),
                         `value` Decimal(38, 0),
                         `gas` Int64,
                         `gas_price` Int64,
                         `input` String CODEC(ZSTD(1)),
                         `block_timestamp` Int32,
                         `max_fee_per_gas` Nullable(Int64),
                         `max_priority_fee_per_gas` Nullable(Int64),
                         `transaction_type` Int32
                     )
                          ENGINE = ReplacingMergeTree
                          PARTITION BY toYYYYMMDD(FROM_UNIXTIME(block_timestamp))
                          ORDER BY (block_timestamp, hash)
                          SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS 7700_blocks
                     (
                         `number`            Int32,
                         `hash`              String CODEC(ZSTD(1)),
                         `parent_hash`       String CODEC(ZSTD(1)),
                         `nonce`             String CODEC(ZSTD(1)),
                         `sha3_uncles`       String CODEC(ZSTD(1)),
                         `logs_bloom`        String CODEC(ZSTD(1)),
                         `transactions_root` String CODEC(ZSTD(1)),
                         `state_root`        String CODEC(ZSTD(1)),
                         `receipts_root`     String CODEC(ZSTD(1)),
                         `miner`             String CODEC(ZSTD(1)),
                         `difficulty`        Decimal(38, 0),
                         `total_difficulty`  Decimal(38, 0),
                         `size`              Int64,
                         `extra_data`        String CODEC(ZSTD(1)),
                         `gas_limit`         Int64,
                         `gas_used`          Int64,
                         `timestamp`         Int32,
                         `transaction_count` Int64,
                         `base_fee_per_gas`  Int64
                     )
                          ENGINE = ReplacingMergeTree
                          PARTITION BY toYYYYMMDD(FROM_UNIXTIME(timestamp))
                          ORDER BY (number, timestamp)
                          SETTINGS index_granularity = 8192;


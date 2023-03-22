CREATE TABLE IF NOT EXISTS `${chain_id}_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (transaction_hash, log_index, address, block_number);


CREATE TABLE IF NOT EXISTS `${chain_id}_receipts`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `cumulative_gas_used` UInt64,
    `gas_used` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `root` String CODEC(ZSTD(1)),
    `status` UInt32,
    `effective_gas_price` UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (transaction_hash, contract_address);

CREATE TABLE IF NOT EXISTS `${chain_id}_transactions`
(
     `hash` String CODEC(ZSTD(1)),
     `nonce` UInt64,
     `block_hash` String CODEC(ZSTD(1)),
     `block_number` UInt64,
     `transaction_index` UInt32,
     `from_address` String CODEC(ZSTD(1)),
     `to_address` Nullable(String) CODEC(ZSTD(1)),
     `value` UInt256,
     `gas` UInt64,
     `gas_price` UInt64,
     `input` String CODEC(ZSTD(1)),
     `block_timestamp` UInt32,
     `max_fee_per_gas` Nullable(Int64),
     `max_priority_fee_per_gas` Nullable(Int64),
     `transaction_type` UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (hash);

CREATE TABLE IF NOT EXISTS `${chain_id}_blocks`
(
    `number`            UInt64 CODEC(Delta(8), LZ4),
    `hash`              String CODEC(ZSTD(1)),
    `parent_hash`       String CODEC(ZSTD(1)),
    `nonce`             String CODEC(ZSTD(1)),
    `sha3_uncles`       String CODEC(ZSTD(1)),
    `logs_bloom`        String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root`        String CODEC(ZSTD(1)),
    `receipts_root`     String CODEC(ZSTD(1)),
    `miner`             String CODEC(ZSTD(1)),
    `difficulty`        UInt64,
    `total_difficulty`  UInt64,
    `size`              UInt64,
    `extra_data`        String CODEC(ZSTD(1)),
    `gas_limit`         UInt64,
    `gas_used`          UInt64,
    `timestamp`         UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas`  Nullable(Int64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(timestamp))
ORDER BY (number);

CREATE TABLE IF NOT EXISTS `${chain_id}_token_transfers`
(
    `token_address` String CODEC(ZSTD(1)),
    `from_address` String CODEC(ZSTD(1)),
    `to_address` String CODEC(ZSTD(1)),
    `value` UInt256,
    `transaction_hash` String CODEC(ZSTD(1)),
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS `${chain_id}_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `from_address` String CODEC(ZSTD(1)),
    `to_address` String CODEC(ZSTD(1)),
    `value` UInt256,
    `input` String CODEC(ZSTD(1)),
    `output` String CODEC(ZSTD(1)),
    `trace_type` String CODEC(ZSTD(1)),
    `call_type` String CODEC(ZSTD(1)),
    `reward_type` String CODEC(ZSTD(1)),
    `gas` UInt64,
    `gas_used` UInt64,
    `subtraces` UInt32,
    `trace_address` String CODEC(ZSTD(1)),
    `error` String CODEC(ZSTD(1)),
    `status` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1)),
    `trace_id` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(FROM_UNIXTIME(block_timestamp))
ORDER BY (trace_id);

CREATE TABLE IF NOT EXISTS `${chain_id}_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number);

CREATE TABLE IF NOT EXISTS `${chain_id}_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number);

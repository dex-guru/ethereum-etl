CREATE TABLE IF NOT EXISTS `${log}`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    PROJECTION _logs_hash
    (   SELECT *
        ORDER BY (transaction_hash)
    )

)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (address, transaction_hash, log_index)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;


CREATE TABLE IF NOT EXISTS `${transaction}`
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
    `gas_price` UInt256,
    `input` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32),

    PROJECTION _transactions_hash
    (   SELECT *
        ORDER BY (hash)
    )
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (block_number, block_timestamp, receipt_contract_address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;


CREATE TABLE IF NOT EXISTS `${transaction}_address`
(
    address                     String,
    hash                        String,
    nonce                       UInt64,
    block_hash                  String,
    block_number                UInt64,
    transaction_index           UInt32,
    from_address                String,
    to_address                  String,
    value                       UInt256,
    gas                         UInt64,
    gas_price                   UInt64,
    input                       String,
    block_timestamp             DateTime,
    max_fee_per_gas Nullable(Int64),
    max_priority_fee_per_gas Nullable(Int64),
    transaction_type            UInt32,
    receipt_cumulative_gas_used UInt64,
    receipt_gas_used            UInt64,
    receipt_contract_address    String,
    receipt_root Nullable(String),
    receipt_status              UInt32,
    receipt_effective_gas_price UInt256,
    receipt_logs_count Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
PARTITION BY substring(address, 1, 3)
ORDER BY (address, block_timestamp, hash);


CREATE MATERIALIZED VIEW IF NOT EXISTS `${transaction}_by_from_address_mv`
            TO `${transaction}_address`
            (
             `address` String,
             `hash` String,
             `nonce` UInt64,
             `block_hash` String,
             `block_number` UInt64,
             `transaction_index` UInt32,
             `from_address` String,
             `to_address` String,
             `value` UInt256,
             `gas` UInt64,
             `gas_price` UInt64,
             `input` String,
             `block_timestamp` DateTime,
             `max_fee_per_gas` Nullable(Int64),
             `max_priority_fee_per_gas` Nullable(Int64),
             `transaction_type` UInt32,
             `receipt_cumulative_gas_used` UInt64,
             `receipt_gas_used` UInt64,
             `receipt_contract_address` String,
             `receipt_root` Nullable(String),
             `receipt_status` UInt32,
             `receipt_effective_gas_price` UInt256,
             `receipt_logs_count` Nullable(UInt32)
                )
AS
SELECT from_address                AS address,
       hash,
       nonce,
       block_hash,
       block_number,
       transaction_index,
       from_address,
       ifNull(to_address, '')      AS to_address,
       value,
       gas,
       gas_price,
       input,
       toDateTime(block_timestamp) AS block_timestamp,
       max_fee_per_gas,
       max_priority_fee_per_gas,
       transaction_type,
       receipt_cumulative_gas_used,
       receipt_gas_used,
       ifNull(receipt_contract_address, '') AS receipt_contract_address,
       receipt_root,
       receipt_status,
       receipt_effective_gas_price,
       receipt_logs_count
FROM `${transaction}`
WHERE from_address IS NOT NULL;


CREATE MATERIALIZED VIEW IF NOT EXISTS `${transaction}_by_to_address_mv`
            TO `${transaction}_address`
            (
             `address` Nullable(String),
             `hash` String,
             `nonce` UInt64,
             `block_hash` String,
             `block_number` UInt64,
             `transaction_index` UInt32,
             `from_address` String,
             `to_address` String,
             `value` UInt256,
             `gas` UInt64,
             `gas_price` UInt64,
             `input` String,
             `block_timestamp` DateTime,
             `max_fee_per_gas` Nullable(Int64),
             `max_priority_fee_per_gas` Nullable(Int64),
             `transaction_type` UInt32,
             `receipt_cumulative_gas_used` UInt64,
             `receipt_gas_used` UInt64,
             `receipt_contract_address` String,
             `receipt_root` Nullable(String),
             `receipt_status` UInt32,
             `receipt_effective_gas_price` UInt256,
             `receipt_logs_count` Nullable(UInt32)
                )
AS
SELECT to_address                  AS address,
       hash,
       nonce,
       block_hash,
       block_number,
       transaction_index,
       from_address,
       ifNull(to_address, '') AS to_address,
       value,
       gas,
       gas_price,
       input,
       toDateTime(block_timestamp) AS block_timestamp,
       max_fee_per_gas,
       max_priority_fee_per_gas,
       transaction_type,
       receipt_cumulative_gas_used,
       receipt_gas_used,
       ifNull(receipt_contract_address, '') AS receipt_contract_address,
       receipt_root,
       receipt_status,
       receipt_effective_gas_price,
       receipt_logs_count
FROM `${transaction}`
WHERE to_address IS NOT NULL;


CREATE TABLE IF NOT EXISTS `${block}`
(
    `number`            UInt64 CODEC(Delta(8), LZ4),
    `hash`              String CODEC(ZSTD(1)),
    `parent_hash`       String CODEC(ZSTD(1)),
    `nonce`             Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles`       String CODEC(ZSTD(1)),
    `logs_bloom`        String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root`        String CODEC(ZSTD(1)),
    `receipts_root`     String CODEC(ZSTD(1)),
    `miner`             String CODEC(ZSTD(1)),
    `difficulty`        UInt256,
    `total_difficulty`  UInt256,
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
ORDER BY (timestamp, number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS `${token_transfer}`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `from_address` String CODEC(ZSTD(1)),
    `to_address` String CODEC(ZSTD(1)),
    `value` UInt256,
    `transaction_hash` String CODEC(ZSTD(1)),
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1)),
    `operator_address` Nullable(String) CODEC(ZSTD(1)), // ERC721, ERC1155
    `token_id` Nullable(UInt256),  // ERC721, ERC1155
    `is_nft` Bool MATERIALIZED isNotNull(token_id), // ERC721, ERC1155
    PROJECTION _transfers_token_address
    (   SELECT *
        ORDER BY (token_address, is_nft)
    ),
    PROJECTION _transfers_from_address
    (   SELECT *
        ORDER BY (from_address, is_nft)
    ),
    PROJECTION _transfers_to_address
    (   SELECT *
        ORDER BY (to_address, is_nft)
    )
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key=1;

CREATE TABLE IF NOT EXISTS `${token_balance}`
(
    `token_address` String CODEC(ZSTD),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD),
    `token_id` UInt256 CODEC(ZSTD)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key=1;

CREATE TABLE IF NOT EXISTS `${trace}`
(
    `transaction_hash` Nullable(String) CODEC(ZSTD(1)),
    `transaction_index` Nullable(UInt32),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` String CODEC(ZSTD(1)),
    `value` UInt256,
    `input` Nullable(String) CODEC(ZSTD(1)),
    `output` Nullable(String) CODEC(ZSTD(1)),
    `trace_type` String CODEC(ZSTD(1)),
    `call_type` Nullable(String) CODEC(ZSTD(1)),
    `reward_type` Nullable(String) CODEC(ZSTD(1)),
    `gas` Nullable(UInt64),
    `gas_used` Nullable(UInt64),
    `subtraces` UInt32,
    `trace_address` Array(UInt64) CODEC(ZSTD(1)),
    `error` Nullable(String) CODEC(ZSTD(1)),
    `status` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1)),
    `trace_id` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(FROM_UNIXTIME(block_timestamp))
ORDER BY (trace_id);

CREATE TABLE IF NOT EXISTS `${token}`
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

CREATE TABLE IF NOT EXISTS `${contract}`
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

CREATE TABLE IF NOT EXISTS `${error}`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta, LZ4),
    `block_number` UInt64 CODEC(Delta, LZ4),
    `block_timestamp` UInt32 CODEC(Delta, LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY (timestamp);


CREATE TABLE IF NOT EXISTS `${geth_trace}`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY substring(transaction_hash, 1, 3)
ORDER BY (transaction_hash, block_number);


CREATE TABLE IF NOT EXISTS `${internal_transfer}`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY substring(transaction_hash, 1, 3)
ORDER BY (transaction_hash, block_number, id);

CREATE TABLE IF NOT EXISTS etl_delay
(
    `entity_type` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` Int32
)
ENGINE = MergeTree
ORDER BY (block_number)
TTL indexed_at + INTERVAL 3 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS etl_delay_${block}_mv
TO etl_delay
AS
SELECT
    number as block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    ${chain_id} AS chain_id,
    'block' AS entity_type
FROM `${block}`;


CREATE MATERIALIZED VIEW IF NOT EXISTS etl_delay_${transaction}_mv
TO etl_delay
AS
SELECT
    block_number,
    block_timestamp as timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    ${chain_id} AS chain_id,
    'transaction' AS entity_type
FROM `${transaction}`;

CREATE MATERIALIZED VIEW IF NOT EXISTS etl_delay_${geth_trace}_mv
TO etl_delay
AS
SELECT
    block_number,
    block_timestamp as timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    ${chain_id} AS chain_id,
    'geth_trace' AS entity_type
FROM `${geth_trace}`;

CREATE MATERIALIZED VIEW IF NOT EXISTS etl_delay_${internal_transfer}_mv
TO etl_delay
AS
SELECT
    block_number,
    block_timestamp as timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    ${chain_id} AS chain_id,
    'internal_transfer' AS entity_type
FROM `${internal_transfer}`;

CREATE TABLE IF NOT EXISTS `${native_balance}`
(
    `address` String CODEC(ZSTD),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD),
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number);

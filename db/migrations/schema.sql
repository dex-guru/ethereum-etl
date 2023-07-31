CREATE TABLE `1_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `1_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `1_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `1_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `1_count_active_addresses_mv` TO `1_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    1 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `1_transactions`;

CREATE TABLE `1_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `1_count_uniq_contracts_mv` TO `1_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    1 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `1_logs`;

CREATE TABLE `1_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `1_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `1_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `1_event_inventory_mv` TO `1_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `1_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `1_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `1_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `1_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `1_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `1_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `1_internal_transfers_from_address_mv` TO `1_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `1_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `1_internal_transfers_to_address_mv` TO `1_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `1_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `1_logs_to_events_mv` TO `1_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `1_logs` AS logs
INNER JOIN `1_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `1_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `1_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `1_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `1_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `1_token_transfers_transaction_hash_mv` TO `1_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `1_token_transfers`;

CREATE TABLE `1_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `1_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `1_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `1_transactions_by_from_address_mv` TO `1_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `1_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `1_transactions_by_hash_mv` TO `1_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `1_transactions`;

CREATE MATERIALIZED VIEW `1_transactions_by_to_address_mv` TO `1_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `1_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `1_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `10_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `10_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `10_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `10_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `10_count_active_addresses_mv` TO `10_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    10 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `10_transactions`;

CREATE TABLE `10_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `10_count_uniq_contracts_mv` TO `10_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    10 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `10_logs`;

CREATE TABLE `10_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `10_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `10_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `10_event_inventory_mv` TO `10_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `10_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `10_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `10_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `10_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `10_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `10_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `10_internal_transfers_from_address_mv` TO `10_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `10_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `10_internal_transfers_to_address_mv` TO `10_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `10_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `10_logs_to_events_mv` TO `10_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `10_logs` AS logs
INNER JOIN `10_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `10_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `10_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `10_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `10_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `10_token_transfers_transaction_hash_mv` TO `10_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `10_token_transfers`;

CREATE TABLE `10_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `10_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `10_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `10_transactions_by_from_address_mv` TO `10_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `10_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `10_transactions_by_hash_mv` TO `10_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `10_transactions`;

CREATE MATERIALIZED VIEW `10_transactions_by_to_address_mv` TO `10_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `10_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `10_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `100_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `100_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `100_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `100_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `100_count_active_addresses_mv` TO `100_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    100 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `100_transactions`;

CREATE TABLE `100_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `100_count_uniq_contracts_mv` TO `100_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    100 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `100_logs`;

CREATE TABLE `100_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `100_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `100_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `100_event_inventory_mv` TO `100_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `100_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `100_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `100_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `100_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `100_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `100_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `100_internal_transfers_from_address_mv` TO `100_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `100_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `100_internal_transfers_to_address_mv` TO `100_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `100_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `100_logs_to_events_mv` TO `100_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `100_logs` AS logs
INNER JOIN `100_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `100_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `100_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `100_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `100_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `100_token_transfers_transaction_hash_mv` TO `100_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `100_token_transfers`;

CREATE TABLE `100_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `100_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `100_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `100_transactions_by_from_address_mv` TO `100_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `100_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `100_transactions_by_hash_mv` TO `100_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `100_transactions`;

CREATE MATERIALIZED VIEW `100_transactions_by_to_address_mv` TO `100_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `100_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `100_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `137_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `137_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `137_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `137_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `137_count_active_addresses_mv` TO `137_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    137 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `137_transactions`;

CREATE TABLE `137_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `137_count_uniq_contracts_mv` TO `137_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    137 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `137_logs`;

CREATE TABLE `137_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `137_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `137_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `137_event_inventory_mv` TO `137_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `137_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `137_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `137_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `137_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `137_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `137_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `137_internal_transfers_from_address_mv` TO `137_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `137_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `137_internal_transfers_to_address_mv` TO `137_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `137_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `137_logs_to_events_mv` TO `137_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `137_logs` AS logs
INNER JOIN `137_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `137_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `137_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `137_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `137_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `137_token_transfers_transaction_hash_mv` TO `137_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `137_token_transfers`;

CREATE TABLE `137_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `137_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `137_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `137_transactions_by_from_address_mv` TO `137_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `137_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `137_transactions_by_hash_mv` TO `137_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `137_transactions`;

CREATE MATERIALIZED VIEW `137_transactions_by_to_address_mv` TO `137_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `137_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `137_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `250_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `250_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `250_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `250_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `250_count_active_addresses_mv` TO `250_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    250 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `250_transactions`;

CREATE TABLE `250_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `250_count_uniq_contracts_mv` TO `250_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    250 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `250_logs`;

CREATE TABLE `250_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `250_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `250_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `250_event_inventory_mv` TO `250_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `250_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `250_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `250_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `250_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `250_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `250_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `250_internal_transfers_from_address_mv` TO `250_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `250_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `250_internal_transfers_to_address_mv` TO `250_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `250_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `250_logs_to_events_mv` TO `250_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `250_logs` AS logs
INNER JOIN `250_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `250_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `250_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `250_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `250_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `250_token_transfers_transaction_hash_mv` TO `250_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `250_token_transfers`;

CREATE TABLE `250_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `250_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `250_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `250_transactions_by_from_address_mv` TO `250_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `250_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `250_transactions_by_hash_mv` TO `250_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `250_transactions`;

CREATE MATERIALIZED VIEW `250_transactions_by_to_address_mv` TO `250_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `250_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `250_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42161_count_active_addresses_mv` TO `42161_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    42161 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `42161_transactions`;

CREATE TABLE `42161_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `42161_count_uniq_contracts_mv` TO `42161_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    42161 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `42161_logs`;

CREATE TABLE `42161_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `42161_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `42161_event_inventory_mv` TO `42161_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `42161_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `42161_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `42161_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42161_internal_transfers_from_address_mv` TO `42161_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `42161_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `42161_internal_transfers_to_address_mv` TO `42161_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `42161_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `42161_logs_to_events_mv` TO `42161_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `42161_logs` AS logs
INNER JOIN `42161_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `42161_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `42161_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `42161_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42161_token_transfers_transaction_hash_mv` TO `42161_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `42161_token_transfers`;

CREATE TABLE `42161_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `42161_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42161_transactions_by_from_address_mv` TO `42161_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `42161_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `42161_transactions_by_hash_mv` TO `42161_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `42161_transactions`;

CREATE MATERIALIZED VIEW `42161_transactions_by_to_address_mv` TO `42161_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `42161_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `42161_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42170_count_active_addresses_mv` TO `42170_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    42170 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `42170_transactions`;

CREATE TABLE `42170_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `42170_count_uniq_contracts_mv` TO `42170_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    42170 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `42170_logs`;

CREATE TABLE `42170_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `42170_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `42170_event_inventory_mv` TO `42170_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `42170_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `42170_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `42170_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42170_internal_transfers_from_address_mv` TO `42170_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `42170_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `42170_internal_transfers_to_address_mv` TO `42170_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `42170_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `42170_logs_to_events_mv` TO `42170_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `42170_logs` AS logs
INNER JOIN `42170_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `42170_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `42170_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `42170_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42170_token_transfers_transaction_hash_mv` TO `42170_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `42170_token_transfers`;

CREATE TABLE `42170_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `42170_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `42170_transactions_by_from_address_mv` TO `42170_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `42170_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `42170_transactions_by_hash_mv` TO `42170_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `42170_transactions`;

CREATE MATERIALIZED VIEW `42170_transactions_by_to_address_mv` TO `42170_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `42170_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `42170_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `56_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `56_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `56_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `56_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `56_count_active_addresses_mv` TO `56_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    56 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `56_transactions`;

CREATE TABLE `56_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `56_count_uniq_contracts_mv` TO `56_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    56 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `56_logs`;

CREATE TABLE `56_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `56_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `56_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `56_event_inventory_mv` TO `56_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `56_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `56_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `56_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `56_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `56_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `56_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `56_internal_transfers_from_address_mv` TO `56_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `56_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `56_internal_transfers_to_address_mv` TO `56_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `56_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `56_logs_to_events_mv` TO `56_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `56_logs` AS logs
INNER JOIN `56_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `56_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `56_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `56_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `56_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `56_token_transfers_transaction_hash_mv` TO `56_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `56_token_transfers`;

CREATE TABLE `56_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `56_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `56_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `56_transactions_by_from_address_mv` TO `56_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `56_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `56_transactions_by_hash_mv` TO `56_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `56_transactions`;

CREATE MATERIALIZED VIEW `56_transactions_by_to_address_mv` TO `56_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `56_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `56_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7700_count_active_addresses_mv` TO `7700_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    7700 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `7700_transactions`;

CREATE TABLE `7700_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `7700_count_uniq_contracts_mv` TO `7700_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    7700 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `7700_logs`;

CREATE TABLE `7700_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `7700_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `7700_event_inventory_mv` TO `7700_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `7700_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `7700_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `7700_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7700_internal_transfers_from_address_mv` TO `7700_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `7700_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `7700_internal_transfers_to_address_mv` TO `7700_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `7700_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `7700_logs_to_events_mv` TO `7700_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `7700_logs` AS logs
INNER JOIN `7700_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `7700_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `7700_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `7700_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7700_token_transfers_transaction_hash_mv` TO `7700_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `7700_token_transfers`;

CREATE TABLE `7700_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `7700_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7700_transactions_by_from_address_mv` TO `7700_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `7700_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `7700_transactions_by_hash_mv` TO `7700_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `7700_transactions`;

CREATE MATERIALIZED VIEW `7700_transactions_by_to_address_mv` TO `7700_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `7700_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `7700_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7701_count_active_addresses_mv` TO `7701_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    7701 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `7701_transactions`;

CREATE TABLE `7701_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `7701_count_uniq_contracts_mv` TO `7701_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    7701 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `7701_logs`;

CREATE TABLE `7701_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `7701_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `7701_event_inventory_mv` TO `7701_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `7701_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `7701_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `7701_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7701_internal_transfers_from_address_mv` TO `7701_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `7701_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `7701_internal_transfers_to_address_mv` TO `7701_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `7701_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `7701_logs_to_events_mv` TO `7701_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `7701_logs` AS logs
INNER JOIN `7701_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `7701_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `7701_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `7701_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7701_token_transfers_transaction_hash_mv` TO `7701_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `7701_token_transfers`;

CREATE TABLE `7701_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `7701_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `7701_transactions_by_from_address_mv` TO `7701_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `7701_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `7701_transactions_by_hash_mv` TO `7701_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `7701_transactions`;

CREATE MATERIALIZED VIEW `7701_transactions_by_to_address_mv` TO `7701_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `7701_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `7701_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `8453_count_active_addresses_mv` TO `8453_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    8453 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `8453_transactions`;

CREATE TABLE `8453_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `8453_count_uniq_contracts_mv` TO `8453_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    8453 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `8453_logs`;

CREATE TABLE `8453_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `8453_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `8453_event_inventory_mv` TO `8453_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `8453_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `8453_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `8453_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `8453_internal_transfers_from_address_mv` TO `8453_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `8453_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `8453_internal_transfers_to_address_mv` TO `8453_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `8453_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `8453_logs_to_events_mv` TO `8453_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `8453_logs` AS logs
INNER JOIN `8453_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `8453_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `8453_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `8453_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `8453_token_transfers_transaction_hash_mv` TO `8453_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `8453_token_transfers`;

CREATE TABLE `8453_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `8453_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `8453_transactions_by_from_address_mv` TO `8453_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `8453_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `8453_transactions_by_hash_mv` TO `8453_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `8453_transactions`;

CREATE MATERIALIZED VIEW `8453_transactions_by_to_address_mv` TO `8453_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `8453_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `8453_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_blocks`
(
    `number` UInt64 CODEC(Delta(8), LZ4),
    `hash` String CODEC(ZSTD(1)),
    `parent_hash` String CODEC(ZSTD(1)),
    `nonce` Nullable(String) CODEC(ZSTD(1)),
    `sha3_uncles` String CODEC(ZSTD(1)),
    `logs_bloom` String CODEC(ZSTD(1)),
    `transactions_root` String CODEC(ZSTD(1)),
    `state_root` String CODEC(ZSTD(1)),
    `receipts_root` String CODEC(ZSTD(1)),
    `miner` String CODEC(ZSTD(1)),
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `size` UInt64,
    `extra_data` String CODEC(ZSTD(1)),
    `gas_limit` UInt64,
    `gas_used` UInt64,
    `timestamp` UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas` Nullable(Int64),
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY chain_id
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_contracts`
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_transactions`
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `84531_count_active_addresses_mv` TO `84531_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    84531 AS chain_id,
    uniqState(toNullable(from_address)) AS active_addresses
FROM `84531_transactions`;

CREATE TABLE `84531_logs`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `84531_count_uniq_contracts_mv` TO `84531_chain_counts`
(
    `chain_id` UInt32,
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT
    84531 AS chain_id,
    uniqState(toNullable(address)) AS uniq_contracts
FROM `84531_logs`;

CREATE TABLE `84531_errors`
(
    `item_id` String CODEC(ZSTD(1)),
    `timestamp` UInt32 CODEC(Delta(4), LZ4),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `block_timestamp` UInt32 CODEC(Delta(4), LZ4),
    `kind` LowCardinality(String),
    `data_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY timestamp
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `abi_types` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE TABLE `84531_event_inventory_src`
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `abi_type` String,
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, abi_type)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW `84531_event_inventory_mv` TO `84531_event_inventory`
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `abi_types` Array(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(abi_type) AS abi_types,
            event_signature,
            event_name,
            event_abi_json
        FROM `84531_event_inventory_src`
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `84531_event_inventory` AS dst USING (event_signature_hash_and_log_topic_count)
SETTINGS join_algorithm = 'direct';

CREATE TABLE `84531_events`
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `contract_address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `event_name` String CODEC(ZSTD(1)),
    `event_signature_hash` String ALIAS topics[1],
    `topic_count` UInt8 ALIAS toUInt8(length(topics))
)
ENGINE = ReplacingMergeTree
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, topics[1], transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_geth_traces`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_internal_transfers`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_internal_transfers_address`
(
    `address` String CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `from_address` Nullable(String) CODEC(ZSTD(1)),
    `to_address` Nullable(String) CODEC(ZSTD(1)),
    `value` UInt256 CODEC(ZSTD(1)),
    `gas_limit` UInt64 CODEC(ZSTD(1)),
    `id` String CODEC(ZSTD(1)),
    `block_hash` Nullable(String),
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `84531_internal_transfers_from_address_mv` TO `84531_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    from_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `84531_internal_transfers`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `84531_internal_transfers_to_address_mv` TO `84531_internal_transfers_address`
(
    `address` Nullable(String),
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String
) AS
SELECT
    to_address AS address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash
FROM `84531_internal_transfers`
WHERE to_address IS NOT NULL;

CREATE MATERIALIZED VIEW `84531_logs_to_events_mv` TO `84531_events`
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `data` String,
    `topics` Array(String),
    `contract_address` String,
    `event_name` LowCardinality(String)
) AS
SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `84531_logs` AS logs
INNER JOIN `84531_event_inventory` AS info ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm = 'direct';

CREATE TABLE `84531_native_balances`
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_token_balances`
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (token_address, holder_address, token_id, block_number)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `84531_token_transfers`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE `84531_token_transfers_transaction_hash`
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
    `operator_address` Nullable(String) CODEC(ZSTD(1)),
    `token_id` Nullable(UInt256),
    `is_nft` Bool MATERIALIZED token_id IS NOT NULL
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `84531_token_transfers_transaction_hash_mv` TO `84531_token_transfers_transaction_hash`
(
    `token_address` String,
    `token_standard` LowCardinality(String),
    `from_address` String,
    `to_address` String,
    `value` UInt256,
    `transaction_hash` String,
    `log_index` UInt32,
    `block_timestamp` UInt32,
    `block_number` UInt64,
    `block_hash` String,
    `operator_address` Nullable(String),
    `token_id` Nullable(UInt256)
) AS
SELECT
    token_address,
    token_standard,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_timestamp,
    block_number,
    block_hash,
    operator_address,
    token_id
FROM `84531_token_transfers`;

CREATE TABLE `84531_tokens`
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `total_supply` UInt256,
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (address, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_traces`
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
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE `84531_transactions_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW `84531_transactions_by_from_address_mv` TO `84531_transactions_address`
(
    `address` String,
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    from_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `84531_transactions`
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW `84531_transactions_by_hash_mv` TO `84531_transactions_hash`
(
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `84531_transactions`;

CREATE MATERIALIZED VIEW `84531_transactions_by_to_address_mv` TO `84531_transactions_address`
(
    `address` Nullable(String),
    `hash` String,
    `nonce` UInt64,
    `block_hash` String,
    `block_number` UInt64,
    `transaction_index` UInt32,
    `from_address` String,
    `to_address` Nullable(String),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `input` String,
    `block_timestamp` UInt32,
    `max_fee_per_gas` Nullable(Int64),
    `max_priority_fee_per_gas` Nullable(Int64),
    `transaction_type` Nullable(UInt32),
    `receipt_cumulative_gas_used` Nullable(UInt64),
    `receipt_gas_used` Nullable(UInt64),
    `receipt_contract_address` Nullable(String),
    `receipt_root` Nullable(String),
    `receipt_status` Nullable(UInt32),
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32)
) AS
SELECT
    to_address AS address,
    hash,
    nonce,
    block_hash,
    block_number,
    transaction_index,
    from_address,
    to_address,
    value,
    gas,
    gas_price,
    input,
    block_timestamp,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    transaction_type,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_root,
    receipt_status,
    receipt_effective_gas_price,
    receipt_logs_count
FROM `84531_transactions`
WHERE to_address IS NOT NULL;

CREATE TABLE `84531_transactions_hash`
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
    `transaction_type` UInt32,
    `receipt_cumulative_gas_used` UInt64,
    `receipt_gas_used` UInt64,
    `receipt_contract_address` Nullable(String) CODEC(ZSTD(1)),
    `receipt_root` Nullable(String) CODEC(ZSTD(1)),
    `receipt_status` UInt32,
    `receipt_effective_gas_price` UInt256,
    `receipt_logs_count` Nullable(UInt32)
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;

CREATE TABLE alembic_version
(
    `version_num` String,
    `dt` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(dt)
ORDER BY tuple()
SETTINGS index_granularity = 8192;

CREATE TABLE etl_delay
(
    `entity_type` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` Int32
)
ENGINE = MergeTree
ORDER BY block_number
TTL indexed_at + toIntervalDay(3)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW etl_delay_1_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    1 AS chain_id,
    'block' AS entity_type
FROM `1_blocks`;

CREATE MATERIALIZED VIEW etl_delay_1_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    1 AS chain_id,
    'geth_trace' AS entity_type
FROM `1_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_1_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    1 AS chain_id,
    'internal_transfer' AS entity_type
FROM `1_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_1_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    1 AS chain_id,
    'transaction' AS entity_type
FROM `1_transactions`;

CREATE MATERIALIZED VIEW etl_delay_10_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    10 AS chain_id,
    'block' AS entity_type
FROM `10_blocks`;

CREATE MATERIALIZED VIEW etl_delay_10_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    10 AS chain_id,
    'geth_trace' AS entity_type
FROM `10_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_10_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    10 AS chain_id,
    'internal_transfer' AS entity_type
FROM `10_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_10_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    10 AS chain_id,
    'transaction' AS entity_type
FROM `10_transactions`;

CREATE MATERIALIZED VIEW etl_delay_100_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    100 AS chain_id,
    'block' AS entity_type
FROM `100_blocks`;

CREATE MATERIALIZED VIEW etl_delay_100_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    100 AS chain_id,
    'geth_trace' AS entity_type
FROM `100_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_100_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    100 AS chain_id,
    'internal_transfer' AS entity_type
FROM `100_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_100_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    100 AS chain_id,
    'transaction' AS entity_type
FROM `100_transactions`;

CREATE MATERIALIZED VIEW etl_delay_137_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    137 AS chain_id,
    'block' AS entity_type
FROM `137_blocks`;

CREATE MATERIALIZED VIEW etl_delay_137_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    137 AS chain_id,
    'geth_trace' AS entity_type
FROM `137_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_137_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    137 AS chain_id,
    'internal_transfer' AS entity_type
FROM `137_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_137_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    137 AS chain_id,
    'transaction' AS entity_type
FROM `137_transactions`;

CREATE MATERIALIZED VIEW etl_delay_250_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    250 AS chain_id,
    'block' AS entity_type
FROM `250_blocks`;

CREATE MATERIALIZED VIEW etl_delay_250_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    250 AS chain_id,
    'geth_trace' AS entity_type
FROM `250_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_250_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    250 AS chain_id,
    'internal_transfer' AS entity_type
FROM `250_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_250_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    250 AS chain_id,
    'transaction' AS entity_type
FROM `250_transactions`;

CREATE MATERIALIZED VIEW etl_delay_42161_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42161 AS chain_id,
    'block' AS entity_type
FROM `42161_blocks`;

CREATE MATERIALIZED VIEW etl_delay_42161_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42161 AS chain_id,
    'geth_trace' AS entity_type
FROM `42161_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_42161_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42161 AS chain_id,
    'internal_transfer' AS entity_type
FROM `42161_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_42161_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42161 AS chain_id,
    'transaction' AS entity_type
FROM `42161_transactions`;

CREATE MATERIALIZED VIEW etl_delay_42170_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42170 AS chain_id,
    'block' AS entity_type
FROM `42170_blocks`;

CREATE MATERIALIZED VIEW etl_delay_42170_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42170 AS chain_id,
    'geth_trace' AS entity_type
FROM `42170_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_42170_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42170 AS chain_id,
    'internal_transfer' AS entity_type
FROM `42170_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_42170_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    42170 AS chain_id,
    'transaction' AS entity_type
FROM `42170_transactions`;

CREATE MATERIALIZED VIEW etl_delay_56_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    56 AS chain_id,
    'block' AS entity_type
FROM `56_blocks`;

CREATE MATERIALIZED VIEW etl_delay_56_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    56 AS chain_id,
    'geth_trace' AS entity_type
FROM `56_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_56_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    56 AS chain_id,
    'internal_transfer' AS entity_type
FROM `56_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_56_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt8,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    56 AS chain_id,
    'transaction' AS entity_type
FROM `56_transactions`;

CREATE MATERIALIZED VIEW etl_delay_7700_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7700 AS chain_id,
    'block' AS entity_type
FROM `7700_blocks`;

CREATE MATERIALIZED VIEW etl_delay_7700_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7700 AS chain_id,
    'geth_trace' AS entity_type
FROM `7700_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_7700_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7700 AS chain_id,
    'internal_transfer' AS entity_type
FROM `7700_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_7700_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7700 AS chain_id,
    'transaction' AS entity_type
FROM `7700_transactions`;

CREATE MATERIALIZED VIEW etl_delay_7701_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7701 AS chain_id,
    'block' AS entity_type
FROM `7701_blocks`;

CREATE MATERIALIZED VIEW etl_delay_7701_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7701 AS chain_id,
    'geth_trace' AS entity_type
FROM `7701_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_7701_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7701 AS chain_id,
    'internal_transfer' AS entity_type
FROM `7701_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_7701_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    7701 AS chain_id,
    'transaction' AS entity_type
FROM `7701_transactions`;

CREATE MATERIALIZED VIEW etl_delay_8453_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    8453 AS chain_id,
    'block' AS entity_type
FROM `8453_blocks`;

CREATE MATERIALIZED VIEW etl_delay_8453_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    8453 AS chain_id,
    'geth_trace' AS entity_type
FROM `8453_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_8453_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    8453 AS chain_id,
    'internal_transfer' AS entity_type
FROM `8453_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_8453_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt16,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    8453 AS chain_id,
    'transaction' AS entity_type
FROM `8453_transactions`;

CREATE MATERIALIZED VIEW etl_delay_84531_blocks_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt32,
    `entity_type` String
) AS
SELECT
    number AS block_number,
    timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    84531 AS chain_id,
    'block' AS entity_type
FROM `84531_blocks`;

CREATE MATERIALIZED VIEW etl_delay_84531_geth_traces_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt32,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    84531 AS chain_id,
    'geth_trace' AS entity_type
FROM `84531_geth_traces`;

CREATE MATERIALIZED VIEW etl_delay_84531_internal_transfers_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` DateTime,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt32,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    84531 AS chain_id,
    'internal_transfer' AS entity_type
FROM `84531_internal_transfers`;

CREATE MATERIALIZED VIEW etl_delay_84531_transactions_mv TO etl_delay
(
    `block_number` UInt64,
    `timestamp` UInt32,
    `indexed_at` DateTime,
    `delay` Int32,
    `chain_id` UInt32,
    `entity_type` String
) AS
SELECT
    block_number,
    block_timestamp AS timestamp,
    now() AS indexed_at,
    now() - toDateTime(timestamp) AS delay,
    84531 AS chain_id,
    'transaction' AS entity_type
FROM `84531_transactions`;

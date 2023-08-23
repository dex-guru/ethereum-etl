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
    `is_reorged` Bool DEFAULT 0,
    INDEX logs_block_number block_number TYPE minmax GRANULARITY 1

)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, address, log_index)
SETTINGS index_granularity = 8192;


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
    `is_reorged` Bool DEFAULT 0,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;


CREATE TABLE IF NOT EXISTS `${transaction}_address`
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
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32),
    `is_reorged` Bool DEFAULT 0,
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS `${transaction}_by_from_address_mv`
            TO `${transaction}_address`
AS
SELECT from_address                AS address,
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
       receipt_logs_count,
       is_reorged
FROM `${transaction}`
WHERE from_address IS NOT NULL;


CREATE MATERIALIZED VIEW IF NOT EXISTS `${transaction}_by_to_address_mv`
            TO `${transaction}_address`
AS
SELECT to_address                  AS address,
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
       receipt_logs_count,
       is_reorged
FROM `${transaction}`
WHERE to_address IS NOT NULL;


CREATE TABLE IF NOT EXISTS `${transaction}_hash`
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
    `receipt_effective_gas_price` Nullable(UInt256),
    `receipt_logs_count` Nullable(UInt32),
    `is_reorged` Bool DEFAULT 0,
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS `${transaction}_by_hash_mv`
            TO `${transaction}_hash`
AS
SELECT hash,
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
       receipt_logs_count,
       is_reorged
FROM `${transaction}`;

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
    `difficulty`         UInt256,
    `total_difficulty`   UInt256,
    `size`              UInt64,
    `extra_data`        String CODEC(ZSTD(1)),
    `gas_limit`         UInt64,
    `gas_used`          UInt64,
    `timestamp`         UInt32,
    `transaction_count` UInt64,
    `base_fee_per_gas`  Nullable(Int64),
    `is_reorged`        Bool DEFAULT 0,
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY number
SETTINGS index_granularity = 8192;

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
    `is_reorged` Bool DEFAULT 0,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE IF NOT EXISTS `${token_transfer}_address`
(
    `address` String CODEC(ZSTD(1)),
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
    `is_reorged` Bool DEFAULT 0,
)
ENGINE = ReplacingMergeTree
ORDER BY (address, token_standard, token_id, transaction_hash, log_index)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS `${token_transfer}_from_address_mv`
            TO `${token_transfer}_address`
AS
SELECT  from_address as address,
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
        token_id,
        is_reorged
FROM `${token_transfer}`;

CREATE MATERIALIZED VIEW IF NOT EXISTS `${token_transfer}_to_address_mv`
            TO `${token_transfer}_address`
AS
SELECT  to_address as address,
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
        token_id,
        is_reorged
FROM `${token_transfer}`;

CREATE MATERIALIZED VIEW IF NOT EXISTS `${token_transfer}_token_address_mv`
            TO `${token_transfer}_address`
AS
SELECT  token_address as address,
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
        token_id,
        is_reorged
FROM `${token_transfer}`;

CREATE TABLE IF NOT EXISTS `${token_transfer}_transaction_hash`
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
    `is_reorged` Bool DEFAULT 0,
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS `${token_transfer}_transaction_hash_mv`
            TO `${token_transfer}_transaction_hash`
AS
SELECT  token_address,
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
        token_id,
        is_reorged
FROM `${token_transfer}`;

CREATE TABLE IF NOT EXISTS `${token_balance}`
(
    `token_address` String CODEC(ZSTD),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD),
    `token_id` UInt256 CODEC(ZSTD),
    `block_hash` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
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
    `trace_id` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
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
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1)),
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
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1))
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
    `block_hash` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY (timestamp);


CREATE TABLE IF NOT EXISTS `${geth_trace}`
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY transaction_hash
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS `${internal_transfer}`
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
    `is_reorged` Bool DEFAULT 0,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS `${internal_transfer}_address`
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
    `is_reorged` Bool DEFAULT 0,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS `${internal_transfer}_from_address_mv`
            TO `${internal_transfer}_address`
AS
SELECT
    from_address as address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash,
    is_reorged
FROM `${internal_transfer}`
WHERE from_address IS NOT NULL;


CREATE MATERIALIZED VIEW IF NOT EXISTS `${internal_transfer}_to_address_mv`
            TO `${internal_transfer}_address`
AS
SELECT
    to_address as address,
    transaction_hash,
    block_timestamp,
    block_number,
    from_address,
    to_address,
    value,
    gas_limit,
    id,
    block_hash,
    is_reorged
FROM `${internal_transfer}`
WHERE to_address IS NOT NULL;


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
    `block_hash` String CODEC(ZSTD),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD),
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(block_timestamp))
ORDER BY (address, block_number);

CREATE TABLE IF NOT EXISTS `${chain_id}_chain_counts`
(
    active_addresses          AggregateFunction(uniq, Nullable(String)),
    uniq_contracts            AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY tuple();

CREATE MATERIALIZED VIEW IF NOT EXISTS `${chain_id}_count_active_addresses_mv`
            TO `${chain_id}_chain_counts`
(
    active_addresses          AggregateFunction(uniq, Nullable(String)),
    uniq_contracts            AggregateFunction(uniq, Nullable(String))
)
AS
SELECT
    uniqState(toNullable(from_address)) as active_addresses
FROM `${chain_id}_transactions`;

CREATE MATERIALIZED VIEW IF NOT EXISTS `${chain_id}_count_uniq_contracts_mv`
            TO `${chain_id}_chain_counts`
(
    active_addresses          AggregateFunction(uniq, Nullable(String)),
    uniq_contracts            AggregateFunction(uniq, Nullable(String))
)
AS
SELECT
    uniqState(toNullable(address)) as uniq_contracts
FROM `${chain_id}_logs`;

-------------------------------------------------------------------------------
-- from ethereumetl/scripts/clickhouse.sql.tpl
-------------------------------------------------------------------------------
CREATE TABLE `${chain_id}_event_inventory_src` ${on_cluster}
(
    event_signature_hash LowCardinality(String),
    event_signature LowCardinality(String),
    event_topic_count UInt8,
    event_name LowCardinality(String),
    abi_type String,
    event_abi_json String CODEC(ZSTD(1)),
)
ENGINE = ${replacing_merge_tree}
ORDER BY (event_signature, event_topic_count, abi_type);

CREATE TABLE `${chain_id}_event_inventory` ${on_cluster}
(
    event_signature_hash_and_log_topic_count Tuple(LowCardinality(String), UInt8),
    event_signature_hash ALIAS tupleElement(event_signature_hash_and_log_topic_count, 1),
    event_topic_count ALIAS tupleElement(event_signature_hash_and_log_topic_count, 2),
    abi_types Array(LowCardinality(String)),
    event_signature LowCardinality(String),
    event_name LowCardinality(String),
    event_abi_json String CODEC(ZSTD(1)),
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY event_signature_hash_and_log_topic_count;

CREATE MATERIALIZED VIEW `${chain_id}_event_inventory_mv` ${on_cluster}
TO `${chain_id}_event_inventory`
AS
WITH src as (
    SELECT
        (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
        groupArray(abi_type) AS abi_types,
        event_signature,
        event_name,
        event_abi_json
    FROM `${chain_id}_event_inventory_src`
    GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
)
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.abi_types, src.abi_types))) AS abi_types,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN `${chain_id}_event_inventory` dst USING event_signature_hash_and_log_topic_count
SETTINGS join_algorithm='direct'
;

CREATE TABLE `${chain_id}_events` ${on_cluster}
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
    `event_signature_hash` ALIAS arrayElement(topics, 1),
    `topic_count` ALIAS toUInt8(length(topics)),
)
ENGINE = ${replacing_merge_tree}
PARTITION BY intDivOrZero(block_number, 100000)
ORDER BY (contract_address, arrayElement(topics, 1), transaction_hash, log_index);

CREATE MATERIALIZED VIEW `${chain_id}_logs_to_events_mv` ${on_cluster}
TO `${chain_id}_events`
AS SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.block_hash,
    logs.block_number,
    logs.data,
    logs.topics,
    logs.address AS contract_address,
    info.event_name
FROM `${chain_id}_logs` logs
JOIN `${chain_id}_event_inventory` info
ON (arrayElement(logs.topics, 1), toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
SETTINGS join_algorithm='direct';

"""

CHAIN_IDS = {
    1: "eth",
    10: "optimism",
    100: "gnosis",
    137: "polygon",
    250: "fantom",
    42161: "arbitrum",
    42170: "nova",
    56: "bsc",
    7700: "canto",
    7701: "acanto",
    8453: "basemainnet",
    84531: "base",
}

ENTITY_TABLE_NAMES = {
    "block": "blocks",
    "contract": "contracts",
    "error": "errors",
    "geth_trace": "geth_traces",
    "internal_transfer": "internal_transfers",
    "log": "logs",
    "native_balance": "native_balances",
    "token": "tokens",
    "token_balance": "token_balances",
    "token_transfer": "token_transfers",
    "trace": "traces",
    "transaction": "transactions",
}


def upgrade() -> None:
    conn = op.get_bind()
    result = conn.execute("SELECT count() FROM system.replicas")
    if result is None:
        # offline mode
        is_replicated_clickhouse = os.getenv('CLICKHOUSE_REPLICATED', '').lower() in ('true', '1')
    else:
        is_replicated_clickhouse = result.one()[0] > 0

    for chain_id in CHAIN_IDS:
        prefixed_entity_table_names = {k: f"{chain_id}_{v}" for k, v in ENTITY_TABLE_NAMES.items()}
        schema_sql = render_sql_template(
            is_replicated_clickhouse,
            SCHEMA_TEMPLATE,
            chain_id=chain_id,
            **prefixed_entity_table_names,
        )
        ddls = filter(None, map(str.strip, schema_sql.split(";\n")))
        for ddl in ddls:
            op.execute(ddl)


def downgrade() -> None:
    raise NotImplementedError("Downgrade from the initial schema is not supported")

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



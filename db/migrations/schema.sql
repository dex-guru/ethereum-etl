CREATE TABLE alembic_version
(
    `version_num` String,
    `dt` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(dt)
ORDER BY tuple()
SETTINGS index_granularity = 8192;

CREATE TABLE blocks
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
    `is_reorged` Bool DEFAULT 0,
    INDEX blocks_timestamp timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (number, hash)
SETTINGS index_granularity = 8192;

CREATE TABLE candles_1d
(
    `timestamp` UInt64,
    `token_address` String,
    `pool_address` String,
    `factory_address` String,
    `c_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_s` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_s` SimpleAggregateFunction(max, Float64),
    `l_s` SimpleAggregateFunction(min, Float64),
    `c_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_n` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_n` SimpleAggregateFunction(max, Float64),
    `l_n` SimpleAggregateFunction(min, Float64),
    `v_s` SimpleAggregateFunction(sum, Float64),
    `v_n` SimpleAggregateFunction(sum, Float64),
    `liq_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `liq_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `tx_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(timestamp))
ORDER BY (token_address, pool_address, timestamp)
SETTINGS index_granularity = 8192;

CREATE TABLE dex_trades
(
    `block_number` UInt64 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt64 CODEC(ZSTD(1)),
    `transaction_hash` String CODEC(ZSTD(1)),
    `log_index` UInt64 CODEC(ZSTD(1)),
    `transaction_type` String CODEC(ZSTD(1)),
    `token_addresses` Array(String) CODEC(ZSTD(1)),
    `amounts` Array(Float64) CODEC(ZSTD(1)),
    `amount_stable` Float64 CODEC(ZSTD(1)),
    `amount_native` Float64 CODEC(ZSTD(1)),
    `prices_stable` Array(Float64) CODEC(ZSTD(1)),
    `prices_native` Array(Float64) CODEC(ZSTD(1)),
    `pool_address` String CODEC(ZSTD(1)),
    `factory_address` LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    `lp_token_address` String DEFAULT '' CODEC(ZSTD(1)),
    `reserves` Array(Float64) DEFAULT [] CODEC(ZSTD(1)),
    `reserves_stable` Array(Float64) DEFAULT [] CODEC(ZSTD(1)),
    `reserves_native` Array(Float64) DEFAULT [] CODEC(ZSTD(1)),
    `wallet_address` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW candles_1d_mv TO candles_1d
(
    `timestamp` UInt64,
    `token_address` String,
    `pool_address` String,
    `factory_address` String,
    `c_s` Tuple(UInt64, Float64),
    `o_s` Tuple(UInt64, Float64),
    `h_s` Float64,
    `l_s` Float64,
    `c_n` Tuple(UInt64, Float64),
    `o_n` Tuple(UInt64, Float64),
    `h_n` Float64,
    `l_n` Float64,
    `v_s` Float64,
    `v_n` Float64,
    `liq_s` Float64,
    `liq_n` Float64,
    `tx_count` UInt64
) AS
SELECT
    toStartOfDay(FROM_UNIXTIME(block_timestamp)) AS timestamp,
    tokens_data.1 AS token_address,
    pool_address AS pool_address,
    factory_address AS factory_address,
    max((block_timestamp, tokens_data.2)) AS c_s,
    min((block_timestamp, tokens_data.2)) AS o_s,
    max(tokens_data.2) AS h_s,
    min(tokens_data.2) AS l_s,
    max((block_timestamp, tokens_data.3)) AS c_n,
    min((block_timestamp, tokens_data.3)) AS o_n,
    max(tokens_data.3) AS h_n,
    min(tokens_data.3) AS l_n,
    sum(abs(tokens_data.4) * (tokens_data.2)) AS v_s,
    sum(abs(tokens_data.4) * (tokens_data.3)) AS v_n,
    max((block_timestamp, (tokens_data.5) * (tokens_data.2))) AS liq_s,
    max((block_timestamp, (tokens_data.5) * (tokens_data.3))) AS liq_n,
    countDistinct(swap_id) AS tx_count
FROM
(
    SELECT
        arrayJoin(arrayZip(token_addresses, arrayMap(i -> if(i <= length(prices_stable), prices_stable[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(prices_native), prices_native[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(amounts), amounts[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(reserves), reserves[i], 0), arrayEnumerate(token_addresses)))) AS tokens_data,
        pool_address AS pool_address,
        factory_address AS factory_address,
        block_timestamp AS block_timestamp,
        concat(transaction_hash, toString(log_index)) AS swap_id
    FROM dex_trades
    WHERE transaction_type = 'swap'
)
WHERE ((tokens_data.2) > 0) AND ((tokens_data.3) > 0)
GROUP BY
    token_address,
    pool_address,
    timestamp,
    factory_address;

CREATE TABLE candles_1h
(
    `timestamp` UInt64,
    `token_address` String CODEC(ZSTD(1)),
    `pool_address` String CODEC(ZSTD(1)),
    `factory_address` String CODEC(ZSTD(1)),
    `c_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_s` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_s` SimpleAggregateFunction(max, Float64),
    `l_s` SimpleAggregateFunction(min, Float64),
    `c_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_n` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_n` SimpleAggregateFunction(max, Float64),
    `l_n` SimpleAggregateFunction(min, Float64),
    `v_s` SimpleAggregateFunction(sum, Float64),
    `v_n` SimpleAggregateFunction(sum, Float64),
    `liq_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `liq_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `tx_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(timestamp))
ORDER BY (token_address, pool_address, timestamp)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW candles_1h_mv TO candles_1h
(
    `timestamp` UInt64,
    `token_address` String,
    `pool_address` String,
    `factory_address` String,
    `c_s` Tuple(UInt64, Float64),
    `o_s` Tuple(UInt64, Float64),
    `h_s` Float64,
    `l_s` Float64,
    `c_n` Tuple(UInt64, Float64),
    `o_n` Tuple(UInt64, Float64),
    `h_n` Float64,
    `l_n` Float64,
    `v_s` Float64,
    `v_n` Float64,
    `liq_s` Float64,
    `liq_n` Float64,
    `tx_count` UInt64
) AS
SELECT
    toStartOfHour(FROM_UNIXTIME(block_timestamp)) AS timestamp,
    tokens_data.1 AS token_address,
    pool_address AS pool_address,
    factory_address AS factory_address,
    max((block_timestamp, tokens_data.2)) AS c_s,
    min((block_timestamp, tokens_data.2)) AS o_s,
    max(tokens_data.2) AS h_s,
    min(tokens_data.2) AS l_s,
    max((block_timestamp, tokens_data.3)) AS c_n,
    min((block_timestamp, tokens_data.3)) AS o_n,
    max(tokens_data.3) AS h_n,
    min(tokens_data.3) AS l_n,
    sum(abs(tokens_data.4) * (tokens_data.2)) AS v_s,
    sum(abs(tokens_data.4) * (tokens_data.3)) AS v_n,
    max((block_timestamp, (tokens_data.5) * (tokens_data.2))) AS liq_s,
    max((block_timestamp, (tokens_data.5) * (tokens_data.3))) AS liq_n,
    countDistinct(swap_id) AS tx_count
FROM
(
    SELECT
        arrayJoin(arrayZip(token_addresses, arrayMap(i -> if(i <= length(prices_stable), prices_stable[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(prices_native), prices_native[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(amounts), amounts[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(reserves), reserves[i], 0), arrayEnumerate(token_addresses)))) AS tokens_data,
        pool_address AS pool_address,
        factory_address AS factory_address,
        block_timestamp AS block_timestamp,
        concat(transaction_hash, toString(log_index)) AS swap_id
    FROM dex_trades
    WHERE transaction_type = 'swap'
)
WHERE ((tokens_data.2) > 0) AND ((tokens_data.3) > 0)
GROUP BY
    token_address,
    pool_address,
    timestamp,
    factory_address;

CREATE TABLE candles_1m
(
    `timestamp` UInt64,
    `token_address` String CODEC(ZSTD(1)),
    `pool_address` String CODEC(ZSTD(1)),
    `factory_address` String CODEC(ZSTD(1)),
    `c_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_s` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_s` SimpleAggregateFunction(max, Float64),
    `l_s` SimpleAggregateFunction(min, Float64),
    `c_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_n` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_n` SimpleAggregateFunction(max, Float64),
    `l_n` SimpleAggregateFunction(min, Float64),
    `v_s` SimpleAggregateFunction(sum, Float64),
    `v_n` SimpleAggregateFunction(sum, Float64),
    `liq_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `liq_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `tx_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(timestamp))
ORDER BY (token_address, pool_address, timestamp)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW candles_1m_mv TO candles_1m
(
    `timestamp` UInt64,
    `token_address` String,
    `pool_address` String,
    `factory_address` String,
    `c_s` Tuple(UInt64, Float64),
    `o_s` Tuple(UInt64, Float64),
    `h_s` Float64,
    `l_s` Float64,
    `c_n` Tuple(UInt64, Float64),
    `o_n` Tuple(UInt64, Float64),
    `h_n` Float64,
    `l_n` Float64,
    `v_s` Float64,
    `v_n` Float64,
    `liq_s` Float64,
    `liq_n` Float64,
    `tx_count` UInt64
) AS
SELECT
    toStartOfMinute(FROM_UNIXTIME(block_timestamp)) AS timestamp,
    tokens_data.1 AS token_address,
    pool_address AS pool_address,
    factory_address AS factory_address,
    max((block_timestamp, tokens_data.2)) AS c_s,
    min((block_timestamp, tokens_data.2)) AS o_s,
    max(tokens_data.2) AS h_s,
    min(tokens_data.2) AS l_s,
    max((block_timestamp, tokens_data.3)) AS c_n,
    min((block_timestamp, tokens_data.3)) AS o_n,
    max(tokens_data.3) AS h_n,
    min(tokens_data.3) AS l_n,
    sum(abs(tokens_data.4) * (tokens_data.2)) AS v_s,
    sum(abs(tokens_data.4) * (tokens_data.3)) AS v_n,
    max((block_timestamp, (tokens_data.5) * (tokens_data.2))) AS liq_s,
    max((block_timestamp, (tokens_data.5) * (tokens_data.3))) AS liq_n,
    countDistinct(swap_id) AS tx_count
FROM
(
    SELECT
        arrayJoin(arrayZip(token_addresses, arrayMap(i -> if(i <= length(prices_stable), prices_stable[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(prices_native), prices_native[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(amounts), amounts[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(reserves), reserves[i], 0), arrayEnumerate(token_addresses)))) AS tokens_data,
        pool_address AS pool_address,
        factory_address AS factory_address,
        block_timestamp AS block_timestamp,
        concat(transaction_hash, toString(log_index)) AS swap_id
    FROM dex_trades
    WHERE transaction_type = 'swap'
)
WHERE ((tokens_data.2) > 0) AND ((tokens_data.3) > 0)
GROUP BY
    token_address,
    pool_address,
    timestamp,
    factory_address;

CREATE TABLE candles_5m
(
    `timestamp` UInt64,
    `token_address` String CODEC(ZSTD(1)),
    `pool_address` String CODEC(ZSTD(1)),
    `factory_address` String CODEC(ZSTD(1)),
    `c_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_s` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_s` SimpleAggregateFunction(max, Float64),
    `l_s` SimpleAggregateFunction(min, Float64),
    `c_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `o_n` SimpleAggregateFunction(min, Tuple(UInt64, Float64)),
    `h_n` SimpleAggregateFunction(max, Float64),
    `l_n` SimpleAggregateFunction(min, Float64),
    `v_s` SimpleAggregateFunction(sum, Float64),
    `v_n` SimpleAggregateFunction(sum, Float64),
    `liq_s` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `liq_n` SimpleAggregateFunction(max, Tuple(UInt64, Float64)),
    `tx_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(FROM_UNIXTIME(timestamp))
ORDER BY (token_address, pool_address, timestamp)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW candles_5m_mv TO candles_5m
(
    `timestamp` UInt64,
    `token_address` String,
    `pool_address` String,
    `factory_address` String,
    `c_s` Tuple(UInt64, Float64),
    `o_s` Tuple(UInt64, Float64),
    `h_s` Float64,
    `l_s` Float64,
    `c_n` Tuple(UInt64, Float64),
    `o_n` Tuple(UInt64, Float64),
    `h_n` Float64,
    `l_n` Float64,
    `v_s` Float64,
    `v_n` Float64,
    `liq_s` Float64,
    `liq_n` Float64,
    `tx_count` UInt64
) AS
SELECT
    toStartOfFiveMinute(FROM_UNIXTIME(block_timestamp)) AS timestamp,
    tokens_data.1 AS token_address,
    pool_address AS pool_address,
    factory_address AS factory_address,
    max((block_timestamp, tokens_data.2)) AS c_s,
    min((block_timestamp, tokens_data.2)) AS o_s,
    max(tokens_data.2) AS h_s,
    min(tokens_data.2) AS l_s,
    max((block_timestamp, tokens_data.3)) AS c_n,
    min((block_timestamp, tokens_data.3)) AS o_n,
    max(tokens_data.3) AS h_n,
    min(tokens_data.3) AS l_n,
    sum(abs(tokens_data.4) * (tokens_data.2)) AS v_s,
    sum(abs(tokens_data.4) * (tokens_data.3)) AS v_n,
    max((block_timestamp, (tokens_data.5) * (tokens_data.2))) AS liq_s,
    max((block_timestamp, (tokens_data.5) * (tokens_data.3))) AS liq_n,
    countDistinct(swap_id) AS tx_count
FROM
(
    SELECT
        arrayJoin(arrayZip(token_addresses, arrayMap(i -> if(i <= length(prices_stable), prices_stable[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(prices_native), prices_native[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(amounts), amounts[i], 0), arrayEnumerate(token_addresses)), arrayMap(i -> if(i <= length(reserves), reserves[i], 0), arrayEnumerate(token_addresses)))) AS tokens_data,
        pool_address AS pool_address,
        factory_address AS factory_address,
        block_timestamp AS block_timestamp,
        concat(transaction_hash, toString(log_index)) AS swap_id
    FROM dex_trades
    WHERE transaction_type = 'swap'
)
WHERE ((tokens_data.2) > 0) AND ((tokens_data.3) > 0)
GROUP BY
    token_address,
    pool_address,
    timestamp,
    factory_address;

CREATE TABLE chain_counts
(
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
)
ENGINE = AggregatingMergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;

CREATE TABLE contracts
(
    `address` String CODEC(ZSTD(1)),
    `bytecode` String CODEC(ZSTD(1)),
    `function_sighashes` Array(String) CODEC(ZSTD(1)),
    `is_erc20` UInt8,
    `is_erc721` UInt8,
    `block_number` UInt64
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY address;

CREATE TABLE transactions
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
    `gas_price` Nullable(UInt256),
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
ORDER BY (block_number, hash, block_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW count_active_addresses_mv TO chain_counts
(
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT uniqState(toNullable(from_address)) AS active_addresses
FROM transactions;

CREATE TABLE logs
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, block_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW count_uniq_contracts_mv TO chain_counts
(
    `active_addresses` AggregateFunction(uniq, Nullable(String)),
    `uniq_contracts` AggregateFunction(uniq, Nullable(String))
) AS
SELECT uniqState(toNullable(address)) AS uniq_contracts
FROM logs;

CREATE TABLE dex_pools
(
    `address` String CODEC(ZSTD(1)),
    `factory_address` LowCardinality(String),
    `token_addresses` Array(String) CODEC(ZSTD(1)),
    `lp_token_addresses` Array(String) CODEC(ZSTD(1)),
    `fee` UInt16,
    `underlying_token_addresses` Array(String)
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY address;

CREATE MATERIALIZED VIEW dex_trades_factory_mv TO dex_trades_token_wallet_pool_factory_hash
(
    `filter_column` LowCardinality(String),
    `block_number` UInt64,
    `block_hash` String,
    `block_timestamp` UInt64,
    `transaction_hash` String,
    `log_index` UInt64,
    `transaction_type` String,
    `token_addresses` Array(String),
    `amounts` Array(Float64),
    `amount_stable` Float64,
    `amount_native` Float64,
    `prices_stable` Array(Float64),
    `prices_native` Array(Float64),
    `pool_address` String,
    `factory_address` LowCardinality(String),
    `lp_token_address` String,
    `reserves` Array(Float64),
    `reserves_stable` Array(Float64),
    `reserves_native` Array(Float64),
    `wallet_addresses` Array(String),
    `is_reorged` Bool
) AS
SELECT
    factory_address AS filter_column,
    block_number AS block_number,
    block_hash AS block_hash,
    block_timestamp AS block_timestamp,
    transaction_hash AS transaction_hash,
    log_index AS log_index,
    transaction_type AS transaction_type,
    token_addresses AS token_addresses,
    amounts AS amounts,
    amount_stable AS amount_stable,
    amount_native AS amount_native,
    prices_stable AS prices_stable,
    prices_native AS prices_native,
    pool_address AS pool_address,
    factory_address AS factory_address,
    lp_token_address AS lp_token_address,
    reserves AS reserves,
    reserves_stable AS reserves_stable,
    reserves_native AS reserves_native,
    [wallet_address] AS wallet_addresses,
    is_reorged AS is_reorged
FROM dex_trades;

CREATE MATERIALIZED VIEW dex_trades_pool_mv TO dex_trades_token_wallet_pool_factory_hash
(
    `filter_column` String,
    `block_number` UInt64,
    `block_hash` String,
    `block_timestamp` UInt64,
    `transaction_hash` String,
    `log_index` UInt64,
    `transaction_type` String,
    `token_addresses` Array(String),
    `amounts` Array(Float64),
    `amount_stable` Float64,
    `amount_native` Float64,
    `prices_stable` Array(Float64),
    `prices_native` Array(Float64),
    `pool_address` String,
    `factory_address` LowCardinality(String),
    `lp_token_address` String,
    `reserves` Array(Float64),
    `reserves_stable` Array(Float64),
    `reserves_native` Array(Float64),
    `wallet_addresses` Array(String),
    `is_reorged` Bool
) AS
SELECT
    pool_address AS filter_column,
    block_number AS block_number,
    block_hash AS block_hash,
    block_timestamp AS block_timestamp,
    transaction_hash AS transaction_hash,
    log_index AS log_index,
    transaction_type AS transaction_type,
    token_addresses AS token_addresses,
    amounts AS amounts,
    amount_stable AS amount_stable,
    amount_native AS amount_native,
    prices_stable AS prices_stable,
    prices_native AS prices_native,
    pool_address AS pool_address,
    factory_address AS factory_address,
    lp_token_address AS lp_token_address,
    reserves AS reserves,
    reserves_stable AS reserves_stable,
    reserves_native AS reserves_native,
    [wallet_address] AS wallet_addresses,
    is_reorged AS is_reorged
FROM dex_trades;

CREATE MATERIALIZED VIEW dex_trades_token_mv TO dex_trades_token_wallet_pool_factory_hash
(
    `filter_column` String,
    `block_number` UInt64,
    `block_hash` String,
    `block_timestamp` UInt64,
    `transaction_hash` String,
    `log_index` UInt64,
    `transaction_type` String,
    `token_addresses` Array(String),
    `amounts` Array(Float64),
    `amount_stable` Float64,
    `amount_native` Float64,
    `prices_stable` Array(Float64),
    `prices_native` Array(Float64),
    `pool_address` String,
    `factory_address` LowCardinality(String),
    `lp_token_address` String,
    `reserves` Array(Float64),
    `reserves_stable` Array(Float64),
    `reserves_native` Array(Float64),
    `wallet_addresses` Array(String),
    `is_reorged` Bool
) AS
SELECT
    arrayJoin(token_addresses) AS filter_column,
    block_number AS block_number,
    block_hash AS block_hash,
    block_timestamp AS block_timestamp,
    transaction_hash AS transaction_hash,
    log_index AS log_index,
    transaction_type AS transaction_type,
    token_addresses AS token_addresses,
    amounts AS amounts,
    amount_stable AS amount_stable,
    amount_native AS amount_native,
    prices_stable AS prices_stable,
    prices_native AS prices_native,
    pool_address AS pool_address,
    factory_address AS factory_address,
    lp_token_address AS lp_token_address,
    reserves AS reserves,
    reserves_stable AS reserves_stable,
    reserves_native AS reserves_native,
    [wallet_address] AS wallet_addresses,
    is_reorged AS is_reorged
FROM dex_trades;

CREATE TABLE dex_trades_token_wallet_pool_factory_hash
(
    `filter_column` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt64,
    `transaction_hash` String CODEC(ZSTD(1)),
    `log_index` UInt64,
    `transaction_type` String CODEC(ZSTD(1)),
    `token_addresses` Array(String),
    `amounts` Array(Float64),
    `amount_stable` Float64,
    `amount_native` Float64,
    `prices_stable` Array(Float64),
    `prices_native` Array(Float64),
    `pool_address` String CODEC(ZSTD(1)),
    `factory_address` LowCardinality(String) DEFAULT '',
    `lp_token_address` String DEFAULT '',
    `reserves` Array(Float64) DEFAULT [],
    `reserves_stable` Array(Float64) DEFAULT [],
    `reserves_native` Array(Float64) DEFAULT [],
    `wallet_addresses` Array(String),
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (filter_column, block_timestamp, transaction_hash, log_index, block_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW dex_trades_transaction_hash_mv TO dex_trades_token_wallet_pool_factory_hash
(
    `filter_column` String,
    `block_number` UInt64,
    `block_hash` String,
    `block_timestamp` UInt64,
    `transaction_hash` String,
    `log_index` UInt64,
    `transaction_type` String,
    `token_addresses` Array(String),
    `amounts` Array(Float64),
    `amount_stable` Float64,
    `amount_native` Float64,
    `prices_stable` Array(Float64),
    `prices_native` Array(Float64),
    `pool_address` String,
    `factory_address` LowCardinality(String),
    `lp_token_address` String,
    `reserves` Array(Float64),
    `reserves_stable` Array(Float64),
    `reserves_native` Array(Float64),
    `wallet_addresses` Array(String),
    `is_reorged` Bool
) AS
SELECT
    transaction_hash AS filter_column,
    block_number AS block_number,
    block_hash AS block_hash,
    block_timestamp AS block_timestamp,
    transaction_hash AS transaction_hash,
    log_index AS log_index,
    transaction_type AS transaction_type,
    token_addresses AS token_addresses,
    amounts AS amounts,
    amount_stable AS amount_stable,
    amount_native AS amount_native,
    prices_stable AS prices_stable,
    prices_native AS prices_native,
    pool_address AS pool_address,
    factory_address AS factory_address,
    lp_token_address AS lp_token_address,
    reserves AS reserves,
    reserves_stable AS reserves_stable,
    reserves_native AS reserves_native,
    [wallet_address] AS wallet_addresses,
    is_reorged AS is_reorged
FROM dex_trades;

CREATE MATERIALIZED VIEW dex_trades_wallet_mv TO dex_trades_token_wallet_pool_factory_hash
(
    `filter_column` String,
    `block_number` UInt64,
    `block_hash` String,
    `block_timestamp` UInt64,
    `transaction_hash` String,
    `log_index` UInt64,
    `transaction_type` String,
    `token_addresses` Array(String),
    `amounts` Array(Float64),
    `amount_stable` Float64,
    `amount_native` Float64,
    `prices_stable` Array(Float64),
    `prices_native` Array(Float64),
    `pool_address` String,
    `factory_address` LowCardinality(String),
    `lp_token_address` String,
    `reserves` Array(Float64),
    `reserves_stable` Array(Float64),
    `reserves_native` Array(Float64),
    `wallet_addresses` Array(String),
    `is_reorged` Bool
) AS
SELECT
    wallet_address AS filter_column,
    block_number AS block_number,
    block_hash AS block_hash,
    block_timestamp AS block_timestamp,
    transaction_hash AS transaction_hash,
    log_index AS log_index,
    transaction_type AS transaction_type,
    token_addresses AS token_addresses,
    amounts AS amounts,
    amount_stable AS amount_stable,
    amount_native AS amount_native,
    prices_stable AS prices_stable,
    prices_native AS prices_native,
    pool_address AS pool_address,
    factory_address AS factory_address,
    lp_token_address AS lp_token_address,
    reserves AS reserves,
    reserves_stable AS reserves_stable,
    reserves_native AS reserves_native,
    [wallet_address] AS wallet_addresses,
    is_reorged AS is_reorged
FROM dex_trades;

CREATE TABLE errors
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

CREATE MATERIALIZED VIEW etl_delay_blocks_mv TO etl_delay
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
FROM blocks;

CREATE TABLE geth_traces
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, block_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW etl_delay_geth_traces_mv TO etl_delay
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
FROM geth_traces;

CREATE TABLE internal_transfers
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
    INDEX block_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, id, block_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW etl_delay_internal_transfers_mv TO etl_delay
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
FROM internal_transfers;

CREATE MATERIALIZED VIEW etl_delay_transactions_mv TO etl_delay
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
FROM transactions;

CREATE TABLE event_inventory
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `event_signature_hash` String ALIAS event_signature_hash_and_log_topic_count.1,
    `event_topic_count` UInt8 ALIAS event_signature_hash_and_log_topic_count.2,
    `namespace` Array(LowCardinality(String)),
    `contract_name` Array(LowCardinality(String)),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PRIMARY KEY event_signature_hash_and_log_topic_count
ORDER BY event_signature_hash_and_log_topic_count
SETTINGS index_granularity = 8192;

CREATE TABLE event_inventory_src
(
    `event_signature_hash` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_topic_count` UInt8,
    `event_name` LowCardinality(String),
    `namespace` LowCardinality(String),
    `contract_name` LowCardinality(String),
    `event_abi_json` String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (event_signature, event_topic_count, namespace)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW event_inventory_mv TO event_inventory
(
    `event_signature_hash_and_log_topic_count` Tuple(LowCardinality(String), UInt8),
    `namespace` LowCardinality(String),
    `contract_name` LowCardinality(String),
    `event_signature` LowCardinality(String),
    `event_name` LowCardinality(String),
    `event_abi_json` String
) AS
WITH src AS
    (
        SELECT
            (event_signature_hash, event_topic_count) AS event_signature_hash_and_log_topic_count,
            groupArray(namespace) AS namespace,
            groupArray(contract_name) AS contract_name,
            event_signature,
            event_name,
            event_abi_json
        FROM event_inventory_src
        GROUP BY (event_signature_hash, event_topic_count, event_signature, event_name, event_abi_json)
    )
SELECT
    src.event_signature_hash_and_log_topic_count,
    arraySort(arrayDistinct(arrayConcat(dst.namespace, src.namespace))) AS namespace,
    arraySort(arrayDistinct(arrayConcat(dst.contract_name, src.contract_name))) AS contract_name,
    src.event_signature,
    src.event_name,
    src.event_abi_json
FROM src
LEFT JOIN event_inventory AS dst USING (event_signature_hash_and_log_topic_count);

CREATE MATERIALIZED VIEW geth_traces_by_transaction_hash TO geth_traces_transaction_hash
(
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `traces_json` String,
    `block_hash` String,
    `is_reorged` Bool
) AS
SELECT
    transaction_hash,
    block_timestamp,
    block_number,
    traces_json,
    block_hash,
    is_reorged
FROM geth_traces;

CREATE TABLE geth_traces_transaction_hash
(
    `transaction_hash` String CODEC(ZSTD(1)),
    `block_timestamp` DateTime CODEC(DoubleDelta),
    `block_number` UInt64 CODEC(Delta(8), LZ4),
    `traces_json` String CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, block_hash)
SETTINGS index_granularity = 8192;

CREATE TABLE internal_transfers_address
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
    `block_hash` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
    INDEX block_number block_number TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, transaction_hash, id, block_hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW internal_transfers_from_address_mv TO internal_transfers_address
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
    `block_hash` String,
    `is_reorged` Bool
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
    block_hash,
    is_reorged
FROM internal_transfers
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW internal_transfers_to_address_mv TO internal_transfers_address
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
    `block_hash` String,
    `is_reorged` Bool
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
    block_hash,
    is_reorged
FROM internal_transfers
WHERE to_address IS NOT NULL;

CREATE TABLE internal_transfers_transaction_hash
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
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, block_number, id, block_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW internal_transfers_transaction_hash_mv TO internal_transfers_transaction_hash
(
    `transaction_hash` String,
    `block_timestamp` DateTime,
    `block_number` UInt64,
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` UInt256,
    `gas_limit` UInt64,
    `id` String,
    `block_hash` String,
    `is_reorged` Bool
) AS
SELECT
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
FROM internal_transfers;

CREATE TABLE logs_address
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (address, transaction_hash, log_index, block_hash)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW logs_by_address_mv TO logs_address
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `address` String,
    `data` String,
    `topics` Array(String),
    `is_reorged` Bool
) AS
SELECT
    log_index,
    transaction_hash,
    transaction_index,
    block_hash,
    block_number,
    address,
    data,
    topics,
    is_reorged
FROM logs;

CREATE MATERIALIZED VIEW logs_by_transaction_hash_mv TO logs_transaction_hash
(
    `log_index` UInt32,
    `transaction_hash` String,
    `transaction_index` UInt32,
    `block_hash` String,
    `block_number` UInt64,
    `address` String,
    `data` String,
    `topics` Array(String),
    `is_reorged` Bool
) AS
SELECT
    log_index,
    transaction_hash,
    transaction_index,
    block_hash,
    block_number,
    address,
    data,
    topics,
    is_reorged
FROM logs;

CREATE TABLE logs_transaction_hash
(
    `log_index` UInt32,
    `transaction_hash` String CODEC(ZSTD(1)),
    `transaction_index` UInt32,
    `block_hash` String CODEC(ZSTD(1)),
    `block_number` UInt64,
    `address` String CODEC(ZSTD(1)),
    `data` String CODEC(ZSTD(1)),
    `topics` Array(String) CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, block_hash)
SETTINGS index_granularity = 8192;

CREATE TABLE native_balances
(
    `address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_hash` String CODEC(ZSTD(1)),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
    INDEX block_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, address, block_hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE token_balances
(
    `token_address` String CODEC(ZSTD(1)),
    `token_standard` LowCardinality(String) DEFAULT '',
    `holder_address` String CODEC(ZSTD(1)),
    `block_number` UInt64 CODEC(DoubleDelta),
    `block_timestamp` UInt32 CODEC(DoubleDelta),
    `value` UInt256 CODEC(ZSTD(1)),
    `token_id` UInt256 CODEC(ZSTD(1)),
    `block_hash` String CODEC(ZSTD(1)),
    `is_reorged` Bool DEFAULT 0,
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, token_address, holder_address, token_id, block_hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE token_transfers
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
    INDEX blocks_timestamp block_timestamp TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (block_number, transaction_hash, log_index, token_id, block_hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE TABLE token_transfers_address
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
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (address, token_standard, token_id, transaction_hash, log_index, block_hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW token_transfers_from_address_mv TO token_transfers_address
(
    `address` String,
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
    `token_id` Nullable(UInt256),
    `is_reorged` Bool
) AS
SELECT
    from_address AS address,
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
FROM token_transfers;

CREATE MATERIALIZED VIEW token_transfers_to_address_mv TO token_transfers_address
(
    `address` String,
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
    `token_id` Nullable(UInt256),
    `is_reorged` Bool
) AS
SELECT
    to_address AS address,
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
FROM token_transfers;

CREATE MATERIALIZED VIEW token_transfers_token_address_mv TO token_transfers_address
(
    `address` String,
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
    `token_id` Nullable(UInt256),
    `is_reorged` Bool
) AS
SELECT
    token_address AS address,
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
FROM token_transfers;

CREATE TABLE token_transfers_transaction_hash
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
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (transaction_hash, log_index, token_id, block_hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW token_transfers_transaction_hash_mv TO token_transfers_transaction_hash
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
    `token_id` Nullable(UInt256),
    `is_reorged` Bool
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
    token_id,
    is_reorged
FROM token_transfers;

CREATE TABLE tokens
(
    `address` String CODEC(ZSTD(1)),
    `name` String CODEC(ZSTD(1)),
    `symbol` String CODEC(ZSTD(1)),
    `decimals` UInt8,
    `total_supply` UInt256
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY address;

CREATE TABLE traces
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
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(FROM_UNIXTIME(block_timestamp))
ORDER BY trace_id
SETTINGS index_granularity = 8192;

CREATE TABLE transactions_address
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
    `gas_price` Nullable(UInt64),
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
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (address, from_address, to_address, hash, block_hash)
SETTINGS allow_nullable_key = 1, index_granularity = 8192;

CREATE MATERIALIZED VIEW transactions_by_from_address_mv TO transactions_address
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
    `gas_price` Nullable(UInt256),
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
    `receipt_logs_count` Nullable(UInt32),
    `is_reorged` Bool
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
    receipt_logs_count,
    is_reorged
FROM transactions
WHERE from_address IS NOT NULL;

CREATE MATERIALIZED VIEW transactions_by_hash_mv TO transactions_hash
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
    `gas_price` Nullable(UInt256),
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
    `receipt_logs_count` Nullable(UInt32),
    `is_reorged` Bool
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
    receipt_logs_count,
    is_reorged
FROM transactions;

CREATE MATERIALIZED VIEW transactions_by_to_address_mv TO transactions_address
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
    `gas_price` Nullable(UInt256),
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
    `receipt_logs_count` Nullable(UInt32),
    `is_reorged` Bool
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
    receipt_logs_count,
    is_reorged
FROM transactions
WHERE to_address IS NOT NULL;

CREATE TABLE transactions_count
(
    `date` Date,
    `transactions_count` Int256
)
ENGINE = SummingMergeTree
ORDER BY date
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW transactions_count_mv TO transactions_count
(
    `date` Date,
    `transactions_count` Int256
) AS
SELECT
    date AS date,
    sum(count) AS transactions_count
FROM
(
    SELECT
        if(is_reorged > 0, sum(toInt256(-transaction_count)), sum(toInt256(transaction_count))) AS count,
        toDate(toStartOfDay(FROM_UNIXTIME(timestamp))) AS date
    FROM blocks
    GROUP BY
        is_reorged,
        date
)
GROUP BY date;

CREATE TABLE transactions_hash
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
    `gas_price` Nullable(UInt64),
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
    `is_reorged` Bool DEFAULT 0
)
ENGINE = ReplacingMergeTree
ORDER BY (hash, block_number, block_hash)
SETTINGS index_granularity = 8192;

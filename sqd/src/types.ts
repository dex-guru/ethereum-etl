// Type definitions for the indexer

export interface BlockRow {
    number: number;
    hash: string;
    parent_hash: string;
    nonce: string | null;
    sha3_uncles: string;
    logs_bloom: string;
    transactions_root: string;
    state_root: string;
    receipts_root: string;
    miner: string;
    difficulty: number;
    total_difficulty: number;
    size: number;
    extra_data: string;
    gas_limit: number;
    gas_used: number;
    l1_block_number: number;
    base_fee_per_gas: string | null;
    timestamp: number;
    transaction_count: number;
}

export interface TransactionRow {
    hash: string;
    nonce: number;
    block_hash: string;
    block_number: number;
    transaction_index: number;
    from_address: string;
    to_address: string | null;
    value: string;
    gas: bigint;
    gas_price: bigint | null;
    input: string;
    block_timestamp: bigint;
    max_fee_per_gas: bigint | null;
    max_priority_fee_per_gas: bigint | null;
    transaction_type: number | null;
    receipt_cumulative_gas_used: bigint | null;
    receipt_gas_used: bigint | null;
    receipt_contract_address: string | null;
    receipt_root: string | null;
    receipt_status: number | null;
    receipt_effective_gas_price: bigint | null;
    receipt_logs_count: number | null;
    is_reorged: boolean;
}

export interface LogRow {
    log_index: number;
    transaction_hash: string;
    transaction_index: number;
    block_hash: string;
    block_number: number;
    address: string;
    data: string;
    topics: string[];
    is_reorged: boolean;
}

export interface Transfer {
    token_address: string;
    token_standard: 'ERC-20' | 'ERC-721' | 'ERC-1155';
    from_address: string;
    to_address: string;
    value: string;
    transaction_hash: string;
    log_index: number;
    block_timestamp: number;
    block_number: number;
    block_hash: string;
    operator_address: string | null;
    token_id: string | null;
}

export interface BalanceChange {
    token_address: string;
    holder_address: string;
    token_id: string | null;
    amount: string;
    block_number: number;
    block_timestamp: number;
    token_standard: string;
    transaction_hash: string;
    log_index: number;
}

export interface NativeBalance {
    address: string;
    block_number: number;
    block_hash: string;
    block_timestamp: number;
    value: string;
    is_reorged: number;
}

export interface BatchManager {
    blocks: BlockRow[];
    transactions: TransactionRow[];
    logs: LogRow[];
    transfers: Transfer[];
    nativeBalances: NativeBalance[];
    balanceIncreases: BalanceChange[];
    balanceDecreases: BalanceChange[];
    snapshots: any[];
} 
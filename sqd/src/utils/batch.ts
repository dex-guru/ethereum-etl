import {insert as clickhouseInsert} from '../clickhouseClient/clickhouseClient';
import {AMQP_CONFIG, CONFIG, shouldStore} from '../config';
import {rabbitmqPublisher} from '../services/rabbitmqPublisher';
import {BalanceChange, BatchManager, BlockRow, LogRow, NativeBalance, TransactionRow, Transfer} from '../types';

// Convert BigInt to string recursively
export function convertBigInt(obj: any): any {
    if (typeof obj === 'bigint') {
        return obj.toString();
    } else if (Array.isArray(obj)) {
        return obj.map(convertBigInt);
    } else if (obj && typeof obj === 'object') {
        const res: any = {};
        for (const key in obj) {
            res[key] = convertBigInt(obj[key]);
        }
        return res;
    }
    return obj;
}

// Generic batch insert function with retry, interface unchanged
async function insertBatch(tableName: string, batch: any[], batchType: string): Promise<void> {
    if (batch.length === 0) return;
    const maxRetries = 5;
    const retryDelayMs = 1000;
    let attempt = 0;
    while (attempt <= maxRetries) {
        try {
            await clickhouseInsert(tableName, batch.map(convertBigInt));
            return;
        } catch (error) {
            attempt++;
            console.error(`Error inserting ${batchType} batch (attempt ${attempt}):`, error);
            if (attempt > maxRetries) {
                throw error;
            }
            await new Promise(res => setTimeout(res, retryDelayMs));
        }
    }
}

// Specific insert functions
export const insertBlocksBatch = (batch: BlockRow[]) => 
    insertBatch('blocks', batch, 'blocks');

export const insertTransactionsBatch = (batch: TransactionRow[]) => 
    insertBatch('transactions', batch, 'transactions');

export const insertLogsBatch = (batch: LogRow[]) => 
    insertBatch('logs', batch, 'logs');

export const insertTransfersBatch = (batch: Transfer[]) => 
    insertBatch('token_transfers', batch, 'transfers');

export const insertNativeBalancesBatch = (batch: NativeBalance[]) => 
    insertBatch('native_balances', batch, 'native balances');

export const insertBalanceIncreasesBatch = (batch: BalanceChange[]) => 
    insertBatch('token_balance_increases', batch, 'balance increases');

export const insertBalanceDecreasesBatch = (batch: BalanceChange[]) => 
    insertBatch('token_balance_decreases', batch, 'balance decreases');

export const insertSnapshotsBatch = (batch: any[]) => 
    insertBatch('token_balance_snapshots', batch, 'snapshots');

// Batch manager class
export class BatchManagerImpl implements BatchManager {
    blocks: BlockRow[] = [];
    transactions: TransactionRow[] = [];
    logs: LogRow[] = [];
    transfers: Transfer[] = [];
    nativeBalances: NativeBalance[] = [];
    balanceIncreases: BalanceChange[] = [];
    balanceDecreases: BalanceChange[] = [];
    snapshots: any[] = [];


    async flushIfNeeded(): Promise<void> {
        if (shouldStore.blocks() && this.blocks.length >= CONFIG.BATCH_SIZE) {
            await insertBlocksBatch(this.blocks);
            this.blocks = [];
        }
        if (shouldStore.transactions() && this.transactions.length >= CONFIG.BATCH_SIZE) {
            await insertTransactionsBatch(this.transactions);
            this.transactions = [];
        }
        if (shouldStore.logs() && this.logs.length >= CONFIG.BATCH_SIZE) {
            await insertLogsBatch(this.logs);
            this.logs = [];
        }
        
        // Handle transfers and AMQP publishing together
        if (shouldStore.transfers() && this.transfers.length >= CONFIG.BATCH_SIZE) {
            await Promise.all([
                rabbitmqPublisher.publishBatchMessage(
                    this.transfers[0].block_number, 
                    this.transfers[this.transfers.length - 1].block_number, 
                    AMQP_CONFIG.ROUTING_KEY
                ),
                insertTransfersBatch(this.transfers)
            ]);
            this.transfers = [];
        }
        
        if (shouldStore.nativeBalances() && this.nativeBalances.length >= CONFIG.BATCH_SIZE) {
            await insertNativeBalancesBatch(this.nativeBalances);
            this.nativeBalances = [];
        }
        if (shouldStore.tokenBalances() && this.balanceIncreases.length >= CONFIG.BATCH_SIZE) {
            await Promise.all([
                insertBalanceIncreasesBatch(this.balanceIncreases),
                insertBalanceDecreasesBatch(this.balanceDecreases)
            ]);
            this.balanceIncreases = [];
            this.balanceDecreases = [];
        }
        if (this.snapshots.length >= CONFIG.BATCH_SIZE) {
            await insertSnapshotsBatch(this.snapshots);
            this.snapshots = [];
        }
    }

    async flushAll(): Promise<void> {
        // Double-buffering: swap arrays to avoid race conditions
        const blocksBatch = this.blocks;
        this.blocks = [];
        const transactionsBatch = this.transactions;
        this.transactions = [];
        const logsBatch = this.logs;
        this.logs = [];
        const transfersBatch = this.transfers;
        this.transfers = [];
        const nativeBalancesBatch = this.nativeBalances;
        this.nativeBalances = [];
        const balanceIncreasesBatch = this.balanceIncreases;
        this.balanceIncreases = [];
        const balanceDecreasesBatch = this.balanceDecreases;
        this.balanceDecreases = [];
        const snapshotsBatch = this.snapshots;
        this.snapshots = [];

        // Collect all independent flush promises
        const promises = [];

        if (shouldStore.blocks() && blocksBatch.length > 0) {
            promises.push(
                insertBlocksBatch(blocksBatch).catch(e => console.error('Error flushing blocks:', e))
            );
        }
        if (shouldStore.transactions() && transactionsBatch.length > 0) {
            promises.push(
                insertTransactionsBatch(transactionsBatch).catch(e => console.error('Error flushing transactions:', e))
            );
        }
        if (shouldStore.logs() && logsBatch.length > 0) {
            promises.push(
                insertLogsBatch(logsBatch).catch(e => console.error('Error flushing logs:', e))
            );
        }
        if (shouldStore.nativeBalances() && nativeBalancesBatch.length > 0) {
            promises.push(
                insertNativeBalancesBatch(nativeBalancesBatch).catch(e => console.error('Error flushing native balances:', e))
            );
        }
        if (shouldStore.tokenBalances() && balanceIncreasesBatch.length > 0) {
            promises.push(
                Promise.all([
                    insertBalanceIncreasesBatch(balanceIncreasesBatch),
                    insertBalanceDecreasesBatch(balanceDecreasesBatch)
                ]).catch(e => console.error('Error flushing token balances:', e))
            );
        }
        if (snapshotsBatch.length > 0) {
            promises.push(
                insertSnapshotsBatch(snapshotsBatch).catch(e => console.error('Error flushing snapshots:', e))
            );
        }
        // Transfers: keep AMQP and DB insert together
        if (shouldStore.transfers() && transfersBatch.length > 0) {
            promises.push(
                Promise.all([
                    rabbitmqPublisher.publishBatchMessage(
                        transfersBatch[0].block_number,
                        transfersBatch[transfersBatch.length - 1].block_number,
                        AMQP_CONFIG.ROUTING_KEY
                    ),
                    insertTransfersBatch(transfersBatch)
                ]).catch(e => console.error('Error flushing transfers:', e))
            );
        }

        await Promise.all(promises);
    }
} 
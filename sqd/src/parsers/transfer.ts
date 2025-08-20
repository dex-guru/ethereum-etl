import {Log} from '@subsquid/evm-processor';
import {events as usdtEvents} from '../abi/usdt';
import {BalanceChange, Transfer} from '../types';
import {CONFIG, shouldStore} from '../config';

export interface ParsedTransfer {
    transfer: Transfer;
    balanceIncrease?: BalanceChange;
    balanceDecrease?: BalanceChange;
}

// Parse ERC20 Transfer event
export function parseERC20Transfer(
    log: Log, 
    blockNumber: number, 
    blockHash: string, 
    blockTimestamp: number
): ParsedTransfer | null {
    try {
        const parsed = usdtEvents.Transfer.decode(log);
        const transactionHash = (log as any).transactionHash ?? log.transaction?.hash ?? '';
        
        const transfer: Transfer = {
            token_address: log.address,
            token_standard: 'ERC-20',
            from_address: parsed.from,
            to_address: parsed.to,
            value: parsed.value.toString(),
            transaction_hash: transactionHash,
            log_index: log.logIndex ?? 0,
            block_timestamp: blockTimestamp,
            block_number: blockNumber,
            block_hash: blockHash,
            operator_address: null,
            token_id: null,
        };
        
        const result: ParsedTransfer = { transfer };
        
        // Balance increase for receiver (unless it's a burn to 0x0)
        if (parsed.to !== CONFIG.ZERO_ADDRESS && shouldStore.tokenBalances()) {
            result.balanceIncrease = {
                token_address: log.address,
                holder_address: parsed.to,
                token_id: null,
                amount: parsed.value.toString(),
                block_number: blockNumber,
                block_timestamp: blockTimestamp,
                token_standard: 'ERC-20',
                transaction_hash: transactionHash,
                log_index: log.logIndex ?? 0
            };
            result.balanceDecrease = {
                token_address: log.address,
                holder_address: parsed.from,
                token_id: null,
                amount: parsed.value.toString(),
                block_number: blockNumber,
                block_timestamp: blockTimestamp,
                token_standard: 'ERC-20',
                transaction_hash: transactionHash,
                log_index: log.logIndex ?? 0
            };
        }
        return result;
    } catch (e) {
        console.error('Error decoding ERC20 transfer event:', e);
        return null;
    }
}

// Parse ERC721 Transfer event
export function parseERC721Transfer(
    log: Log, 
    blockNumber: number, 
    blockHash: string, 
    blockTimestamp: number
): ParsedTransfer | null {
    try {
        const fromAddress = '0x' + log.topics[1].slice(26);
        const toAddress = '0x' + log.topics[2].slice(26);
        const tokenId = BigInt(log.topics[3]).toString();
        const transactionHash = (log as any).transactionHash ?? log.transaction?.hash ?? '';
        
        const transfer: Transfer = {
            token_address: log.address,
            token_standard: 'ERC-721',
            from_address: fromAddress,
            to_address: toAddress,
            value: '1',
            transaction_hash: transactionHash,
            log_index: log.logIndex ?? 0,
            block_timestamp: blockTimestamp,
            block_number: blockNumber,
            block_hash: blockHash,
            operator_address: null,
            token_id: tokenId,
        };
        
        const result: ParsedTransfer = { transfer };
        
        // Balance changes for NFTs
        if (toAddress !== CONFIG.ZERO_ADDRESS && shouldStore.tokenBalances()) {
            result.balanceIncrease = {
                token_address: log.address,
                holder_address: toAddress,
                token_id: tokenId,
                amount: '1',
                block_number: blockNumber,
                block_timestamp: blockTimestamp,
                token_standard: 'ERC-721',
                transaction_hash: transactionHash,
                log_index: log.logIndex ?? 0
            };
            result.balanceDecrease = {
                token_address: log.address,
                holder_address: fromAddress,
                token_id: tokenId,
                amount: '1',
                block_number: blockNumber,
                block_timestamp: blockTimestamp,
                token_standard: 'ERC-721',
                transaction_hash: transactionHash,
                log_index: log.logIndex ?? 0
            };
        }
        return result;
    } catch (e) {
        console.error('Error decoding ERC721 transfer event:', e);
        return null;
    }
}

// Parse ERC1155 TransferSingle event
export function parseERC1155Transfer(
    log: Log, 
    blockNumber: number, 
    blockHash: string, 
    blockTimestamp: number
): ParsedTransfer | null {
    try {
        const operatorAddress = '0x' + log.topics[1].slice(26);
        const fromAddress = '0x' + log.topics[2].slice(26);
        const toAddress = '0x' + log.topics[3].slice(26);
        
        // Decode data field for id and value
        const dataHex = log.data.slice(2);
        const tokenId = BigInt('0x' + dataHex.slice(0, 64)).toString();
        const value = BigInt('0x' + dataHex.slice(64, 128)).toString();
        const transactionHash = (log as any).transactionHash ?? log.transaction?.hash ?? '';
        
        const transfer: Transfer = {
            token_address: log.address,
            token_standard: 'ERC-1155',
            from_address: fromAddress,
            to_address: toAddress,
            value: value,
            transaction_hash: transactionHash,
            log_index: log.logIndex ?? 0,
            block_timestamp: blockTimestamp,
            block_number: blockNumber,
            block_hash: blockHash,
            operator_address: operatorAddress,
            token_id: tokenId,
        };
        
        const result: ParsedTransfer = { transfer };
        
        // Balance changes for ERC1155
        if (toAddress !== CONFIG.ZERO_ADDRESS && shouldStore.tokenBalances()) {
            result.balanceIncrease = {
                token_address: log.address,
                holder_address: toAddress,
                token_id: tokenId,
                amount: value,
                block_number: blockNumber,
                block_timestamp: blockTimestamp,
                token_standard: 'ERC-1155',
                transaction_hash: transactionHash,
                log_index: log.logIndex ?? 0
            };
            result.balanceDecrease = {
                token_address: log.address,
                holder_address: fromAddress,
                token_id: tokenId,
                amount: value,
                block_number: blockNumber,
                block_timestamp: blockTimestamp,
                token_standard: 'ERC-1155',
                transaction_hash: transactionHash,
                log_index: log.logIndex ?? 0
            };
        }

        return result;
    } catch (e) {
        console.error('Error decoding ERC1155 transfer event:', e);
        return null;
    }
} 
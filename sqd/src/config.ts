import {config} from 'dotenv';

config();

// Entity types for storage configuration
export const ENTITY_TYPES = {
    BLOCKS: 'block',
    TRANSACTIONS: 'transaction',
    LOGS: 'log',
    TRANSFERS: 'token_transfer',
    NATIVE_BALANCES: 'native_balance',
    TOKEN_BALANCE: 'token_balance',
} as const;

// Default entity types to store
const DEFAULT_ENTITY_TYPES = [
    ENTITY_TYPES.BLOCKS,
    ENTITY_TYPES.TRANSACTIONS,
    ENTITY_TYPES.LOGS,
    ENTITY_TYPES.TRANSFERS,
    ENTITY_TYPES.TOKEN_BALANCE,
].join(',');

// Helper function to parse OUTPUT string and extract URLs by type
function parseOutputUrls(outputString: string) {
    const outputs = outputString.split(',').map(url => url.trim()).filter(url => url.length > 0);
    
    const clickhouseUrls = outputs.filter(url => url.startsWith('clickhouse'));
    const amqpUrls = outputs.filter(url => url.startsWith('amqp://'));
    
    return {
        clickhouseUrls,
        amqpUrls,
        allOutputs: outputs
    };
}

// Storage configuration flags
export const CONFIG = {
    // Entity types configuration - comma-separated string like in envs.py
    CHAIN_ID: process.env.CHAIN_ID,
    ENTITY_TYPES: process.env.ENTITY_TYPES || DEFAULT_ENTITY_TYPES,
    
    // Processing configuration
    BATCH_SIZE: process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : 100,
    
    // TTL configuration (in days)
    BALANCE_CHANGES_TTL_DAYS: process.env.BALANCE_CHANGES_TTL_DAYS ? parseInt(process.env.BALANCE_CHANGES_TTL_DAYS) : 90,
    
    // Network configuration - using _URL suffix like in envs.py
    GATEWAY_URL: process.env.GATEWAY_URL,
    PROVIDER_URL: process.env.PROVIDER_URL,
    RPC_ETH_RATE_LIMIT: process.env.RPC_ETH_RATE_LIMIT ? parseInt(process.env.RPC_ETH_RATE_LIMIT) : 0,
    
    // Output configuration - following envs.py pattern with comma-separated destinations
    OUTPUT: process.env.OUTPUT || process.env.ETH_CLICKHOUSE_URL || 'clickhouse+http://default:@localhost:8123/base',
    
    // ClickHouse retry configuration
    CLICKHOUSE_TIMEOUT: process.env.CLICKHOUSE_TIMEOUT ? parseInt(process.env.CLICKHOUSE_TIMEOUT) : 30000,
    CLICKHOUSE_MAX_RETRIES: process.env.CLICKHOUSE_MAX_RETRIES ? parseInt(process.env.CLICKHOUSE_MAX_RETRIES) : 3,
    CLICKHOUSE_RETRY_DELAY: process.env.CLICKHOUSE_RETRY_DELAY ? parseInt(process.env.CLICKHOUSE_RETRY_DELAY) : 1000,
    CLICKHOUSE_MAX_RETRY_DELAY: process.env.CLICKHOUSE_MAX_RETRY_DELAY ? parseInt(process.env.CLICKHOUSE_MAX_RETRY_DELAY) : 10000,
    
    // RabbitMQ configuration - fallback env vars for queue/exchange names
    QUEUE_NAME: process.env.QUEUE_NAME || process.env.RABBITMQ_QUEUE || 'enriched_dex_trade__enriched_transfer',
    EXCHANGE_NAME: process.env.EXCHANGE_NAME || process.env.CHAIN_ID + '_indexation_etl',
    ROUTING_KEY: process.env.ROUTING_KEY || process.env.RABBITMQ_ROUTING_KEY || 'token_transfer',
    
    // Constants
    ZERO_ADDRESS: '0x0000000000000000000000000000000000000000',
    TRANSFER_TOPIC: '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
    ERC1155_TRANSFER_SINGLE_TOPIC: '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
} as const;

// Parse output URLs
const PARSED_OUTPUTS = parseOutputUrls(CONFIG.OUTPUT);

// Helper function to parse ClickHouse connection string
export function parseClickHouseUrl(url: string) {
    try {
        // Parse URLs like: clickhouse+http://username:password@host:port/database
        const urlObj = new URL(url);
        
        // Extract protocol (remove the +http part if present)
        const protocol = urlObj.protocol.replace('clickhouse+', '').replace(':', '');
        
        // Build the base URL for ClickHouse client
        const baseUrl = `${protocol}://${urlObj.hostname}${urlObj.port ? ':' + urlObj.port : ''}/${urlObj.pathname.replace('/', '')}`;
        
        return {
            url: baseUrl,
            username: urlObj.username || 'default',
            password: urlObj.password || '',
            database: urlObj.pathname.replace('/', '') || 'base',
            host: urlObj.hostname,
            port: urlObj.port || '8123'
        };
    } catch (error) {
        console.warn('Failed to parse ClickHouse URL, using fallback values:', error);
        // Fallback to individual environment variables if URL parsing fails
        return {
            url: process.env.CLICKHOUSE_HOST || 'http://localhost:8123',
            username: process.env.CLICKHOUSE_USER || 'default',
            password: process.env.CLICKHOUSE_PASSWORD || '',
            database: process.env.CLICKHOUSE_DATABASE || 'base',
            host: process.env.CLICKHOUSE_HOST || 'localhost',
            port: process.env.CLICKHOUSE_PORT || '8123'
        };
    }
}

// Helper function to parse AMQP connection string and extract components
export function parseAmqpUrl(url: string) {
    try {
        // Parse URLs like: amqp://username:password@host:port/vhost
        const urlObj = new URL(url);
        
        return {
            url: url,
            username: urlObj.username || 'guest',
            password: urlObj.password || 'guest',
            host: urlObj.hostname || 'localhost',
            port: urlObj.port || '5672',
            vhost: urlObj.pathname.replace('/', '') || ''
        };
    } catch (error) {
        console.warn('Failed to parse AMQP URL:', error);
        return null;
    }
}

// Get ClickHouse configuration from OUTPUT (use first ClickHouse URL found)
const clickhouseUrl = PARSED_OUTPUTS.clickhouseUrls[0] || 'clickhouse+http://default:@localhost:8123/base';
export const CLICKHOUSE_CONFIG = parseClickHouseUrl(clickhouseUrl);

// Get AMQP configuration from OUTPUT (use first AMQP URL found) or fallback to env vars
const amqpUrlFromOutput = PARSED_OUTPUTS.amqpUrls[0];
const amqpConfig = amqpUrlFromOutput ? parseAmqpUrl(amqpUrlFromOutput) : null;

export const AMQP_CONFIG = {
    URL: amqpConfig?.url || process.env.AMQP_URL || process.env.RABBITMQ_URL || 'amqp://localhost:5672',
    ENABLED: PARSED_OUTPUTS.amqpUrls.length > 0 || !!process.env.AMQP_URL || !!process.env.RABBITMQ_URL,
    QUEUE_NAME: CONFIG.QUEUE_NAME,
    EXCHANGE_NAME: CONFIG.EXCHANGE_NAME,
    ROUTING_KEY: CONFIG.ROUTING_KEY,
};

// Helper function to check if an entity type should be stored
export function shouldStoreEntityType(entityType: string): boolean {
    const entityTypes = CONFIG.ENTITY_TYPES.split(',').map(type => type.trim());
    return entityTypes.includes(entityType);
}

// Helper functions for specific entity types
export const shouldStore = {
    blocks: () => shouldStoreEntityType(ENTITY_TYPES.BLOCKS),
    transactions: () => shouldStoreEntityType(ENTITY_TYPES.TRANSACTIONS),
    logs: () => shouldStoreEntityType(ENTITY_TYPES.LOGS),
    transfers: () => shouldStoreEntityType(ENTITY_TYPES.TRANSFERS),
    nativeBalances: () => shouldStoreEntityType(ENTITY_TYPES.NATIVE_BALANCES),
    tokenBalances: () => shouldStoreEntityType(ENTITY_TYPES.TOKEN_BALANCE),
};

// Log configuration on startup
export function logConfiguration(): void {
    console.log('Storage configuration:');
    console.log(`  ENTITY_TYPES: ${CONFIG.ENTITY_TYPES}`);
    console.log(`  - Store blocks: ${shouldStore.blocks()}`);
    console.log(`  - Store transactions: ${shouldStore.transactions()}`);
    console.log(`  - Store logs: ${shouldStore.logs()}`);
    console.log(`  - Store transfers: ${shouldStore.transfers()}`);
    console.log(`  - Store native balances: ${shouldStore.nativeBalances()}`);
    console.log(`  - Store balance increases: ${shouldStore.tokenBalances()}`);

    console.log(`  BATCH_SIZE: ${CONFIG.BATCH_SIZE}`);
    console.log(`  BALANCE_CHANGES_TTL_DAYS: ${CONFIG.BALANCE_CHANGES_TTL_DAYS}`);
    
    console.log('Output configuration:');
    console.log(`  OUTPUT: ${CONFIG.OUTPUT}`);
    console.log(`  Parsed outputs: ${PARSED_OUTPUTS.allOutputs.join(', ')}`);
    console.log(`  ClickHouse URLs: ${PARSED_OUTPUTS.clickhouseUrls.join(', ')}`);
    console.log(`  AMQP URLs: ${PARSED_OUTPUTS.amqpUrls.join(', ')}`);
    
    console.log('ClickHouse configuration:');
    console.log(`  URL: ${CLICKHOUSE_CONFIG.url}`);
    console.log(`  Username: ${CLICKHOUSE_CONFIG.username}`);
    console.log(`  Database: ${CLICKHOUSE_CONFIG.database}`);
    console.log(`  Timeout: ${CONFIG.CLICKHOUSE_TIMEOUT}ms`);
    console.log(`  Max Retries: ${CONFIG.CLICKHOUSE_MAX_RETRIES}`);
    
    console.log('AMQP configuration:');
    console.log(`  ENABLED: ${AMQP_CONFIG.ENABLED}`);
    console.log(`  URL: ${AMQP_CONFIG.URL}`);
    console.log(`  QUEUE_NAME: ${AMQP_CONFIG.QUEUE_NAME}`);
    console.log(`  EXCHANGE_NAME: ${AMQP_CONFIG.EXCHANGE_NAME}`);
    console.log(`  ROUTING_KEY: ${AMQP_CONFIG.ROUTING_KEY}`);
} 
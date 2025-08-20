# Squid EVM Indexer

A high-performance EVM blockchain indexer built with Subsquid that stores data in ClickHouse. This indexer is optimized for tracking token transfers and balances across multiple token standards (ERC-20, ERC-721, ERC-1155).

## Features

- **Multi-standard Support**: Indexes ERC-20, ERC-721, and ERC-1155 token transfers
- **Balance Tracking**: Maintains current token balances using materialized views
- **Automatic Cleanup**: TTL-based deletion of old balance change records
- **Native Balance Support**: Can track ETH balance changes via state diffs
- **Configurable Storage**: Choose which data types to store via environment variables
- **Batch Processing**: Efficient batch inserts to ClickHouse
- **Docker Support**: Complete containerized deployment
- **Graceful Shutdown**: Ensures all data is flushed on exit
- **Retry Logic**: Automatic retry with exponential backoff for ClickHouse operations

## Table of Contents

- [Quick Start](#quick-start)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [CI/CD](#cicd)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Data Management](#data-management)
- [Performance](#performance)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

## Quick Start

### Local Development

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Set up environment variables**:
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

3. **Start ClickHouse**:
   ```bash
   docker-compose up -d clickhouse
   ```

4. **Run migrations**:
   ```bash
   npm run migrate
   ```

5. **Start the indexer**:
   ```bash
   npm run start
   ```

### Docker Deployment

1. **Setup environment**:
   ```bash
   cp env.example .env
   # Edit .env with your values
   ```

2. **Start everything**:
   ```bash
   npm run docker:up
   ```

3. **Run migrations**:
   ```bash
   docker-compose exec indexer npm run migrate
   ```

4. **View logs**:
   ```bash
   npm run docker:logs
   ```

### Kubernetes Deployment (Helm)

1. **Add Bitnami repository**:
   ```bash
   helm repo add bitnami https://charts.bitnami.com/bitnami
   helm repo update
   ```

2. **Install with Helm**:
   ```bash
   helm install squid-indexer ./helm/squid-indexer \
     --set config.gatewayUrl="https://v2.archive.subsquid.io/network/ethereum-mainnet" \
     --set config.rpcEthHttp="https://rpc.ankr.com/eth" \
     --set clickhouse.auth.password="your-secure-password"
   ```

3. **Production deployment**:
   ```bash
   helm install squid-indexer-prod ./helm/squid-indexer \
     -f ./helm/squid-indexer/values-prod.yaml \
     --namespace squid-indexer-prod \
     --create-namespace
   ```

## Docker Deployment

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- `.env` file with required environment variables

### Docker Commands

```bash
# Build the image
npm run docker:build

# Start all services
npm run docker:up

# Stop all services
npm run docker:down

# View indexer logs
npm run docker:logs

# Run migrations
docker-compose exec indexer npm run migrate

# Access ClickHouse CLI
docker-compose exec clickhouse clickhouse-client --database=base
```

### Services

- **ClickHouse**: Port 8123 (HTTP), 9000 (Native), data persisted in volume
- **PostgreSQL**: Port 5432 (optional, for additional storage)
- **Indexer**: Multi-stage build, non-root user, auto-restart

## Kubernetes Deployment

### Helm Chart Features

- **Security**: Non-root execution, read-only filesystem, dropped capabilities
- **Scalability**: Horizontal Pod Autoscaler support
- **Monitoring**: Prometheus ServiceMonitor integration
- **Flexibility**: Embedded or external ClickHouse options
- **Environments**: Separate values files for dev/staging/prod

### Installation

```bash
# Development
helm install squid-indexer-dev ./helm/squid-indexer \
  -f ./helm/squid-indexer/values-dev.yaml

# Production  
helm install squid-indexer-prod ./helm/squid-indexer \
  -f ./helm/squid-indexer/values-prod.yaml \
  --set externalClickhouse.password="secure-password"
```

### Configuration Options

- **Autoscaling**: Automatic scaling based on CPU/memory
- **Ingress**: TLS termination and load balancing
- **Security**: Pod security contexts and network policies
- **Monitoring**: Prometheus metrics and alerting
- **Storage**: Persistent volumes for ClickHouse data

See [Helm Chart README](./helm/squid-indexer/README.md) for detailed configuration.

## CI/CD

### GitLab CI Pipeline

The project includes a complete GitLab CI/CD pipeline with:

- **Test Stage**: Unit tests, linting, and build verification
- **Build Stage**: Docker image building and registry push
- **Security Stage**: Container vulnerability scanning
- **Deploy Stage**: Automated deployment to dev/staging/prod

### Pipeline Features

- **Multi-environment**: Separate deployments for dev, staging, production
- **Security Scanning**: Trivy container scanning
- **Rollback Support**: Easy rollback to previous versions
- **Manual Gates**: Production deployments require manual approval

### Environment Variables

Configure these in GitLab CI/CD settings:

```bash
# Kubernetes
KUBE_CONFIG                 # Base64 encoded kubeconfig

# Development
DEV_GATEWAY_URL            # Subsquid gateway for dev
DEV_RPC_ETH_HTTP          # RPC endpoint for dev
DEV_CLICKHOUSE_PASSWORD   # ClickHouse password for dev

# Staging  
STAGING_GATEWAY_URL       # Subsquid gateway for staging
STAGING_RPC_ETH_HTTP     # RPC endpoint for staging
STAGING_CLICKHOUSE_PASSWORD # ClickHouse password for staging

# Production
PROD_GATEWAY_URL          # Subsquid gateway for prod
PROD_RPC_ETH_HTTP        # RPC endpoint for prod
PROD_CLICKHOUSE_HOST     # External ClickHouse host
PROD_CLICKHOUSE_PASSWORD # ClickHouse password for prod
```

### Deployment Strategy

- **Develop Branch**: Auto-deploy to development environment
- **Main Branch**: Manual deploy to staging environment  
- **Tags**: Manual deploy to production environment
- **Rollbacks**: Manual rollback jobs available

## Configuration

### Required Environment Variables

```bash
# Network Configuration (Required)
GATEWAY_URL=https://v2.archive.subsquid.io/network/ethereum-mainnet
RPC_ETH_HTTP=https://rpc.ankr.com/eth
RPC_ETH_RATE_LIMIT=10

# ClickHouse Configuration
CLICKHOUSE_HOST=http://localhost:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=password123
CLICKHOUSE_DATABASE=base
```

### Storage Configuration

Control what data to store with these flags:

| Variable | Default | Description |
|----------|---------|-------------|
| `STORE_BLOCKS` | `false` | Store full block data |
| `STORE_TRANSACTIONS` | `false` | Store transaction data |
| `STORE_LOGS` | `false` | Store raw log data |
| `STORE_TRANSFERS` | `true` | Extract and store token transfers |
| `STORE_NATIVE_BALANCES` | `false` | Track ETH balance changes |
| `STORE_BALANCE_INCREASES` | `true` | Store balance increases |
| `STORE_BALANCE_DECREASES` | `true` | Store balance decreases |

### Processing Configuration

```bash
BATCH_SIZE=100                    # Records per batch
BALANCE_CHANGES_TTL_DAYS=90      # Days to keep balance changes
CLICKHOUSE_TIMEOUT=30000         # Request timeout (ms)
CLICKHOUSE_MAX_RETRIES=3         # Max retry attempts
```

### Example Configurations

**Token Balance Tracking Only (Default)**:
```bash
STORE_TRANSFERS=true
STORE_BALANCE_INCREASES=true
STORE_BALANCE_DECREASES=true
BALANCE_CHANGES_TTL_DAYS=90
```

**Full Indexing**:
```bash
STORE_BLOCKS=true
STORE_TRANSACTIONS=true
STORE_LOGS=true
STORE_TRANSFERS=true
STORE_NATIVE_BALANCES=true
STORE_BALANCE_INCREASES=true
STORE_BALANCE_DECREASES=true
BALANCE_CHANGES_TTL_DAYS=180
```

**Native Balance Tracking Only**:
```bash
STORE_NATIVE_BALANCES=true
STORE_TRANSFERS=false
STORE_BALANCE_INCREASES=false
STORE_BALANCE_DECREASES=false
```

## Architecture

### Project Structure

```
sqd-indexer/
├── src/
│   ├── abi/                    # ABI definitions
│   ├── clickhouseClient/       # ClickHouse integration
│   │   ├── clickhouseClient.ts # Client and insert operations
│   │   ├── healthCheck.ts      # Health checks and schema validation
│   │   └── runMigrations.ts    # Migration runner
│   ├── parsers/                # Event parsers
│   │   └── transfer.ts         # Transfer event parsers
│   ├── utils/                  # Utility modules
│   │   └── batch.ts           # Batch management
│   ├── config.ts              # Configuration management
│   ├── types.ts               # TypeScript type definitions
│   └── main.ts                # Main processor entry point
├── db/migrations/             # ClickHouse schema migrations
├── docker-compose.yaml        # Container orchestration
├── Dockerfile                 # Multi-stage container build
└── .dockerignore              # Docker build optimization
```

### Data Flow

1. **Processor Configuration**: Dynamically configured based on enabled features
2. **Event Parsing**: Separate parsers for each token standard
3. **Batch Management**: Efficient batching with automatic flushing
4. **Retry Logic**: Automatic retry with exponential backoff
5. **Automatic Cleanup**: TTL-based data lifecycle management

## Data Management

### Balance Tracking

The indexer uses a sophisticated balance tracking system:

1. **Balance Changes**: Stored in `token_balance_increases` and `token_balance_decreases`
2. **Materialized View**: `current_token_balances_mv` aggregates changes in real-time
3. **Query Views**: Multiple views for different query patterns
4. **TTL Cleanup**: Automatic deletion of old change records

### TTL (Time To Live)

TTL automatically manages storage by deleting old records:

- **Configuration**: Set `BALANCE_CHANGES_TTL_DAYS` (default: 90 days)
- **Scope**: Only affects detailed change records
- **Preservation**: Current balances remain in materialized views
- **Benefits**: Reduces storage costs and improves query performance

#### TTL Monitoring

```sql
-- Check table sizes
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows,
    min(min_date) as oldest_date,
    max(max_date) as newest_date
FROM system.parts
WHERE table IN ('token_balance_increases', 'token_balance_decreases')
GROUP BY table;

-- Force TTL application (resource-intensive)
OPTIMIZE TABLE token_balance_increases FINAL;
OPTIMIZE TABLE token_balance_decreases FINAL;
```

### Database Schema

Key tables created by migrations:

- `token_balance_increases` - Balance increase records (with TTL)
- `token_balance_decreases` - Balance decrease records (with TTL)
- `current_token_balances_mv` - Real-time balance aggregation
- `token_balances` - Query view for current balances
- `current_erc20_balances` - ERC-20 specific view
- `current_nft_ownership` - NFT ownership view
- `nft_collection_stats` - Collection statistics

## Performance

### Optimizations

- **Dynamic Configuration**: Only fetches required data from RPC
- **Batch Processing**: Configurable batch sizes for memory management
- **Materialized Views**: Efficient balance queries
- **Chunked Inserts**: Large batches split to avoid timeouts
- **Retry Logic**: Automatic recovery from transient failures
- **TTL Cleanup**: Automatic removal of old data

### Retry Configuration

The indexer includes robust retry logic:

```bash
CLICKHOUSE_TIMEOUT=30000         # Request timeout (30 seconds)
CLICKHOUSE_MAX_RETRIES=3         # Maximum retry attempts
CLICKHOUSE_RETRY_DELAY=1000      # Initial delay (1 second)
CLICKHOUSE_MAX_RETRY_DELAY=10000 # Maximum delay (10 seconds)
```

Retry triggers:
- Timeout errors
- Connection reset/refused
- Network timeouts

### Chunk Sizes

Data is inserted in optimized chunks:
- **Blocks**: 1,000 records per chunk
- **Transactions**: 1,000 records per chunk
- **Logs**: 2,000 records per chunk
- **Token Transfers**: 1,000 records per chunk
- **Native Balances**: 1,000 records per chunk

## Development

### Adding New Token Standards

1. Add parser function in `src/parsers/transfer.ts`
2. Update the main processing loop in `src/main.ts`
3. Add any new types to `src/types.ts`

### Available Scripts

```bash
# Development
npm run start:ts        # Run with ts-node
npm run dev            # Build and run
npm run build          # Compile TypeScript

# Database
npm run migrate        # Run migrations
npm run migrate:build  # Run with compiled code

# Docker
npm run docker:build   # Build Docker image
npm run docker:up      # Start all services
npm run docker:down    # Stop all services
npm run docker:logs    # View indexer logs
```

### Health Checks

The processor performs automatic health checks:

1. **ClickHouse Connectivity**: Verifies connection and response
2. **Schema Validation**: Ensures required tables exist
3. **Startup Verification**: Logs configuration on startup

Health check failure causes immediate exit with error details.

## Troubleshooting

### Common Issues

**Migration Failures**:
```bash
# Check ClickHouse status
docker-compose ps clickhouse

# View ClickHouse logs
docker-compose logs clickhouse

# Manually run migrations
docker-compose exec indexer npm run migrate
```

**Connection Timeouts**:
```bash
# Increase timeout
CLICKHOUSE_TIMEOUT=60000

# Reduce batch size
BATCH_SIZE=50

# Check ClickHouse performance
docker-compose exec clickhouse clickhouse-client --query "SHOW PROCESSLIST"
```

**Memory Issues**:
```bash
# Reduce batch size
BATCH_SIZE=50

# Monitor memory usage
docker stats sqd-indexer-indexer-1
```

### Monitoring

```bash
# View service status
docker-compose ps

# Check logs
docker-compose logs -f indexer
docker-compose logs -f clickhouse

# Access ClickHouse
docker-compose exec clickhouse clickhouse-client --database=base

# View table sizes
docker-compose exec clickhouse clickhouse-client --database=base --query "
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts
WHERE database = 'base'
GROUP BY table
ORDER BY sum(bytes) DESC"
```

### Production Considerations

1. **Security**:
   - Change default ClickHouse password
   - Use secrets management for sensitive data
   - Consider network security policies

2. **Performance**:
   - Adjust `BATCH_SIZE` based on requirements
   - Monitor ClickHouse resource usage
   - Consider scaling strategies

3. **Monitoring**:
   - Set up log aggregation
   - Monitor container health
   - Track processing metrics

4. **Backup**:
   - Regular ClickHouse data backups
   - Migration file versioning
   - Configuration backups

## FAQ

**Q: Will I lose current balances when TTL expires?**
A: No, current balances are maintained in the materialized view.

**Q: Can I change TTL after table creation?**
A: Yes, use `ALTER TABLE ... MODIFY TTL` command.

**Q: How do I query historical balances?**
A: Use the balance tracking tables within the TTL period, or implement snapshots for longer history.

**Q: Can I recover deleted data?**
A: No, once deleted by TTL, data cannot be recovered. Use backups if needed.

**Q: How do I scale for high throughput?**
A: Increase `BATCH_SIZE`, optimize ClickHouse settings, consider horizontal scaling.

## License

MIT 
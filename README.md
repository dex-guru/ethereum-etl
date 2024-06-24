# Guru Network Ethereum ETL

Ethereum ETL lets you convert blockchain data into convenient formats like CSVs and relational databases.


*Do you just want to query Ethereum data right away? Use the [public dataset in Guru Warehouse](https://warehouse.dex.guru).*


## Quickstart

Copy `.env.sample`, rename with `.env` and set CHAIN_ID and PROVIDER_URL

### Run with docker-compose as a streamer

By default, Ethereum ETL will export entities to clickhouse. You can change the destination in the `.env` file with OUTPUT env variable.

1. Install Docker: https://docs.docker.com/get-docker/
2. Install Docker Compose: https://docs.docker.com/compose/install/
3. Run the following command to start clickhouse:
   ```bash
   docker-compose up -d clickhouse
   ```

4. After initializing clickhouse, run the following command to create database. Name of the database is specified in the `.env` file.:
   
   ```bash
   docker-compose up init-ch-db
   ```

5. You can specify entities to export in the `.env` file.
See supported entities [here](ethereumetl/enumeration/entity_type.py). 

6. Run the following command to start the streamer:
   ```bash
   docker-compose up indexer
   ```

---

### Running certain command

Install Ethereum ETL:

```bash
pip3 install -r requirements.txt
```

IF you want to use clickhouse as a destination, make sure to apply migrations:

```bash
CLICKHOUSE_URL=clickhouse+http://default:@localhost:8123/ethereum alembic upgrade head 
```

Export blocks and transactions ([Schema](db/migrations/schema.sql), [Reference](docs/commands.md#export_blocks_and_transactions)):

```bash
ethereumetl export_blocks_and_transactions --start-block 0 --end-block 500000 \
--blocks-output blocks.csv --transactions-output transactions.csv \
--provider-uri https://mainnet.infura.io/v3/${INFURA_API_KEY}
```

Export ERC20 and ERC721 transfers ([Schema](docs/schema.md#token_transferscsv), [Reference](docs/commands.md##export_token_transfers)):

```bash
ethereumetl export_token_transfers --start-block 0 --end-block 500000 \
--provider-uri file://$HOME/Library/Ethereum/geth.ipc --output token_transfers.csv
```

Export traces ([Schema](docs/schema.md#tracescsv), [Reference](docs/commands.md#export_traces)):

```bash
ethereumetl export_traces --start-block 0 --end-block 500000 \
--provider-uri file://$HOME/Library/Ethereum/parity.ipc --output traces.csv
```

---

Stream blocks, transactions, logs, token_transfers continually to console ([Reference](docs/commands.md#stream)):

```bash
ethereumetl stream --start-block 500000 -e block,transaction,log,token_transfer --log-file log.txt \
--provider-uri https://mainnet.infura.io/v3/7aef3f0cd1f64408b163814b22cc643c
```

Find other commands [here](ethereumetl/cli).

Supported export destinations [here](blockchainetl/jobs/exporters).


## Linters/formatters

### Install

```bash
pip install black ruff mypy
```

### Run

TL;DR all-at-once run and fix:

```bash
ruff check --fix . && black . && mypy .
```

Or one-by-one:
* Check and auto-fix with: `ruff --fix .`
* Check typing: `mypy .`
* Auto-format all: `black .`

## Useful Links

- [Schema](db/migrations/schema.sql)
- [Command Reference](docs/commands.md)


## Running Tests

```bash
export ETHEREUM_ETL_RUN_SLOW_TESTS=True
export PROVIDER_URL=<your_porvider_uri>
pytest -vv
``` 

## Projects using Ethereum ETL
* [Google](https://goo.gl/oY5BCQ) - Public BigQuery Ethereum datasets
* [Nansen](https://nansen.ai/query?ref=ethereumetl) - Analytics platform for Ethereum

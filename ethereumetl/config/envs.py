from typing import Literal

from pydantic import AnyUrl, BaseSettings

from ethereumetl.enumeration import entity_type


class EnvsConfig(BaseSettings):
    CHAIN_ID: int = 1
    # ' file://relative/path/to/file.txt'
    # ' redis://localhost:6379/0?key=last_synced_block'
    # ' postgresql://user:pass@localhost:5432/db?table_name=last_synced_block'
    # ' clickhouse://default:@localhost:8123?table_name=last_synced_block'
    # ' clickhouse+native://default:@localhost:9000?table_name=last_synced_block'
    # ' or any SQLAlchemy supported connection string.'
    # ' Query parameters:'
    # ' table_name, sync_id - table name and primary key for SQL databases;'
    # ' key - key for Redis.'
    LAST_SYNCED_BLOCK_PROVIDER: str = 'file://last_block.txt'
    LAG: int = 0
    PROVIDER_URL: str = 'https://rpc-mainnet.maticvigil.com'
    # help = 'Either Google PubSub topic path e.g. projects/your-project/topics/crypto_ethereum; '
    # 'or Postgres connection url e.g. postgresql+pg8000://postgres:admin@127.0.0.1:5432/ethereum; '
    # 'or Clickhouse connection url e.g. clickhouse://default:@localhost/ethereum'
    # 'or GCS bucket e.g. gs://your-bucket-name; '
    # 'or kafka, output name and connection host:port e.g. kafka/127.0.0.1:9092 '
    # 'or Kinesis, e.g. kinesis://your-data-stream-name'
    # 'If not specified will print to console'
    OUTPUT: str | None = "output.json"
    START_BLOCK: int | None | Literal['latest'] = None
    END_BLOCK: int | None = None
    ENTITY_TYPES: str = ','.join(entity_type.ALL_FOR_STREAMING)
    POLLING_PERIOD: int = 3
    BATCH_SIZE: int = 10
    BLOCK_BATCH_SIZE: int = 1
    MAX_WORKERS: int = 5
    LOGGING_LEVEL: str = 'INFO'
    LOGSTASH_HOST: str = 'logstash-logstash.logging.svc.cluster.local'
    LOGSTASH_PORT: int = 5959
    LOGSTASH_LOGGING_LEVEL: str = 'INFO'
    LOG_HANDLERS: list[str] = ['console']
    SERVICE_NAME: str = ''
    SKIP_NONE_RECEIPTS: bool = False
    MIN_INSERT_BATCH_SIZE: int = 1
    EXPORT_FROM_CLICKHOUSE: AnyUrl | Literal[''] = ''
    # Overwrite these item types read from ClickHouse using EXPORT_FROM_CLICKHOUSE option when the
    # output is set to the same ClickHouse instance. Comma-separated list of item types.
    REWRITE_CLICKHOUSE: str = ''
    # Restart if last synced block wasn't saved for this amount of seconds
    HEALTH_CHECK_TIMEOUT: int = 600
    IS_VERIFIER: bool = False
    IS_SKIP_CYCLE: bool = True
    ELASTIC_URL: str | None = "http://localhost:9200"
    AMQP_URL: str | None = 'amqp://guest:guest@localhost:5672/dex'
    ROUTING_KEY: str | None = ''
    QUEUE_NAME: str | None = ''
    EXCHANGE_NAME: str | None = ''
    PRICE_IMPORTER_URL: str = ''
    MAINNET_DB_URL: str = ''


envs = EnvsConfig()

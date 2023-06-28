from typing import Tuple
from urllib.parse import parse_qs, urlparse

from clickhouse_driver import Client

from ethereumetl.config.envs import envs


def clickhouse_client_from_url(url) -> Tuple[Client, str]:
    parsed = urlparse(url)
    settings = parse_qs(parsed.query)
    connect_kwargs = {
        'host': parsed.hostname,
        'port': parsed.port,
        'user': parsed.username,
        'password': parsed.password,
        'settings': settings,
        'connect_timeout': 600,
        'send_receive_timeout': 600,
    }
    if parsed.path:
        connect_kwargs['database'] = parsed.path[1:]
    if parsed.scheme == "https":
        connect_kwargs['secure'] = True
    return Client(**connect_kwargs), connect_kwargs['database']


def optimize_tables_service(chain_id: int, by_partition: bool = True):
    client, database = clickhouse_client_from_url(envs.OUTPUT)

    # Get the list of all tables
    tables = client.execute(
        f"SELECT name, engine FROM system.tables WHERE database = '{database}'"
    )

    for table in tables:
        # If the table is a ReplacingMergeTree table
        table_name = table[0]
        table_prefix = table_name.split('_')[1]
        if "ReplacingMergeTree" in table[1] and f'{chain_id}_{table_prefix}' == table[0]:
            print(f"Optimizing table {table[0]}")
            if by_partition:
                # Get the list of all partitions for the current table
                partitions = client.execute(
                    f"SELECT DISTINCT partition FROM system.parts WHERE table = '{table_name}' AND active = 1"
                )
                # Execute the OPTIMIZE command for each partition
                for partition in partitions:
                    if partition[0] == '0x4':
                        print(f"Skip as  {partition[0]} in table {table_name} is a projection")
                        continue
                    print(f"Optimizing partition {partition[0]} in table {table_name}")
                    client.execute(f"OPTIMIZE TABLE {table_name} PARTITION {partition[0]} FINAL")
            else:
                # Execute the OPTIMIZE command for the entire table
                client.execute(f"OPTIMIZE TABLE {table_name} FINAL")

    print("Done")


if __name__ == "__main__":
    optimize_tables_service()

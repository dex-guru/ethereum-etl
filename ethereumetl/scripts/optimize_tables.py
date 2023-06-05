from clickhouse_driver import Client


def optimize_tables_service(chain_id: int, by_partition: bool = True):
    host = '10.0.200.180'
    port = 9000  # Default ClickHouse port; change if needed
    user = 'testuser3'
    password = 'testplpassword'
    database = 'dex_etl'

    # Initialize the client
    client = Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

    # Get the list of all tables
    tables = client.execute(f"SELECT name, engine FROM system.tables WHERE database = '{database}'")

    for table in tables:
        # If the table is a ReplacingMergeTree table
        if "ReplacingMergeTree" in table[1] and str(chain_id) in table[0]:
            print(f"Optimizing table {table[0]}")
            table_name = table[0]
            if by_partition:
                # Get the list of all partitions for the current table
                partitions = client.execute(f"SELECT DISTINCT partition FROM system.parts WHERE table = '{table_name}' AND active = 1")
                # Execute the OPTIMIZE command for each partition
                for partition in partitions:
                    print(f"Optimizing partition {partition[0]} in table {table_name}")
                    client.execute(f"OPTIMIZE TABLE {table_name} PARTITION {partition[0]} FINAL")
            else:
                # Execute the OPTIMIZE command for the entire table
                client.execute(f"OPTIMIZE TABLE {table_name} FINAL")

    print("Done")

if __name__ == "__main__":
    optimize_tables_service()


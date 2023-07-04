from clickhouse_driver import Client

host = '10.0.200.180'
port = 9000  # Default ClickHouse port; change if needed
user = 'testuser3'
password = 'testplpassword'
database = 'dex_etl'

# Initialize the client
client = Client(host=host, port=port, user=user, password=password, database=database)

# fmt: off
# Determine the max block number in the blocks table
max_block_number = client.execute('SELECT max(number) FROM dex_etl.`7700_blocks`')[0][0]  # pyright: ignore
# fmt: on

# Initialize the starting block number
start_block_number = 0

# Define the chunk size
chunk_size = 10000

# Continue until all blocks have been checked
while start_block_number <= max_block_number:
    # Query to compare the transaction counts
    print(f'Checking blocks {start_block_number} to {start_block_number + chunk_size - 1}')
    query = f"""
        SELECT
            b.number AS block_number,
            b.transaction_count AS expected_transaction_count,
            count(t.hash) AS actual_transaction_count
        FROM
            dex_etl.`7700_blocks` AS b
            LEFT JOIN dex_etl.`7700_transactions` AS t ON b.number = t.block_number
        WHERE
            b.number BETWEEN {start_block_number} AND {start_block_number + chunk_size - 1}
        GROUP BY
            b.number,
            b.transaction_count
        HAVING
            expected_transaction_count - actual_transaction_count > 1
    """

    # Execute the query and print the results
    results = client.execute(query)
    for result in results:
        print(
            f'Block number: {result[0]}, Expected transaction count: {result[1]}, Actual transaction count: {result[2]}'
        )

    # Move to the next chunk
    start_block_number += chunk_size

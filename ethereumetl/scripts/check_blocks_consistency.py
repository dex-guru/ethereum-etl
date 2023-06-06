from clickhouse_driver import Client
from typing import List, Tuple

def find_missing_blocks(max_number: int, chain_id: int, chunk_size: int = 100000) -> Tuple[List[Tuple[int, int]], int]:
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
    # Initialize variables to keep track of missing blocks and gaps
    missing_blocks = []
    gaps = []
    total_missing = 0
    expected_number = 1

    # Define a query to get block numbers in chunks
    query = '''
        SELECT number
        FROM dex_etl.%(chain_id)s_blocks
        WHERE number >= %(start)s AND number <= %(end)s
        ORDER BY number
    '''

    # Process data in chunks
    for start in range(1, max_number + 1, chunk_size):
        end = min(start + chunk_size - 1, max_number)
        query_result = client.execute(query, {'chain_id': chain_id,
                                              'start': start, 'end': end})

        # Extract block numbers from the query result
        block_numbers = [row[0] for row in query_result]

        # Find gaps in the block numbers
        for number in block_numbers:
            if number > expected_number:
                gap_start = expected_number
                gap_end = number - 1
                gaps.append((gap_start, gap_end))
                total_missing += (gap_end - gap_start + 1)
            expected_number = number + 1

    # Check for a gap between the last block number and the max_number
    if expected_number <= max_number:
        gaps.append((expected_number, max_number))
        total_missing += (max_number - expected_number + 1)

    return gaps, total_missing

# Example usage
max_block_number = 4258478  # Set the maximum block number you want to check
chain_id = 7700  # Set the chain ID you want to check
gaps, total_missing = find_missing_blocks(max_block_number, chain_id)
print("Gaps:", gaps)
print("Total missing blocks:", total_missing)

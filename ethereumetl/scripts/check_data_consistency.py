from collections import deque
from typing import List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from clickhouse_driver import Client

from ethereumetl.config.envs import envs
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.scripts.optimize_tables import optimize_tables_service

SUPPORTED_CHAINS = [1, 250, 42161, 10, 100, 42170, 7700, 7701, 84531]


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


def get_blocks_missing_transactions(chain_id: int, client: Client):
    # Determine the max block number in the blocks table
    max_block_number = client.execute(f'SELECT max(number) FROM dex_etl.{chain_id}_blocks')[0][0]

    # Initialize the starting block number
    start_block_number = 0

    # Define the chunk size
    chunk_size = 1000

    # Continue until all blocks have been checked
    while start_block_number <= max_block_number:
        # Query to compare the transaction sums
        print(
            'Checking blocks {} to {}'.format(
                start_block_number, start_block_number + chunk_size - 1
            )
        )

        query = f"""
            SELECT 
                b.number AS block_number, 
                b.transaction_count AS expected_transaction_sum, 
                SUM(t.value) AS actual_transaction_sum
            FROM 
                dex_etl.{chain_id}_blocks AS b
                LEFT JOIN dex_etl.{chain_id}_transactions AS t ON b.number = t.block_number
            WHERE 
                b.number BETWEEN {start_block_number} AND {start_block_number + chunk_size - 1}
            GROUP BY 
                b.number, 
                b.transaction_count
            HAVING 
                expected_transaction_sum != actual_transaction_sum
        """

        # Execute the query and print the results
        results = client.execute(query)
        blocks_to_fetch = []
        for result in results:
            print(
                f'Block number: {result[0]}, Expected transaction sum: {result[1]}, Actual transaction sum: {result[2]}'
            )
            # Dig deeper to find the missing transactions
            missing_blocks = get_missing_transaction_blocks(
                chain_id, client, result[0], result[0] + chunk_size - 1
            )
            blocks_to_fetch.extend(missing_blocks)
            print(f'Missing transactions in blocks: {missing_blocks}')

        # Move to the next chunk
        start_block_number += chunk_size
        return blocks_to_fetch


def get_missing_transaction_blocks(
    chain_id: int, client: Client, start_block_number: int, end_block_number: int
):
    query = f"""
        SELECT 
            b.number AS block_number, 
            b.transaction_count AS expected_transaction_sum, 
            SUM(t.value) AS actual_transaction_sum
        FROM 
            dex_etl.{chain_id}_blocks AS b
            LEFT JOIN dex_etl.{chain_id}_transactions AS t ON b.number = t.block_number
        WHERE 
            b.number BETWEEN {start_block_number} AND {end_block_number}
        GROUP BY 
            b.number, 
            b.transaction_count
        HAVING 
            expected_transaction_sum != actual_transaction_sum
    """
    results = client.execute(query)
    return [result[0] for result in results]


def get_blocks_count_difference_last_block(client: Client, chain_id: int):
    last_block_number_statement = f"""
    SELECT
      max(number) as block_number
    FROM
      dex_etl.{chain_id}_blocks
    """

    blocks_count_statement = f"""
    SELECT count(number)
    FROM
        dex_etl.{chain_id}_blocks
    """

    last_block_results = client.execute(last_block_number_statement)
    blocks_count_results = client.execute(blocks_count_statement)
    if last_block_results and blocks_count_results:
        return last_block_results[0][0] - blocks_count_results[0][0], last_block_results[0][0]
    else:
        return None, None


def find_blocks_gaps(
    client: Client, chain_id: int, max_number: int, chunk_size: int = 100000
) -> List[Tuple[int, int]]:
    # Initialize variables to keep track of missing blocks and gaps
    gaps = []
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
        query_result = client.execute(query, {'chain_id': chain_id, 'start': start, 'end': end})

        # Extract block numbers from the query result
        block_numbers = [row[0] for row in query_result]

        # Find gaps in the block numbers
        for number in block_numbers:
            if number > expected_number:
                gap_start = expected_number
                gap_end = number - 1
                gaps.append((gap_start, gap_end))
            expected_number = number + 1

    # Check for a gap between the last block number and the max_number
    if expected_number <= max_number:
        gaps.append((expected_number, max_number))
    return gaps


def find_missing_transactions(
    client: Client,
    chain_id: int,
    start_block_number: int,
    end_block_number: int,
    min_range_size: int = 10000,
):
    missing_block_ranges = []

    # Initialize a queue with the initial range
    queue = deque([(start_block_number, end_block_number)])

    while queue:
        start, end = queue.popleft()

        # Get the difference in transactions count
        difference = get_transactions_count_difference(client, chain_id, start, end)

        # If there's no difference or the difference is negative, continue with the next range
        if difference is None or difference <= 0:
            if difference < 0:
                print(f"Duplicates between blocks {start} and {end}")
                optimize_tables_service(chain_id, by_partition=True)
            continue
        # If the difference is positive, and the range is less than or equal to the minimum size, then this range is missing transactions
        elif end - start <= min_range_size:
            missing_block_ranges.append((start, end))
        else:
            print(
                f"Checking blocks {start} and {end} total {end - start} with difference {difference}"
            )
            # If the difference is positive, and the range is larger than the minimum size, then split the range and
            # add the subranges to the queue.
            mid = (start + end) // 2
            queue.append((start, mid))
            queue.append((mid + 1, end))

    # Merge overlapping or adjacent ranges
    missing_block_ranges.sort()
    merged_ranges = []
    current_start, current_end = missing_block_ranges[0]

    for start, end in missing_block_ranges[1:]:
        if start <= current_end + 1:  # adjacent ranges are considered overlapping
            current_end = max(current_end, end)
        else:
            merged_ranges.append((current_start, current_end))
            current_start, current_end = start, end

    merged_ranges.append((current_start, current_end))

    return merged_ranges


def get_transactions_count_difference(
    client: Client,
    chain_id: int,
    start_block_number: Optional[int] = None,
    end_block_number: Optional[int] = None,
) -> int:
    hashes_count_statement = f"""
    SELECT count(hash)
    FROM dex_etl.{chain_id}_transactions
    """
    if start_block_number and end_block_number:
        hashes_count_statement += f"""
        WHERE block_number BETWEEN {start_block_number} AND {end_block_number}
        """

    total_transactions_from_blocks = f"""
    SELECT SUM(transaction_count) AS total_transaction_count
    FROM dex_etl.{chain_id}_blocks
    WHERE transaction_count > 0
    """
    if start_block_number and end_block_number:
        total_transactions_from_blocks += f"""
        AND number BETWEEN {start_block_number} AND {end_block_number}
        """

    hashes_count_results = client.execute(hashes_count_statement)
    total_transactions_from_blocks_results = client.execute(total_transactions_from_blocks)

    if total_transactions_from_blocks_results and hashes_count_results:
        total_transactions_from_blocks_results = total_transactions_from_blocks_results[0][0]
        hashes_count_results = hashes_count_results[0][0]
        diff = total_transactions_from_blocks_results - hashes_count_results
        return diff
    else:
        return None


def get_missing_blocks_ranges(client: Client, chain_id: int):
    blocks_gaps = []
    blocks_diff, last_block = get_blocks_count_difference_last_block(client, chain_id)
    if blocks_diff > 0:
        blocks_gaps = find_blocks_gaps(client, chain_id, last_block)
    return blocks_gaps, last_block, blocks_diff


def get_missing_transactions_blocks_ranges(client: Client, chain_id: int, last_block: int):
    txns_diff = get_transactions_count_difference(client, chain_id)
    transactions_blocks_gaps = []
    if txns_diff > 0:
        print(f"There are {txns_diff} missing transactions on chain_id: {chain_id}")
        transactions_blocks_gaps = find_missing_transactions(
            client, chain_id, start_block_number=1, end_block_number=last_block
        )
    return transactions_blocks_gaps, txns_diff


def parse_consistency_results(data_diff: int, entity_type: EntityType, chain_id: int):
    if data_diff is None:
        print(f"No {entity_type} results found on chain_id: {chain_id}")
    if data_diff > 0:
        print(f"There are {data_diff} missing {entity_type} on chain_id: {chain_id}")
    elif data_diff < 0:
        print(f"There are {data_diff} duplicated {entity_type} on chain_id: {chain_id}")
        optimize_tables_service(chain_id, by_partition=True)
    else:
        print(f"{entity_type} are consistent on chain_id: {chain_id}")


def resolve_data_consistency_service(chain_id: int):
    client, database = clickhouse_client_from_url(envs.OUTPUT)
    blocks_gaps, last_block, blocks_diff = get_missing_blocks_ranges(
        client=client, chain_id=chain_id
    )
    parse_consistency_results(blocks_diff, EntityType.BLOCK, chain_id)
    if blocks_gaps:
        return blocks_gaps

    transactions_blocks_gaps, txns_diff = get_missing_transactions_blocks_ranges(
        client=client, chain_id=chain_id, last_block=last_block
    )
    parse_consistency_results(txns_diff, EntityType.TRANSACTION, chain_id)
    if transactions_blocks_gaps:
        return transactions_blocks_gaps


if __name__ == "__main__":
    chain_id = 7700
    client, database = clickhouse_client_from_url(envs.OUTPUT)

    if not chain_id:
        chains = SUPPORTED_CHAINS
    else:
        chains = [chain_id]
    for chain_id in chains:
        blocks_gaps, last_block, blocks_diff = get_missing_blocks_ranges(
            client=client, chain_id=chain_id
        )
        parse_consistency_results(blocks_diff, EntityType.BLOCK, chain_id)
        transactions_blocks_gaps, txns_diff = get_missing_transactions_blocks_ranges(
            client=client, chain_id=chain_id, last_block=last_block
        )
        parse_consistency_results(txns_diff, EntityType.TRANSACTION, chain_id)

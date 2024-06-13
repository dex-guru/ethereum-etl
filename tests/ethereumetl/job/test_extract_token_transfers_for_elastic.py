import json
from unittest import mock

import tests.resources
from ethereumetl.jobs.exporters.token_transfers_priced_item_exporter import (
    token_transfers_priced_item_exporter,
)
from ethereumetl.jobs.extract_token_transfers_priced import ExtractTokenTransfersPricedJob
from tests.helpers import compare_lines_ignore_order, read_file

RESOURCE_GROUP = 'test_extract_token_transfers_priced_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group], file_name)


def test_extract_token_transfers_priced_job(tmpdir):
    elastic = mock.MagicMock()
    elastic.search.return_value = {
        'aggregations': {
            'group_by_address': {
                'buckets': [
                    {
                        'doc_count': 38,
                        'key': 'test_address',
                        'latest': {
                            'hits': {
                                'total': {'value': 38, 'relation': 'eq'},
                                'max_score': None,
                                'hits': [
                                    {
                                        '_index': 'rounded_candle-20230929',
                                        '_type': '_doc',
                                        '_id': 'S-600-56-all-0xfebe8c1ed424dbf688551d4e2267e7a53698f0aa-1696008000',
                                        '_score': None,
                                        '_source': {'c': 1},
                                        'sort': [1696008000000],
                                    }
                                ],
                            }
                        },
                    }
                ],
                'doc_count_error_upper_bound': 0,
                'sum_other_doc_count': 0,
            }
        }
    }

    output_file = str(tmpdir.join('actual_transfers_priced.json'))
    resource = 'block_with_transfers'
    token_transfers = read_resource(resource, 'token_transfers_priced.json')
    token_transfers_iterable = json.loads(token_transfers)
    tokens = read_resource(resource, 'tokens.json')
    tokens = json.loads(tokens)
    job = ExtractTokenTransfersPricedJob(
        token_transfers=token_transfers_iterable,
        tokens=tokens,
        batch_size=2,
        item_exporter=token_transfers_priced_item_exporter(output_file),
        max_workers=5,
        elastic_client=elastic,
        chain_id=1,
    )
    job.run()

    print('=====================')
    print(read_file(output_file))
    compare_lines_ignore_order(
        read_resource(resource, 'expected_transfers_priced.json'), read_file(output_file)
    )

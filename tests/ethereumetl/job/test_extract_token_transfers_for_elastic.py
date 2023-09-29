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
        'hits': {
            'hits': [
                {
                    '_source': {
                        'address': 'test_address',
                        'c': 1,
                    },
                    "inner_hits": {
                        "latest": {
                            "hits": {
                                "hits": [
                                    {
                                        "_source": {
                                            "address": "test_address",
                                            "c": 1,
                                        }
                                    }
                                ]
                            }
                        }
                    },
                }
            ]
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

import json

import pytest

import tests.resources
from ethereumetl.jobs.exporters.internal_transfers_item_exporter import (
    internal_transfers_item_exporter,
)
from ethereumetl.jobs.extract_internal_transfers_job import ExtractInternalTransfersJob
from tests.helpers import compare_lines_ignore_order, read_file

INTERNAL_TRANSFER_RESOURCE_GROUP = 'test_extract_internal_transfers_job'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource(
        [INTERNAL_TRANSFER_RESOURCE_GROUP, resource_group], file_name
    )


@pytest.mark.parametrize(
    "resource_group",
    [
        'tx_with_internal_transfers',
        'tx_without_internal_transfers',
    ],
)
def test_extract_internal_transfers_job(tmpdir, resource_group):
    output_file = str(tmpdir.join('actual_internal_transfers.json'))
    geth_traces_content = read_resource(resource_group, 'parsed_geth_trace.json')
    traces_iterable = (json.loads(line) for line in geth_traces_content.splitlines())
    job = ExtractInternalTransfersJob(
        geth_traces_iterable=traces_iterable,
        batch_size=2,
        item_exporter=internal_transfers_item_exporter(output_file),
        max_workers=5,
    )
    job.run()
    compare_lines_ignore_order(
        read_resource(resource_group, 'expected_internal_transfers.json'), read_file(output_file)
    )

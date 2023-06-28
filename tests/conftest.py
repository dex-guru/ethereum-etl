import json
import os
from unittest import mock

import pytest

pytest.register_assert_rewrite("tests.helpers")


@pytest.fixture(scope="session", autouse=True)
def cache_web3_post_requests():
    if os.getenv('CACHE_WEB3_RESPONSES', '').lower() not in ('true', '1'):
        yield
        return

    from diskcache import Cache, JSONDisk
    from web3 import HTTPProvider

    from ethereumetl.providers.rpc import BatchHTTPProvider

    cache = Cache(
        directory=os.path.dirname(__file__) + '/.cache',
        size_limit=10 * 2**20,
        disk=JSONDisk,
        disk_compress_level=0,
    )

    orig_make_request = HTTPProvider.make_request

    def make_request(self, *args):
        try:
            return cache[args]
        except KeyError:
            res = orig_make_request(self, *args)
            cache[args] = res
            return res

    def make_batch_request(self, reqs_json):
        return [
            {**make_request(self, r['method'], r['params']), 'id': r['id']}
            for r in json.loads(reqs_json)
        ]

    with (
        mock.patch.object(
            HTTPProvider,
            'make_request',
            make_request,
        ),
        mock.patch.object(
            BatchHTTPProvider,
            'make_batch_request',
            make_batch_request,
        ),
    ):
        yield

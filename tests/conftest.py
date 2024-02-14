import json
import os
from collections.abc import Generator
from datetime import datetime
from random import randint
from unittest import mock
from unittest.mock import MagicMock
from urllib.parse import urlunparse

import clickhouse_connect
import pytest
from elasticsearch import Elasticsearch
from retry import retry
from web3 import Web3

from ethereumetl.clickhouse import migrate_up
from ethereumetl.service.eth_resolve_log_service import EthResolveLogService
from ethereumetl.utils import parse_clickhouse_url

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


@pytest.fixture
def clickhouse_url() -> Generator[str, None, None]:
    url = os.getenv('TEST_CLICKHOUSE_URL')
    assert url, 'TEST_CLICKHOUSE_URL env var must be set'
    test_db_name = (
        os.getenv('TEST_CLICKHOUSE_DB_NAME') or f'_ethereum_etl_test_{randint(0, 999_999)}'
    )

    def cleanup(client):
        client.command(f'DROP DATABASE IF EXISTS {test_db_name} SYNC')

    params = parse_clickhouse_url(url)
    with clickhouse_connect.create_client(**params) as admin_client:
        cleanup(admin_client)
        admin_client.command(f'CREATE DATABASE {test_db_name}')

        test_params = params.copy()
        test_params['database'] = test_db_name

        test_url = urlunparse(
            (
                'clickhouse+http' if test_params['port'] == 8123 else 'clickhouse+native',
                (
                    f'{test_params["user"]}:{test_params["password"]}'
                    f'@{test_params["host"]}:{test_params["port"]}'
                ),
                '/' + test_params['database'],
                None,
                None,
                None,
            )
        )

        yield test_url

        cleanup(admin_client)


@pytest.fixture
def elastic_url() -> Generator[str, None, None]:
    url = os.getenv('TEST_ELASTICSEARCH_URL')
    assert url, 'TEST_ELASTICSEARCH_URL env var must be set'

    test_index_name = 'transactions_all'
    candles_index_name = 'rounded_candle*'

    def cleanup(client):
        client.indices.delete(index=test_index_name, ignore=[400, 404])
        client.indices.delete(index=candles_index_name, ignore=[400, 404])

    @retry(Exception, tries=60, delay=1)
    def wait_for_create_connection(client):
        client.info()

    with Elasticsearch(url) as admin_client:
        wait_for_create_connection(admin_client)
        cleanup(admin_client)
        admin_client.indices.create(index=test_index_name)
        today_date = datetime.now().strftime('%Y%m%d')
        admin_client.indices.create(
            index=f'rounded_candle-{today_date}',
            mappings={
                "properties": {
                    "address": {"type": "keyword"},
                    "c": {"type": "float"},
                    "t_rounded": {"type": "long"},
                }
            },
        )
        yield url
        cleanup(admin_client)


@pytest.fixture
def clickhouse(clickhouse_url) -> Generator[clickhouse_connect.driver.Client, None, None]:
    params = parse_clickhouse_url(clickhouse_url)
    with clickhouse_connect.create_client(**params) as client:
        yield client


@pytest.fixture
def clickhouse_migrated_url(clickhouse_url) -> str:
    migrate_up(clickhouse_url, 'head')
    return clickhouse_url


@pytest.fixture
def clickhouse_migrated(clickhouse_migrated_url, clickhouse) -> clickhouse_connect.driver.Client:
    return clickhouse


@pytest.fixture(scope="function")
def web3():
    return Web3(MagicMock())


@pytest.fixture()
def eth_resolve_log_service(web3):
    return EthResolveLogService(web3, 1)

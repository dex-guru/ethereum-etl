import os
from typing import Any

from web3 import HTTPProvider

from ethereumetl.providers.rpc import BatchHTTPProvider
from tests.ethereumetl.job.mock_batch_web3_provider import (
    MockBatchWeb3OrWeb3Provider,
    MockBatchWeb3Provider,
)
from tests.ethereumetl.job.mock_web3_provider import MockWeb3OrWeb3Provider, MockWeb3Provider


def get_web3_provider(
    provider_type, read_resource_lambda=None, batch=False, write_resource_lambda=None
):
    if provider_type == 'mock':
        if read_resource_lambda is None:
            raise ValueError(
                "read_resource_lambda must not be None for provider type mock".format()
            )
        if batch:
            provider: Any = MockBatchWeb3Provider(read_resource_lambda)
        else:
            provider = MockWeb3Provider(read_resource_lambda)
    elif provider_type == 'infura':
        provider_url = os.environ.get('PROVIDER_URL', 'http://rpc-gw-stage.dexguru.biz/full/1')
        if batch:
            provider = BatchHTTPProvider(provider_url)
        else:
            provider = HTTPProvider(provider_url)
    elif provider_type == 'mock_or_infura':
        if read_resource_lambda is None:
            raise ValueError(
                f"read_resource_lambda must not be None for provider type {provider_type}"
            )
        if write_resource_lambda is None:
            raise ValueError(
                f"write_resource_lambda must not be None for provider type {provider_type}"
            )

        provider_url = os.environ.get('PROVIDER_URL', 'http://rpc-gw-stage.dexguru.biz/full/1')
        if batch:
            real_provider: Any = BatchHTTPProvider(provider_url)
            provider = MockBatchWeb3OrWeb3Provider(
                read_resource_lambda, write_resource_lambda, real_provider
            )
        else:
            real_provider = HTTPProvider(provider_url)
            provider = MockWeb3OrWeb3Provider(
                read_resource_lambda, write_resource_lambda, real_provider
            )
    else:
        raise ValueError(f'Provider type {provider_type} is unexpected')
    return provider

# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import itertools
import json
import threading
import time
import warnings
from collections.abc import Collection
from datetime import datetime
from functools import cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import clickhouse_connect
import pytz
from clickhouse_connect.driver import Client

from ethereumetl.config.envs import envs
from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from ethereumetl.misc.retriable_value_error import RetriableValueError
from ethereumetl.providers.rpc import BatchHTTPProvider


def hex_to_dec(hex_string):
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except ValueError:
        return hex_string
    except TypeError:
        return hex_string


def to_int_or_none(val):
    if isinstance(val, int):
        return val
    if val is None or val == '':
        return None
    try:
        return int(val)
    except ValueError:
        return None


def chunk_string(string, length):
    return (string[0 + i : length + i] for i in range(0, len(string), length))


def to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()


def validate_range(range_start_incl, range_end_incl):
    if range_start_incl < 0 or range_end_incl < 0:
        raise ValueError('range_start and range_end must be greater or equal to 0')

    if range_end_incl < range_start_incl:
        raise ValueError('range_end must be greater or equal to range_start')


def rpc_response_batch_to_results(response):
    for response_item in response:
        yield rpc_response_to_result(response_item)


def rpc_response_to_result(response):
    if not isinstance(response, dict):
        raise TypeError(f'bad rpc response: expected dict, got {type(response)}')

    result = response.get('result')
    if result is None:
        error_message = f'result is None in response {response}.'
        error: dict | None
        if response.get('error') is None:
            error_message = error_message + ' Make sure Ethereum node is synced.'
            # When nodes are behind a load balancer it makes sense to retry the request in hopes it will go to other,
            # synced node
            raise RetriableValueError(error_message)
        elif (error := response.get('error')) is not None and is_retriable_error(
            error.get('code')
        ):
            raise RetriableValueError(error_message)
        raise ValueError(error_message)
    return result


def is_retriable_error(error_code):
    if error_code is None:
        return False

    if not isinstance(error_code, int):
        return False

    # https://www.jsonrpc.org/specification#error_object
    if error_code == -32603 or (-32000 >= error_code >= -32099):
        return True

    return False


def split_to_batches(start_incl, end_incl, batch_size):
    """start_incl and end_incl are inclusive, the returned batch ranges are also inclusive."""
    for batch_start in range(start_incl, end_incl + 1, batch_size):
        batch_end = min(batch_start + batch_size - 1, end_incl)
        yield batch_start, batch_end


def pairwise(iterable):
    """S -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def check_classic_provider_uri(chain, provider_uri):
    if chain == 'classic' and provider_uri == 'https://mainnet.infura.io':
        warnings.warn(
            "ETC Chain not supported on Infura.io. Using https://ethereumclassic.network instead"
        )
        return 'https://ethereumclassic.network'
    return provider_uri


def timestamp_now() -> int:
    return int(datetime.now(tz=pytz.UTC).timestamp())


def dedup_list_of_dicts(items_list: list) -> list:
    seen = set()
    new_l = []
    for d in items_list:
        t = tuple(d.items())
        if t not in seen:
            seen.add(t)
            new_l.append(d)
    return new_l


class HealthCheck:
    """
    Health check decorator class. It touches the "healthy" file.
    If the worker is not alive, K8s kills the process.

    Important:
    ---------
    This decorator should be set on the periodically called function.
    It needs to set livenessProbe in the helm chart.

    Usage:

    def main():
        while True:
            foo()

    @HealthCheck
    def foo():
        do_something()

    if __name__ == '__main__':
        main()

    """

    def __init__(self, func):
        self.timeout = envs.HEALTH_CHECK_TIMEOUT
        self._last_check = time.monotonic()
        self.func = func
        Path('/tmp/healthy').touch()

    def im_alive(self):
        if time.monotonic() - self._last_check > self.timeout / 3:
            Path('/tmp/healthy').touch()
            self._last_check = time.monotonic()

    def __call__(self, *args, **kwargs):
        self.im_alive()
        return self.func(*args, **kwargs)


def parse_clickhouse_url(url) -> dict[str, Any]:
    parsed = urlparse(url)
    settings = parse_qs(parsed.query)
    connect_kwargs = {
        'host': parsed.hostname or 'localhost',
        'port': parsed.port or (8123 if "http" in parsed.scheme else 9000),
        'user': parsed.username or 'default',
        'password': parsed.password or '',
        'settings': settings,
    }
    if parsed.path:
        connect_kwargs['database'] = parsed.path[1:]
    else:
        connect_kwargs['database'] = 'default'
    if parsed.scheme == "https":
        connect_kwargs['secure'] = True
    connect_kwargs.update((k, v) for k, vs in parse_qs(parsed.query).items() for v in vs)
    return connect_kwargs


def execute_in_batches(
    batch_web3_provider: BatchHTTPProvider,
    batch_work_executor: BatchWorkExecutor,
    rpc_requests: Collection[dict],
) -> list[dict]:
    """
    Returns a list of RPC responses. The order of responses is the same as the order of requests.
    """
    lock = threading.Lock()
    rpc_responses: list[dict] = []

    def execute_rpc_batch(rpc_requests):
        request_text = json.dumps(rpc_requests, check_circular=False)
        batch_response = batch_web3_provider.make_batch_request(request_text)
        with lock:
            rpc_responses.extend(batch_response)

    batch_work_executor.execute(rpc_requests, execute_rpc_batch)
    batch_work_executor.shutdown()

    if len(rpc_responses) != len(rpc_requests):
        raise ValueError('batch RPC: response count does not match request count')

    rpc_responses_by_id = {r['id']: r for r in rpc_responses}
    try:
        return [rpc_responses_by_id[request['id']] for request in rpc_requests]
    except KeyError:
        raise ValueError('batch RPC: some request ids are missing in the response')


def get_default_prices(token_count: int) -> list[list[float]]:
    prices = [[1.0] * token_count for _ in range(token_count)]
    return prices


def get_default_zero_prices(token_count: int) -> list[list[float]]:
    prices = [[0.0] * token_count for _ in range(token_count)]
    return prices


def get_prices_for_two_pool(token0_price: float, token1_price: float):
    prices = get_default_prices(2)
    prices[0][1] = token1_price
    prices[1][0] = token0_price
    return prices


def get_price_matrix_for_price_array(price_array: list) -> list[list[float]]:
    count = len(price_array)
    prices = get_default_prices(count)
    for i in range(count):
        for j in range(count):
            if i == j:
                continue
            prices[i][j] = price_array[j] / price_array[i]
    return prices


class Singleton(type):
    _instances: dict[type, Any] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


@cache
def clickhouse_client_from_url(url) -> Client:
    connect_kwargs = parse_clickhouse_url(url)
    return clickhouse_connect.create_client(
        **connect_kwargs, compress=False, query_limit=0, send_receive_timeout=600
    )

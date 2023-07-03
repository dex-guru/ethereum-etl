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
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytz

from ethereumetl.config.envs import envs
from ethereumetl.misc.retriable_value_error import RetriableValueError


def hex_to_dec(hex_string):
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except ValueError:
        print("Not a hex string %s" % hex_string)
        return hex_string
    except TypeError:
        print("Not a hex string %s" % hex_string)
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
        error_message = 'result is None in response {}.'.format(response)
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


def dynamic_batch_iterator(iterable, batch_size_getter):
    batch = []
    batch_size = batch_size_getter()
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
            batch_size = batch_size_getter()
    if len(batch) > 0:
        yield batch


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
        'port': parsed.port or 8123,
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
    return connect_kwargs

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

import json
import logging
from datetime import datetime


class EthItemTimestampCalculator:
    @staticmethod
    def calculate(item):
        if item is None or not isinstance(item, dict):
            return None

        item_type = item.get('type')
        if item_type in ('token', 'contract', 'pre_event', 'dex_pool'):
            return None

        if item.get('timestamp') is not None:
            return epoch_seconds_to_rfc3339(item.get('timestamp'))
        if item.get('block_timestamp') is not None:
            return epoch_seconds_to_rfc3339(item.get('block_timestamp'))

        logging.warning(f'item_timestamp for item {json.dumps(item)} is None')

        return None


def epoch_seconds_to_rfc3339(timestamp):
    if isinstance(timestamp, datetime):
        return timestamp.isoformat() + 'Z'
    return datetime.utcfromtimestamp(int(timestamp)).isoformat() + 'Z'

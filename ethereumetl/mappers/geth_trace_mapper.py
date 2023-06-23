# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
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
from datetime import datetime

from ethereumetl.domain.geth_trace import EthGethTrace
import json


class EthGethTraceMapper(object):
    @staticmethod
    def json_dict_to_geth_trace(json_dict):
        transaction_hash = json_dict.get('transaction_hash')
        block_number = json_dict.get('block_number')
        if isinstance(json_dict.get('block_timestamp'), int):
            block_timestamp = datetime.utcfromtimestamp(json_dict['block_timestamp'])
        elif isinstance(json_dict.get('block_timestamp'), datetime):
            block_timestamp = json_dict['block_timestamp']
        else:
            block_timestamp = None
        if json_dict.get('transaction_traces'):
            if isinstance(json_dict['transaction_traces'], str):
                transaction_traces = json.loads(json_dict['transaction_traces'])
            else:
                transaction_traces = json_dict['transaction_traces']
        else:
            transaction_traces = json.loads(json_dict.get('traces_json', '[]'))

        geth_trace = EthGethTrace(
            transaction_hash=transaction_hash,
            transaction_traces=transaction_traces,
        )

        return geth_trace

    @staticmethod
    def geth_trace_to_dict(geth_trace: EthGethTrace):
        return {
            'type': 'geth_trace',
            'transaction_hash': geth_trace.transaction_hash,
            'transaction_traces': json.dumps(geth_trace.transaction_traces),
        }

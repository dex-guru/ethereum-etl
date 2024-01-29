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
from dataclasses import asdict
from typing import Any

from ethereumetl.domain.trace import EthTrace
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from ethereumetl.utils import hex_to_dec, to_normalized_address


class EthTraceMapper:
    @staticmethod
    def json_dict_to_trace(json_dict: dict) -> EthTrace:
        action = json_dict.get('action')
        if action is None:
            action = {}

        trace = EthTrace(
            block_number=json_dict.get('blockNumber'),
            transaction_hash=json_dict.get('transactionHash'),
            transaction_index=json_dict.get('transactionPosition'),
            subtraces=json_dict.get('subtraces'),
            trace_address=json_dict.get('traceAddress', []),
            error=json_dict.get('error'),
        )

        result = json_dict.get('result')
        if result is None:
            result = {}

        trace_type = json_dict.get('type')
        trace.trace_type = trace_type

        # common fields in call/create
        if trace_type in ('call', 'create'):
            trace.from_address = to_normalized_address(action.get('from'))
            trace.value = hex_to_dec(action.get('value'))
            trace.gas = hex_to_dec(action.get('gas'))
            trace.gas_used = hex_to_dec(result.get('gasUsed'))

        # process different trace types
        if trace_type == 'call':
            trace.call_type = action.get('callType')
            trace.to_address = to_normalized_address(action.get('to'))
            trace.input = action.get('input')
            trace.output = result.get('output')
        elif trace_type == 'create':
            trace.to_address = result.get('address')
            trace.input = action.get('init')
            trace.output = result.get('code')
        elif trace_type == 'suicide':
            trace.from_address = to_normalized_address(action.get('address'))
            trace.to_address = to_normalized_address(action.get('refundAddress'))
            trace.value = hex_to_dec(action.get('balance'))
        elif trace_type == 'reward':
            trace.to_address = to_normalized_address(action.get('author'))
            trace.value = hex_to_dec(action.get('value'))
            trace.reward_type = action.get('rewardType')

        return trace

    def geth_trace_to_traces(self, geth_trace):
        block_number = geth_trace.block_number
        transaction_traces = geth_trace.transaction_traces

        traces = []

        for tx_index, tx_trace in enumerate(transaction_traces):
            traces.extend(
                self._iterate_transaction_trace(
                    block_number,
                    tx_index,
                    tx_trace,
                )
            )

        return traces

    @staticmethod
    def genesis_alloc_to_trace(allocation):
        address = allocation[0]
        value = allocation[1]

        trace = EthTrace(
            block_number=0,
            to_address=address,
            value=value,
            trace_type='genesis',
            status=1,
        )

        return trace

    @staticmethod
    def daofork_state_change_to_trace(state_change):
        from_address = state_change[0]
        to_address = state_change[1]
        value = state_change[2]

        trace = EthTrace(
            block_number=DAOFORK_BLOCK_NUMBER,
            from_address=from_address,
            to_address=to_address,
            value=value,
            trace_type='daofork',
            status=1,
        )
        return trace

    def _iterate_transaction_trace(self, block_number, tx_index, tx_trace, trace_address=None):
        if trace_address is None:
            trace_address = []
        trace = EthTrace(
            block_number=block_number,
            transaction_index=tx_index,
            from_address=to_normalized_address(tx_trace.get('from')),
            to_address=to_normalized_address(tx_trace.get('to')),
            input=tx_trace.get('input'),
            output=tx_trace.get('output'),
            value=hex_to_dec(tx_trace.get('value')),
            gas=hex_to_dec(tx_trace.get('gas')),
            gas_used=hex_to_dec(tx_trace.get('gasUsed')),
            error=tx_trace.get('error'),
            # lowercase for compatibility with parity traces
            trace_type=tx_trace.get('type').lower(),
        )
        if trace.trace_type == 'selfdestruct':
            # rename to suicide for compatibility with parity traces
            trace.trace_type = 'suicide'
        elif trace.trace_type in ('call', 'callcode', 'delegatecall', 'staticcall'):
            trace.call_type = trace.trace_type
            trace.trace_type = 'call'

        result = [trace]

        calls = tx_trace.get('calls', [])

        trace.subtraces = len(calls)
        trace.trace_address = trace_address

        for call_index, call_trace in enumerate(calls):
            result.extend(
                self._iterate_transaction_trace(
                    block_number, tx_index, call_trace, [*trace_address, call_index]
                )
            )

        return result

    @staticmethod
    def trace_to_dict(trace: EthTrace) -> dict[str, Any]:
        result = asdict(trace)
        result['type'] = str(EntityType.TRACE.value)
        return result

from dataclasses import asdict

from ethereumetl.domain.geth_trace import EthGethTrace
from ethereumetl.domain.internal_transfer import InternalTransfer
from ethereumetl.service.token_transfer_extractor import word_to_address
from ethereumetl.utils import hex_to_dec


class InternalTransferMapper(object):
    @staticmethod
    def geth_trace_to_internal_transfers(geth_trace: EthGethTrace):
        traces = geth_trace.transaction_traces
        transaction_hash = geth_trace.transaction_hash
        depth_0 = '0'

        def dfs(trace, depth):
            if trace.get('value') is not None:
                yield InternalTransfer(
                    from_address=word_to_address(trace.get('from')),
                    to_address=word_to_address(trace.get('to')),
                    value=hex_to_dec(trace['value'])
                    if isinstance(trace['value'], str)
                    else trace['value'],
                    transaction_hash=transaction_hash,
                    id=trace.get('type', 'call').lower() + f'_{depth}',
                    gas_limit=hex_to_dec(trace.get('gas', '0x0'))
                    if isinstance(trace.get('gas'), str)
                    else trace.get('gas'),
                )
            for trace_id, subtrace in enumerate(trace.get('calls', [])):
                if trace_id == 0:
                    depth = f'{depth}{trace_id}'
                else:
                    depth = depth[:-1] + str(trace_id)
                yield from dfs(subtrace, depth)

        return list(dfs(traces, depth_0))

    @staticmethod
    def internal_transfer_to_dict(internal_transfer):
        internal_transfer_as_dict = asdict(internal_transfer)
        internal_transfer_as_dict['type'] = 'internal_transfer'
        return internal_transfer_as_dict

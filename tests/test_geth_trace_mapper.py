from ethereumetl.domain.geth_trace import EthGethTrace
from ethereumetl.mappers.geth_trace_mapper import EthGethTraceMapper


def test_geth_trace_mapper():
    mapper = EthGethTraceMapper()

    geth_trace = EthGethTrace(
        transaction_hash='0x9a5437ec71b74ecf5930b406908ac6999966d38a86d1534b7190ece7599095eb',
        transaction_traces={
            'from': '0xaf21e07e5a929d16026a7b4d88f3906a8d2e4942',
            'gas': '0x0',
            'gasUsed': '0x0',
            'input': '0x',
            'output': '0x',
            'time': '5.168µs',
            'to': '0x5b3c526b152b1f3d8eabe2ec27f49b904ad51cad',
            'type': 'CALL',
            'value': '0x3814695e26625c000',
        },
    )

    geth_trace_dict = mapper.geth_trace_to_dict(geth_trace)
    geth_trace_from_dict = mapper.json_dict_to_geth_trace(geth_trace_dict)

    assert geth_trace == geth_trace_from_dict

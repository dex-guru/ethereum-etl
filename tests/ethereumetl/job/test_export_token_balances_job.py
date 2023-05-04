from ethereumetl.domain.token_balance import EthTokenBalance
from ethereumetl.domain.token_transfer import EthTokenTransferItem
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_token_balances_job import ExportTokenBalancesJob, TokenBalanceParams
from ethereumetl.mappers.token_balance_mapper import EthTokenBalanceMapper


def test_token_balances():
    job = ExportTokenBalancesJob

    token_transfer: EthTokenTransferItem = {
        "type": "token_transfer",
        "token_address": "0xd1988bea35478229ebee68331714b215e3529510",
        "from_address": "0xd8444ef1a23a6811994fc557921949e3327967ce",
        "to_address": "0xd0b0f29f96a55617786439ccb824e75e55c56b66",
        "value": 1,
        "transaction_hash": "0xf43dab5e60694814bb196d20904b6bf0288f7611aa356a133e0035893c4c76b8",
        "log_index": 225,
        "block_number": 17179063,
        "token_standard": "ERC-1155",
        "token_id": 1,
        "operator_address": "0xd8444ef1a23a6811994fc557921949e3327967ce",
    }

    rpc_params1, rpc_params2 = job.prepare_params(token_transfer)
    assert rpc_params1 == TokenBalanceParams(
        token_address="0xd1988bea35478229ebee68331714b215e3529510",
        holder_address="0xd8444ef1a23a6811994fc557921949e3327967ce",
        block_number=17179063,
        token_id=1,
    )
    assert rpc_params2 == TokenBalanceParams(
        token_address="0xd1988bea35478229ebee68331714b215e3529510",
        holder_address="0xd0b0f29f96a55617786439ccb824e75e55c56b66",
        block_number=17179063,
        token_id=1,
    )

    request1 = job.make_rpc_request(0, rpc_params1)
    assert request1 == {
        "id": 0,
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {
                "data": (
                    "0x00fdd58e"
                    "000000000000000000000000d8444ef1a23a6811994fc557921949e3327967ce"
                    "0000000000000000000000000000000000000000000000000000000000000001"
                ),
                "to": "0xd1988bea35478229ebee68331714b215e3529510",
            },
            "0x10621b7",
        ],
    }

    request2 = job.make_rpc_request(1, rpc_params2)
    assert request2 == {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {
                "data": (
                    "0x00fdd58e"
                    "000000000000000000000000d0b0f29f96a55617786439ccb824e75e55c56b66"
                    "0000000000000000000000000000000000000000000000000000000000000001"
                ),
                "to": "0xd1988bea35478229ebee68331714b215e3529510",
            },
            "0x10621b7",
        ],
    }

    response1 = {
        "jsonrpc": "2.0",
        "id": 0,
        "result": "0x0000000000000000000000000000000000000000000000000000000000000884",
    }

    response2 = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": "0x0000000000000000000000000000000000000000000000000000000000000001",
    }

    token_balance1 = job.make_token_balance(rpc_params1, response1)
    assert token_balance1 == EthTokenBalance(
        token_address="0xd1988bea35478229ebee68331714b215e3529510",
        holder_address="0xd8444ef1a23a6811994fc557921949e3327967ce",
        block_number=17179063,
        value=2180,
        token_id=1,
    )

    token_balance2 = job.make_token_balance(rpc_params2, response2)
    assert token_balance2 == EthTokenBalance(
        token_address="0xd1988bea35478229ebee68331714b215e3529510",
        holder_address="0xd0b0f29f96a55617786439ccb824e75e55c56b66",
        block_number=17179063,
        value=1,
        token_id=1,
    )

    token_balance_item1 = EthTokenBalanceMapper.token_balance_to_dict(token_balance1)
    assert token_balance_item1 == {
        "type": EntityType.TOKEN_BALANCE,
        "token_address": "0xd1988bea35478229ebee68331714b215e3529510",
        "holder_address": "0xd8444ef1a23a6811994fc557921949e3327967ce",
        "block_number": 17179063,
        "value": 2180,
        "token_id": 1,
    }

    token_balance_item2 = EthTokenBalanceMapper.token_balance_to_dict(token_balance2)
    assert token_balance_item2 == {
        "type": EntityType.TOKEN_BALANCE,
        "token_address": "0xd1988bea35478229ebee68331714b215e3529510",
        "holder_address": "0xd0b0f29f96a55617786439ccb824e75e55c56b66",
        "block_number": 17179063,
        "value": 1,
        "token_id": 1,
    }

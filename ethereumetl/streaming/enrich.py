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
import dataclasses
import itertools
from collections import defaultdict
from dataclasses import fields
from datetime import datetime

from ethereumetl.domain.internal_transfer import InternalTransfer
from ethereumetl.domain.token_balance import EthTokenBalance
from ethereumetl.domain.token_transfer_priced import TokenTransferPriced
from ethereumetl.mappers.error_mapper import EthErrorMapper
from ethereumetl.mappers.native_balance_mapper import EthNativeBalanceItem
from ethereumetl.utils import dedup_list_of_dicts


def join(left, right, join_fields, left_fields, right_fields):
    left_join_field, right_join_field = join_fields

    def field_list_to_dict(field_list):
        result_dict = {}
        for field in field_list:
            if isinstance(field, tuple):
                result_dict[field[0]] = field[1]
            else:
                result_dict[field] = field
        return result_dict

    left_fields_as_dict = field_list_to_dict(left_fields)
    right_fields_as_dict = field_list_to_dict(right_fields)

    left_map = defaultdict(list)
    for item in left:
        left_map[item[left_join_field]].append(item)

    right_map = defaultdict(list)
    for item in right:
        right_map[item[right_join_field]].append(item)

    for key in left_map:
        for left_item, right_item in itertools.product(left_map[key], right_map[key]):
            result_item = {}
            for src_field, dst_field in left_fields_as_dict.items():
                result_item[dst_field] = left_item.get(src_field)
            for src_field, dst_field in right_fields_as_dict.items():
                result_item[dst_field] = right_item.get(src_field)

            yield result_item


def enrich_transactions(transactions, receipts):
    transactions = dedup_list_of_dicts(transactions)
    receipts = dedup_list_of_dicts(receipts)
    result = list(
        join(
            transactions,
            receipts,
            ('hash', 'transaction_hash'),
            left_fields=[
                'type',
                'hash',
                'nonce',
                'transaction_index',
                'from_address',
                'to_address',
                'value',
                'gas',
                'gas_price',
                'input',
                'block_timestamp',
                'block_number',
                'block_hash',
                'max_fee_per_gas',
                'max_priority_fee_per_gas',
                'transaction_type',
            ],
            right_fields=[
                ('cumulative_gas_used', 'receipt_cumulative_gas_used'),
                ('gas_used', 'receipt_gas_used'),
                ('contract_address', 'receipt_contract_address'),
                ('root', 'receipt_root'),
                ('status', 'receipt_status'),
                ('effective_gas_price', 'receipt_effective_gas_price'),
                ('logs_count', 'receipt_logs_count'),
            ],
        )
    )

    if len(result) < min(len(transactions), len(receipts)):
        raise ValueError(
            "transaction count is wrong after enriching with receipt:"
            f" before_transactions={len(transactions)}"
            f", before_receipts={len(receipts)}"
            f", after={len(result)}"
        )

    return result


def enrich_logs(blocks, logs):
    result = list(
        join(
            logs,
            blocks,
            ('block_number', 'number'),
            [
                'type',
                'log_index',
                'transaction_hash',
                'transaction_index',
                'address',
                'data',
                'topics',
                'block_number',
            ],
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )

    if len(result) != len(logs):
        raise ValueError(
            f"log count changed after enriching with block: {len(logs)} -> {len(result)}"
        )

    return result


def enrich_token_transfers(blocks, token_transfers):
    result = list(
        join(
            token_transfers,
            blocks,
            ('block_number', 'number'),
            [
                'type',
                'token_address',
                'from_address',
                'to_address',
                'value',
                'transaction_hash',
                'log_index',
                'block_number',
                'token_standard',
                'token_id',
                'operator_address',
            ],
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )

    if len(result) != len(token_transfers):
        raise ValueError(
            "token transfer count changed after enriching with block:"
            f" {len(token_transfers)} -> {len(result)}"
        )

    return result


def enrich_token_balances(blocks, token_balances):
    result = list(
        join(
            token_balances,
            blocks,
            ('block_number', 'number'),
            ['type', *(f.name for f in fields(EthTokenBalance))],
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )

    return result


def enrich_traces(blocks, traces):
    result = list(
        join(
            traces,
            blocks,
            ('block_number', 'number'),
            [
                'type',
                'transaction_index',
                'from_address',
                'to_address',
                'value',
                'input',
                'output',
                'trace_type',
                'call_type',
                'reward_type',
                'gas',
                'gas_used',
                'subtraces',
                'trace_address',
                'error',
                'status',
                'transaction_hash',
                'block_number',
                'trace_id',
                'trace_index',
            ],
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )

    if len(result) != len(traces):
        raise ValueError('The number of traces is wrong ' + str(result))

    return result


def enrich_internal_transfers(transactions, internal_transfers):
    result = list(
        join(
            internal_transfers,
            transactions,
            ('transaction_hash', 'hash'),
            (
                'type',
                *(f.name for f in fields(InternalTransfer)),
            ),
            ('block_timestamp', 'block_number', 'block_hash'),
        )
    )

    for item in result:
        item['block_timestamp'] = datetime.utcfromtimestamp(item['block_timestamp'])

    if len(result) != len(internal_transfers):
        raise ValueError('The number of internal transfers is wrong ' + str(result))

    return result


def enrich_geth_traces(transactions, traces_for_transactions):
    result = list(
        join(
            traces_for_transactions,
            transactions,
            ('transaction_hash', 'hash'),
            ['transaction_hash', 'type', ('transaction_traces', 'traces_json')],
            [
                'block_number',
                'block_timestamp',
                'block_hash',
            ],
        )
    )
    if len(result) != len(traces_for_transactions):
        raise ValueError('Geth traces enriched wrongly' + str(result))
    for item in result:
        item['block_timestamp'] = datetime.utcfromtimestamp(item['block_timestamp'])
    return result


def enrich_contracts(blocks, contracts):
    result = list(
        join(
            contracts,
            blocks,
            ('block_number', 'number'),
            [
                'type',
                'address',
                'bytecode',
                'function_sighashes',
                'is_erc20',
                'is_erc721',
                'block_number',
            ],
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )

    if len(result) != len(contracts):
        raise ValueError('The number of contracts is wrong ' + str(result))

    return result


def enrich_tokens(blocks, tokens):
    result = list(
        join(
            tokens,
            blocks,
            ('block_number', 'number'),
            ['type', 'address', 'symbol', 'name', 'decimals', 'total_supply', 'block_number'],
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )

    if len(result) != len(tokens):
        raise ValueError('The number of tokens is wrong ' + str(result))

    return result


def enrich_errors(blocks, errors):
    result = list(
        join(
            errors,
            blocks,
            ('block_number', 'number'),
            EthErrorMapper.ERROR_ITEM_FIELDS,
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )
    return result


def enrich_native_balances(blocks, native_balances):
    return list(
        join(
            native_balances,
            blocks,
            ('block_number', 'number'),
            EthNativeBalanceItem.__required_keys__,
            [
                ('timestamp', 'block_timestamp'),
                ('hash', 'block_hash'),
            ],
        )
    )


def enrich_token_transfers_priced(blocks, token_transfers):
    return list(
        join(
            token_transfers,
            blocks,
            ('block_number', 'number'),
            ['type'] + [f.name for f in dataclasses.fields(TokenTransferPriced)],
            [
                ('timestamp', 'timestamp'),
            ],
        )
    )

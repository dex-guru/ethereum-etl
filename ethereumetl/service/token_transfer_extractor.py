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


import logging
from builtins import map
from typing import Iterator

import eth_abi.exceptions
import eth_abi
from eth_utils import keccak, to_bytes, to_hex

from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.utils import chunk_string, hex_to_dec, to_normalized_address

logger = logging.getLogger(__name__)

# fmt: off

# Standard | Signature                                                                   | Indexed Fields
# ---------+-----------------------------------------------------------------------------+----------------
# ERC-20   | Transfer(address indexed from, address indexed to, uint256 value)           | from, to
# ERC-721  | Transfer(address indexed from, address indexed to, uint256 indexed tokenId) | from, to, tokenId
# ERC-1155 | TransferSingle(address indexed operator, address indexed from,              | operator, from, to
#          | address indexed to, uint256 id, uint256 value)                              |
# ERC-1155 | TransferBatch(address indexed operator, address indexed from,               | operator, from, to
#          | address indexed to, uint256[] ids, uint256[] values)                        |

TRANSFER_EVENT_TOPIC = to_hex(keccak(text="Transfer(address,address,uint256)"))
# ERC721 uses the same event signature as ERC20 but with an extra indexed field
ERC721_TRANSFER_EVENT_TOPIC = TRANSFER_EVENT_TOPIC
ERC1155_SINGLE_TRANSFER_EVENT_TOPIC = to_hex(keccak(text="TransferSingle(address,address,address,uint256,uint256)"))
ERC1155_BATCH_TRANSFER_EVENT_TOPIC = to_hex(keccak(text="TransferBatch(address,address,address,uint256[],uint256[])"))

# fmt: on


def extract_erc20_transfers(receipt_log):
    # Handle unindexed event fields
    topics_with_data = receipt_log.topics + split_to_words(receipt_log.data)
    # if the number of topics and fields in data part != 4, then it's a weird event
    if len(topics_with_data) != 4:
        logger.warning(
            "The number of topics and data parts is not equal to 4 in log %s of transaction %s",
            receipt_log.log_index,
            receipt_log.transaction_hash,
        )
        return
    token_transfer = EthTokenTransfer(
        token_address=to_normalized_address(receipt_log.address),
        from_address=word_to_address(topics_with_data[1]),
        to_address=word_to_address(topics_with_data[2]),
        value=hex_to_dec(topics_with_data[3]),
        transaction_hash=receipt_log.transaction_hash,
        log_index=receipt_log.log_index,
        block_number=receipt_log.block_number,
    )
    yield token_transfer


def extract_erc721_transfers(receipt_log):
    topics = receipt_log.topics
    token_transfer = EthTokenTransfer(
        block_number=receipt_log.block_number,
        transaction_hash=receipt_log.transaction_hash,
        log_index=receipt_log.log_index,
        token_address=to_normalized_address(receipt_log.address),
        from_address=word_to_address(topics[1]),
        to_address=word_to_address(topics[2]),
        token_id=hex_to_dec(topics[3]),
        value=eth_abi.decode_single("uint256", to_bytes(hexstr=receipt_log.data)),
    )
    yield token_transfer


def extract_erc1155_single_transfers(receipt_log):
    topics = receipt_log.topics
    try:
        token_id, value = eth_abi.decode(
            ("uint256", "uint256"),
            to_bytes(hexstr=receipt_log.data),
        )
    except (eth_abi.exceptions.DecodingError, TypeError, ValueError):
        logger.warning(
            "Failed to decode ERC1155 single transfer event data in log %s of transaction %s",
            receipt_log.log_index,
            receipt_log.transaction_hash,
        )
    else:
        token_transfer = EthTokenTransfer(
            block_number=receipt_log.block_number,
            transaction_hash=receipt_log.transaction_hash,
            log_index=receipt_log.log_index,
            token_address=to_normalized_address(receipt_log.address),
            operator_address=word_to_address(topics[1]),
            from_address=word_to_address(topics[2]),
            to_address=word_to_address(topics[3]),
            token_id=token_id,
            value=value,
        )
        yield token_transfer


def extract_erc1155_batch_transfers(receipt_log):
    try:
        token_ids, values = eth_abi.decode(
            ("uint256[]", "uint256[]"),
            to_bytes(hexstr=receipt_log.data),
        )
    except (eth_abi.exceptions.DecodingError, TypeError, ValueError):
        logger.warning(
            "Failed to decode ERC1155 batch transfer event data in log %s of transaction %s",
            receipt_log.log_index,
            receipt_log.transaction_hash,
        )
    else:
        topics = receipt_log.topics
        for token_id, value in zip(token_ids, values):
            token_transfer = EthTokenTransfer(
                block_number=receipt_log.block_number,
                transaction_hash=receipt_log.transaction_hash,
                log_index=receipt_log.log_index,
                token_address=to_normalized_address(receipt_log.address),
                operator_address=word_to_address(topics[1]),
                from_address=word_to_address(topics[2]),
                to_address=word_to_address(topics[3]),
                token_id=token_id,
                value=value,
            )
            yield token_transfer


class EthTokenTransferExtractor(object):
    EXTRACT_BY_TOPICS_LEN_TOPIC0 = {
        (3, TRANSFER_EVENT_TOPIC): extract_erc20_transfers,
        (4, ERC721_TRANSFER_EVENT_TOPIC): extract_erc721_transfers,
        (4, ERC1155_SINGLE_TRANSFER_EVENT_TOPIC): extract_erc1155_single_transfers,
        (4, ERC1155_BATCH_TRANSFER_EVENT_TOPIC): extract_erc1155_batch_transfers,
    }

    def extract_transfers_from_log(self, receipt_log) -> Iterator[EthTokenTransfer]:
        topics = receipt_log.topics

        if topics is None or len(topics) < 1:
            # This is normal, topics can be empty for anonymous events
            return

        try:
            extract = self.EXTRACT_BY_TOPICS_LEN_TOPIC0[(len(topics), topics[0].casefold())]
        except KeyError:
            return
        else:
            yield from extract(receipt_log)


def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: "0x" + word, words))
        return words_with_0x
    return []


def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address("0x" + param[-40:])
    else:
        return to_normalized_address(param)

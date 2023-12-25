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
from web3 import Web3
from web3.middleware import geth_poa_middleware


def build_web3(provider):
    w3 = Web3(provider)
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    return w3


# def parse_event(contract: ABIContract, event_name: str, receipt_log: EthReceiptLog):
#     encoded_topics = [decode_hex(Web3.toHex(topic)) for topic in receipt_log.topics[1:]]
#     indexed_values = [
#         eth_abi.decode_single(t, v)
#         for t, v in zip(contract.topic_indexed_types[event_name], encoded_topics)
#     ]
#     values = eth_abi.decode_abi(contract.topic_types[event_name], decode_hex(receipt_log.data))
#     return {
#         **{
#             **dict(zip(contract.topic_names[event_name], values)),
#             **dict(zip(contract.topic_indexed_names[event_name], indexed_values)),
#         }
#     }

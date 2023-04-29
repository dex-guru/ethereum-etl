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


from ethereumetl.domain.receipt_log import EthReceiptLog
from ethereumetl.service.token_transfer_extractor import EthTokenTransferExtractor

token_transfer_extractor = EthTokenTransferExtractor()


def test_extract_transfer_from_receipt_log():
    log = EthReceiptLog()
    log.address = '0x25c6413359059694A7FCa8e599Ae39Ce1C944Da2'
    log.block_number = 1061946
    log.data = '0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc18'
    log.log_index = 0
    log.topics = [
        '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
        '0x000000000000000000000000e9eeaec75883f0e389a78e2260bfac1776df2f1d',
        '0x0000000000000000000000000000000000000000000000000000000000000000',
    ]
    log.transaction_hash = '0xd62a74c7b04e8e0539398f6ba6a5eb11ad8aa862e77f0af718f0fad19b0b0480'

    [token_transfer] = token_transfer_extractor.extract_transfers_from_log(log)

    assert token_transfer.token_address == '0x25c6413359059694a7fca8e599ae39ce1c944da2'
    assert token_transfer.from_address == '0xe9eeaec75883f0e389a78e2260bfac1776df2f1d'
    assert token_transfer.to_address == '0x0000000000000000000000000000000000000000'
    assert (
        token_transfer.value
        == 115792089237316195423570985008687907853269984665640564039457584007913129638936
    )
    assert (
        token_transfer.transaction_hash
        == '0xd62a74c7b04e8e0539398f6ba6a5eb11ad8aa862e77f0af718f0fad19b0b0480'
    )
    assert token_transfer.block_number == 1061946


def test_extract_transfer_from_receipt_log_with_nft():
    log = EthReceiptLog()
    log.log_index = 68
    log.transaction_hash = "0x54c60539f2dcca7b61924440413bb29139cbaa2a2931f539e1a31edbb6d7cc2e"
    log.transaction_index = 54
    log.address = "0x03bf9f1f807967002c4f9feed1dd4ea542275947"
    log.data = (
        "0x"
        "0000000000000000000000000000000000000000000000000000000000000003"
        "0000000000000000000000000000000000000000000000000000000000000001"
    )
    log.topics = [
        "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
        "0x000000000000000000000000fa1b15df09c2944a91a2f9f10a6133090d4119bd",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x000000000000000000000000fb3a85aff7cab85e7e2b7461703f57ed0105645e",
    ]
    log.block_number = 16896735

    [token_transfer] = token_transfer_extractor.extract_transfers_from_log(log)

    assert token_transfer.block_number == 16896735
    assert token_transfer.from_address == "0x0000000000000000000000000000000000000000"
    assert token_transfer.log_index == 68
    assert token_transfer.operator_address == "0xfa1b15df09c2944a91a2f9f10a6133090d4119bd"
    assert token_transfer.to_address == "0xfb3a85aff7cab85e7e2b7461703f57ed0105645e"
    assert token_transfer.token_address == "0x03bf9f1f807967002c4f9feed1dd4ea542275947"
    assert token_transfer.token_id == 3
    assert (
        token_transfer.transaction_hash
        == "0x54c60539f2dcca7b61924440413bb29139cbaa2a2931f539e1a31edbb6d7cc2e"
    )
    assert token_transfer.value == 1

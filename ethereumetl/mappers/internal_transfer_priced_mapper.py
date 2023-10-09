from dataclasses import asdict
from datetime import datetime

from ethereumetl.domain.token_transfer_priced import Direction, TokenTransferPriced
from ethereumetl.enumeration.entity_type import EntityType


class InternalTransferPricedMapper:
    @staticmethod
    def internal_transfer_to_transfer_priced(
        token_address,
        token_transfer: dict,
        price: float,
        decimals: int,
        symbol: str,
        chain_id: int,
    ):
        amount = token_transfer['value'] / 10**decimals
        timestamp = (
            int(token_transfer['block_timestamp'].timestamp())
            if isinstance(token_transfer['block_timestamp'], datetime)
            else token_transfer['block_timestamp']
        )
        transfer = TokenTransferPriced(
            token_addresses=[token_address],
            wallets=[token_transfer['from_address'], token_transfer['to_address']],
            direction=Direction(
                from_address=token_transfer['from_address'],
                to_address=token_transfer['to_address'],
            ),
            transaction_address=token_transfer['transaction_hash'],
            block_number=token_transfer['block_number'],
            timestamp=timestamp,
            id=f'{token_transfer["transaction_hash"]}-{token_transfer["id"]}',
            transfer_type='native',
            transaction_type='transfer',
            prices_stable=[price],
            amounts=[amount],
            amount_stable=amount * price,
            symbols=[symbol],
            chain_id=chain_id,
        )
        return transfer

    @staticmethod
    def internal_transfer_priced_to_dict(transfer_priced: TokenTransferPriced):
        priced_transfer_as_dict = asdict(transfer_priced)
        priced_transfer_as_dict['type'] = EntityType.INTERNAL_TRANSFER_PRICED.value
        return priced_transfer_as_dict

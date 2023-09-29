from dataclasses import asdict

from ethereumetl.domain.token_transfer import TokenStandard
from ethereumetl.domain.token_transfer_priced import Direction, TokenTransferPriced
from ethereumetl.enumeration.entity_type import EntityType


class TokenTransferPricedMapper:
    @staticmethod
    def token_transfer_to_transfer_priced(
        token_transfer: dict, price: float, decimals: int, symbol: str, chain_id: int
    ):
        if token_transfer['token_standard'] == TokenStandard.ERC20:
            amount = token_transfer['value'] / 10**decimals
        else:
            amount = 1
        transfer = TokenTransferPriced(
            token_addresses=[token_transfer['token_address']],
            wallets=[token_transfer['from_address'], token_transfer['to_address']],
            direction=Direction(
                from_address=token_transfer['from_address'],
                to_address=token_transfer['to_address'],
            ),
            transaction_address=token_transfer['transaction_hash'],
            block_number=token_transfer['block_number'],
            id=f'{token_transfer["transaction_hash"]}-{token_transfer["log_index"]}',
            transfer_type=token_transfer['token_standard'].replace('-', '').lower(),
            transaction_type='transfer',
            prices_stable=[price],
            amounts=[amount],
            amount_stable=amount * price,
            symbols=[symbol],
            chain_id=chain_id,
        )
        return transfer

    @staticmethod
    def transfer_priced_to_dict(transfer_priced: TokenTransferPriced):
        priced_transfer_as_dict = asdict(transfer_priced)
        priced_transfer_as_dict['type'] = EntityType.TOKEN_TRANSFER_PRICED.value
        return priced_transfer_as_dict

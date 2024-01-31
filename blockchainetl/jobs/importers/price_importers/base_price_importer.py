from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface
from ethereumetl.misc.info import get_chain_config


class BasePriceImporter(PriceImporterInterface):
    def __init__(self, chain_id: int):
        self.chain_id = chain_id
        self.stablecoin_addresses = ()
        self.native_token_address = ''

    def open(self):
        chain_config = get_chain_config(self.chain_id)
        self.stablecoin_addresses = chain_config['stablecoin_addresses']
        self.native_token_address = chain_config['native_token']['address']

    def close(self): ...

    def get_stable_price_for_token(
        self, token_address: str, timestamp: int | None = None, block_number: int | None = None
    ) -> float:
        if token_address in self.stablecoin_addresses:
            return 1.0
        return 0

    def get_native_price_for_token(
        self,
        token_address: str,
        timestamp: int | None = None,
        block_number: int | None = None,
    ) -> float:
        if token_address == self.native_token_address:
            return 1.0
        return 0

    def get_token_score(self, token_address: str) -> int:
        if token_address in self.stablecoin_addresses:
            return 2
        if token_address == self.native_token_address:
            return 1
        return 0

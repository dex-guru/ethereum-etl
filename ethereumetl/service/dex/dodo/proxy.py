import logging
from functools import lru_cache

from web3 import Web3

from ethereumetl.domain.dex_pool import EthDexPool
from ethereumetl.domain.dex_trade import EthDexTrade
from ethereumetl.domain.receipt_log import ParsedReceiptLog
from ethereumetl.domain.token import EthToken
from ethereumetl.domain.token_transfer import EthTokenTransfer
from ethereumetl.service.dex.base.interface import DexClientInterface
from ethereumetl.service.dex.dodo.contracts_v1 import DODOv1Amm
from ethereumetl.service.dex.dodo.contracts_v2 import DODOv2Amm

logs = logging.getLogger(__name__)

AMM_TYPE = "dodo"


class DODOAmm(DexClientInterface):
    """
    DODO proxy AMM client
    This class proxying parsing to 2 versions of contracts (v1 and v2).
    Proxying realize by [strategy pattern](https://refactoring.guru/design-patterns/strategy).

    In BackendAmmInventory needed only these contract addresses:
        - DODO
        - GnosisSafeProxy
        - DVM


    # BSC examples:

    Tx with swaps on both contracts versions (v1 and v2):
        0x41d3f26a42a6fe734db47d08c02af8faba22576825568f00b94690768dbca217 (events 115 and 118).
    """

    _amm_clients: list

    def __init__(self, web3: Web3, chain_id: int, path_to_file: str = __file__):
        super().__init__(web3, chain_id)
        self._init_amm_clients(web3, chain_id, path_to_file)

    def _init_amm_clients(self, web3: Web3, chain_id: int, path_to_file: str):
        self._amm_clients = [
            DODOv1Amm(web3, chain_id, path_to_file),
            DODOv2Amm(web3, chain_id, path_to_file),
        ]

    @lru_cache(maxsize=128)
    def _choose_amm_client(self, pool_address: str) -> DODOv1Amm | DODOv2Amm | None:
        logs.debug(f"Choosing amm client for pool {pool_address}")
        for amm_client in self._amm_clients:
            if amm_client.is_pool_address_for_amm(pool_address):
                logs.debug(f"Found amm client {amm_client}")
                return amm_client

        return None

    def resolve_asset_from_log(self, parsed_log: ParsedReceiptLog) -> EthDexPool | None:
        client = self._choose_amm_client(parsed_log.address)
        if not client:
            return None
        return client.resolve_asset_from_log(parsed_log)

    def resolve_receipt_log(
        self,
        parsed_receipt_log: ParsedReceiptLog,
        dex_pool: EthDexPool,
        tokens_for_pool: list[EthToken],
        transfers_for_transaction: list[EthTokenTransfer],
    ) -> EthDexTrade | None:
        client = self._choose_amm_client(dex_pool.address)
        if not client:
            return None
        return client.resolve_receipt_log(
            parsed_receipt_log, dex_pool, tokens_for_pool, transfers_for_transaction
        )

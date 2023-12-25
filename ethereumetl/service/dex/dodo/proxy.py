from collections.abc import Iterator
from functools import lru_cache

from clients.blockchain import BaseBlockchainClient
from clients.blockchain.amm.base.base_contract import BaseContract
from clients.blockchain.amm.dodo.contracts_v1 import DODOv1Amm
from clients.blockchain.amm.dodo.contracts_v2 import DODOv2Amm
from clients.blockchain.interfaces import AmmClientI
from clients.blockchain.models.pool import BasePool
from clients.blockchain.models.tokens import ERC20Token
from clients.blockchain.models.transaction import ReceiptLog
from clients.blockchain.models.transfer import TransferBase
from eth_typing import ChecksumAddress
from utils.logger import get_logger

logs = get_logger(__name__)

AMM_TYPE = "dodo"


class DODOAmm(BaseContract, BaseBlockchainClient, AmmClientI):
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

    _amm_clients: list[AmmClientI]

    def __init__(self, uri: str, amm_type: str, contracts: dict):
        self._init_amm_clients(uri=uri, amm_type=amm_type, contracts=contracts)
        super().__init__(uri=uri, amm_type=amm_type, contracts=contracts)

    def _init_amm_clients(self, uri: str, amm_type: str, contracts: dict):
        self._amm_clients = [
            DODOv1Amm(uri=uri, amm_type=amm_type, contracts=contracts),
            DODOv2Amm(uri=uri, amm_type=amm_type, contracts=contracts),
        ]

    @property
    def pool_contract_names(self) -> Iterator[str]:
        for client in self._amm_clients:
            yield from client.pool_contract_names

    @property
    def pool_contracts_events_names(self) -> Iterator[str]:
        for client in self._amm_clients:
            yield from client.pool_contracts_events_names()

    @lru_cache(maxsize=50)
    def _choose_amm_client(self, pool_address: str) -> AmmClientI:
        logs.debug(f"Choosing amm client for pool {pool_address}")
        for amm_client in self._amm_clients:
            if amm_client.is_pool_address_for_amm(pool_address):
                logs.debug(f"Found amm client {amm_client}")
                return amm_client

        raise ValueError(f"No amm client for pool {pool_address}")

    def get_tokens_addresses_for_pool(self, pool_address: ChecksumAddress) -> list | None:
        client = self._choose_amm_client(pool_address)
        return client.get_tokens_addresses_for_pool(pool_address)

    def get_base_pool(self, address: ChecksumAddress) -> BasePool | None:
        client = self._choose_amm_client(address)
        return client.get_base_pool(address)

    def resolve_receipt_log(
        self,
        receipt_log: ReceiptLog,
        base_pool: BasePool,
        erc20_tokens: list[ERC20Token],
        transfers: list[TransferBase],
    ) -> dict | None:
        client = self._choose_amm_client(base_pool.address)
        return client.resolve_receipt_log(receipt_log, base_pool, erc20_tokens, transfers)

    def get_lp_token_address_for_pool(self, pool_address: str) -> list[str]:
        client = self._choose_amm_client(pool_address)
        return client.get_lp_token_address_for_pool(pool_address)

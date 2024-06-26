from enum import Enum, unique


@unique
class EntityType(str, Enum):
    BLOCK = 'block'
    TRANSACTION = 'transaction'
    RECEIPT = 'receipt'
    LOG = 'log'
    TOKEN_TRANSFER = 'token_transfer'
    TRACE = 'trace'
    GETH_TRACE = 'geth_trace'
    CONTRACT = 'contract'
    TOKEN = 'token'
    INTERNAL_TRANSFER = 'internal_transfer'
    TOKEN_BALANCE = 'token_balance'
    ERROR = 'error'
    NATIVE_BALANCE = 'native_balance'
    TOKEN_TRANSFER_PRICED = 'token_transfer_priced'
    INTERNAL_TRANSFER_PRICED = 'internal_transfer_priced'
    PRE_EVENT = 'pre_event'
    DEX_POOL = 'dex_pool'
    DEX_TRADE = 'dex_trade'
    PARSED_LOG = 'parsed_log'
    ENRICHED_DEX_TRADE = 'enriched_dex_trade'
    ENRICHED_TRANSFER = 'enriched_transfer'

    def __str__(self):
        return self.value


ALL_FOR_STREAMING = (
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.TOKEN_TRANSFER,
    EntityType.TOKEN,
    EntityType.TOKEN_BALANCE,
    EntityType.ERROR,
    EntityType.GETH_TRACE,
    EntityType.INTERNAL_TRANSFER,
    EntityType.NATIVE_BALANCE,
    EntityType.CONTRACT,
    EntityType.TOKEN_TRANSFER_PRICED,
    EntityType.INTERNAL_TRANSFER_PRICED,
    EntityType.PRE_EVENT,
    EntityType.DEX_POOL,
    EntityType.DEX_TRADE,
    EntityType.ENRICHED_DEX_TRADE,
    EntityType.ENRICHED_TRANSFER,
)
ALL_FOR_INFURA = (
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.TOKEN_TRANSFER,
    EntityType.TOKEN_BALANCE,
)
ALL_STATIC = (
    EntityType.DEX_POOL,
    EntityType.TOKEN,
    EntityType.CONTRACT,
)
ALL = tuple(EntityType)

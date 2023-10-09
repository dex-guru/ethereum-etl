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

    def __str__(self):
        return self.value


ALL_FOR_STREAMING = (
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.TOKEN_TRANSFER,
    EntityType.TOKEN,
    EntityType.TOKEN_BALANCE,
    EntityType.TRACE,
    EntityType.ERROR,
    EntityType.GETH_TRACE,
    EntityType.INTERNAL_TRANSFER,
    EntityType.NATIVE_BALANCE,
    EntityType.CONTRACT,
    EntityType.TOKEN_TRANSFER_PRICED,
    EntityType.INTERNAL_TRANSFER_PRICED,
)
ALL_FOR_INFURA = (
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.TOKEN_TRANSFER,
    EntityType.TOKEN_BALANCE,
)
ALL = tuple(EntityType)

from enum import Enum


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
)
ALL_FOR_INFURA = (
    EntityType.BLOCK,
    EntityType.TRANSACTION,
    EntityType.LOG,
    EntityType.TOKEN_TRANSFER,
    EntityType.TOKEN_BALANCE,
)
ALL = tuple(EntityType)

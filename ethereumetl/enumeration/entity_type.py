class EntityType:
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

    ALL_FOR_STREAMING = [
        BLOCK,
        TRANSACTION,
        LOG,
        TOKEN_TRANSFER,
        TOKEN,
        TOKEN_BALANCE,
        TRACE,
        ERROR,
        GETH_TRACE,
        INTERNAL_TRANSFER,
    ]
    ALL_FOR_INFURA = [BLOCK, TRANSACTION, LOG, TOKEN_TRANSFER, TOKEN_BALANCE]
    ALL = [
        BLOCK,
        TRANSACTION,
        RECEIPT,
        LOG,
        TOKEN_TRANSFER,
        TRACE,
        CONTRACT,
        TOKEN,
        TOKEN_BALANCE,
        ERROR,
    ]

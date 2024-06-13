from enum import Enum


class DexPoolFeeAmount(int, Enum):
    UNDEFINED = 0
    VERY_LOW = 100
    LOW = 500
    MEDIUM = 3000
    HIGH = 10000


class TransactionType(Enum):
    swap = "swap"
    burn = "burn"
    mint = "mint"
    transfer = "transfer"
    sync = "sync"

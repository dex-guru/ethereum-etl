from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class OriginMarketplaceListing:
    listing_id: Any
    ipfs_hash: Any
    listing_type: Any
    category: Any
    subcategory: Any
    language: Any
    title: Any
    description: Any
    price: Any
    currency: Any
    block_number: Any
    log_index: Any


@dataclass(slots=True)
class OriginShopProduct:
    listing_id: Any
    product_id: Any
    ipfs_path: Any
    external_id: Any
    parent_external_id: Any
    title: Any
    description: Any
    price: Any
    currency: Any
    image: Any
    option1: Any
    option2: Any
    option3: Any
    block_number: Any
    log_index: Any

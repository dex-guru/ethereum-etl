from dataclasses import asdict

from ethereumetl.domain.origin import OriginMarketplaceListing, OriginShopProduct


class OriginMarketplaceListingMapper:
    @staticmethod
    def listing_to_dict(listing: OriginMarketplaceListing):
        result = asdict(listing)
        result['type'] = 'origin_marketplace_listing'
        return result


class OriginShopProductMapper:
    @staticmethod
    def product_to_dict(product: OriginShopProduct):
        result = asdict(product)
        result['type'] = 'origin_shop_product'
        return result

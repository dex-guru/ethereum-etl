import logging
import re

from ethereumetl.domain.origin import OriginMarketplaceListing, OriginShopProduct
from ethereumetl.ipfs.client import IpfsClient

logger = logging.getLogger('origin')

IPFS_PRIMARY_GATEWAY_URL = 'https://cf-ipfs.com/ipfs'
IPFS_SECONDARY_GATEWAY_URL = 'https://gateway.ipfs.io/ipfs'


# Returns an IPFS client that can be used to fetch Origin Protocol's data.
def get_origin_ipfs_client():
    return IpfsClient([IPFS_PRIMARY_GATEWAY_URL, IPFS_SECONDARY_GATEWAY_URL])


# Parses the shop's HTML index page to extract the name of the IPFS directory under
# which all the shops data is located.
def _get_shop_data_dir(shop_index_page):
    match = re.search('<link rel="data-dir" href="(.+?)"', shop_index_page)
    return match.group(1) if match else None


# Returns the list of products from an Origin Protocol shop.
def _get_origin_shop_products(receipt_log, listing_id, ipfs_client, shop_ipfs_hash):
    results: list[OriginShopProduct] = []
    shop_index_page = ipfs_client.get(shop_ipfs_hash + "/index.html")
    shop_data_dir = _get_shop_data_dir(shop_index_page)

    path = f"{shop_ipfs_hash}/{shop_data_dir}" if shop_data_dir else shop_ipfs_hash
    logger.debug(f"Using shop path {path}")

    products_path = "{}/{}".format(path, 'products.json')
    try:
        products = ipfs_client.get_json(products_path)
    except Exception as e:
        logger.error(f"Listing {listing_id} Failed downloading product {products_path}: {e}")
        return results

    logger.info(f"Found {len(products)} products in for listing {listing_id}")

    # Go through all the products from the shop.
    for product in products:
        product_id = product.get('id')
        if not product_id:
            logger.error('Product entry with missing id in products.json')
            continue

        logger.info(f"Processing product {product_id}")

        # Fetch the product details to get the variants.
        product_base_path = f"{path}/{product_id}"
        product_data_path = "{}/{}".format(product_base_path, 'data.json')
        try:
            product = ipfs_client.get_json(product_data_path)
        except Exception as e:
            logger.error(f"Failed downloading {product_data_path}: {e}")
            continue

        # Extract the top product.
        result = OriginShopProduct(
            block_number=receipt_log.block_number,
            log_index=receipt_log.log_index,
            listing_id=listing_id,
            product_id=f"{listing_id}-{product_id}",
            ipfs_path=product_base_path,
            external_id=str(product.get('externalId')) if product.get('externalId') else None,
            parent_external_id=None,
            title=product.get('title'),
            description=product.get('description'),
            price=product.get('price'),
            currency=product.get('currency', 'fiat-USD'),
            option1=None,
            option2=None,
            option3=None,
            image=product.get('image'),
        )
        results.append(result)

        # Extract the variants, if any.
        variants = product.get('variants', [])
        if len(variants) > 0:
            logger.info(f"Found {len(variants)} variants")
            for variant in variants:
                result = OriginShopProduct(
                    block_number=receipt_log.block_number,
                    log_index=receipt_log.log_index,
                    listing_id=listing_id,
                    product_id="{}-{}".format(listing_id, variant.get('id')),
                    ipfs_path=product_base_path,
                    external_id=(
                        str(variant.get('externalId')) if variant.get('externalId') else None
                    ),
                    parent_external_id=(
                        str(product.get('externalId')) if product.get('externalId') else None
                    ),
                    title=variant.get('title'),
                    description=product.get('description'),
                    price=variant.get('price'),
                    currency=product.get('currency', 'fiat-USD'),
                    option1=variant.get('option1'),
                    option2=variant.get('option2'),
                    option3=variant.get('option3'),
                    image=variant.get('image'),
                )
                results.append(result)

    return results


# Returns a listing from the Origin Protocol marketplace.
def get_origin_marketplace_data(receipt_log, listing_id, ipfs_client, ipfs_hash):
    # Load the listing's metadata from IPFS.
    try:
        listing_data = ipfs_client.get_json(ipfs_hash)
    except Exception as e:
        logger.error(f"Extraction failed. Listing {listing_id} Listing hash {ipfs_hash} - {e}")
        return None, []

    # Fill-in an OriginMarketplaceListing object based on the IPFS data.
    listing = OriginMarketplaceListing(
        block_number=receipt_log.block_number,
        log_index=receipt_log.log_index,
        listing_id=str(listing_id),
        ipfs_hash=ipfs_hash,
        listing_type=listing_data.get('listingType', ''),
        category=listing_data.get('category', ''),
        subcategory=listing_data.get('subCategory', ''),
        language=listing_data.get('language', ''),
        title=listing_data.get('title', ''),
        description=listing_data.get('description', ''),
        price=listing_data.get('price', {}).get('amount', ''),
        currency=listing_data.get('price', {}).get('currency', ''),
    )

    # If it is a shop listing, also extract all the shop data.
    shop_listings = []
    shop_ipfs_hash = listing_data.get('shopIpfsHash')
    if shop_ipfs_hash:
        try:
            shop_listings = _get_origin_shop_products(
                receipt_log, listing_id, ipfs_client, shop_ipfs_hash
            )
        except Exception as e:
            logger.error(
                f"Extraction failed. Listing {listing_id} Shop hash {shop_ipfs_hash} - {e}"
            )

    return listing, shop_listings

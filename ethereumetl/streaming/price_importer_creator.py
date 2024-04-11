from blockchainetl.jobs.importers.price_importers.base_price_importer import BasePriceImporter
from blockchainetl.jobs.importers.price_importers.clickhouse_price_importer import (
    ClickhousePriceImporter,
)
from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface
from blockchainetl.jobs.importers.price_importers.multi_price_importer import MultiPriceImporter


def create_price_importer(chain_id: int, source: str) -> PriceImporterInterface:
    item_importer = determine_price_importer_type(source, chain_id)
    return item_importer


def create_price_importers(chain_id: int, sources: str) -> PriceImporterInterface:
    sources_list = sources.split(',')
    item_importers = [determine_price_importer_type(source, chain_id) for source in sources_list]
    return MultiPriceImporter(chain_id, item_importers)


def determine_price_importer_type(src: str, chain_id: int) -> PriceImporterInterface:
    if not src:
        return BasePriceImporter(chain_id)
    if src.startswith('clickhouse'):
        return ClickhousePriceImporter(chain_id, src)
    raise ValueError(f'Unknown input type {src}.')

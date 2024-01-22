from blockchainetl.jobs.importers.price_importers.base_price_importer import BasePriceImporter
from blockchainetl.jobs.importers.price_importers.interface import PriceImporterInterface
from blockchainetl.jobs.importers.price_importers.multi_price_importer import MultiPriceImporter


def create_price_importer(chain_id: int, input_: str) -> PriceImporterInterface:
    item_importer_type = determine_item_importer_type(input_)
    return item_importer_type(chain_id)


def create_price_importers(chain_id: int, input_: str) -> PriceImporterInterface:
    inputs = input_.split(',')
    item_importers = [determine_item_importer_type(input_)(chain_id) for input_ in inputs]
    return MultiPriceImporter(chain_id, item_importers)


def determine_item_importer_type(input_: str) -> type[PriceImporterInterface]:
    if not input_:
        return BasePriceImporter
    raise ValueError(f'Unknown input type {input_}.')

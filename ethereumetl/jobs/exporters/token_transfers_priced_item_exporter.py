import dataclasses

from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter
from ethereumetl.domain.token_transfer_priced import TokenTransferPriced


def token_transfers_priced_item_exporter(token_transfer_output, converters=()):
    return CompositeItemExporter(
        filename_mapping={'token_transfer_priced': token_transfer_output},
        field_mapping={
            'token_transfer_priced': [f.name for f in dataclasses.fields(TokenTransferPriced)]
        },
        converters=converters,
    )

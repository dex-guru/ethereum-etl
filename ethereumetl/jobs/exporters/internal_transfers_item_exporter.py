import dataclasses

from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter
from ethereumetl.domain.internal_transfer import InternalTransfer


def internal_transfers_item_exporter(internal_transfers_output):
    return CompositeItemExporter(
        filename_mapping={'internal_transfer': internal_transfers_output},
        field_mapping={'internal_transfer': [field.name for field in dataclasses.fields(InternalTransfer)]},
    )

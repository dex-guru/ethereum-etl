import json
import os
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, NamedTuple, cast

import click
import clickhouse_connect
from eth_utils import event_abi_to_log_topic, to_hex

# noinspection PyProtectedMember
from eth_utils.abi import _abi_to_signature

from ethereumetl.utils import parse_clickhouse_url

ABI = Sequence[Mapping[str, Any]]
EventABI = dict[str, Any]
FilePath = str


class EventInventoryRecord(NamedTuple):
    event_signature_hash: str
    event_signature: str
    event_topic_count: int
    event_name: str
    namespace: str
    contract_name: str
    event_abi_json: str


def read_event_abis(dir_path) -> Iterable[tuple[FilePath, ABI]]:
    abi_dir = Path(dir_path)
    assert abi_dir.is_dir()

    for file_path in abi_dir.rglob('*.json'):
        with file_path.open() as f:
            data = json.load(f)

        assert isinstance(data, list)

        rel_path = file_path.relative_to(abi_dir)
        yield str(rel_path), data


def get_event_abis_with_file_paths(
    ABIs: Iterable[tuple[FilePath, ABI]]
) -> Iterable[tuple[str, EventABI]]:
    for file_path, abi_objects in ABIs:
        for abi_object in abi_objects:
            match abi_object:
                case {
                    "anonymous": False,
                    "type": "event",
                    "name": str(),
                    "inputs": list(),
                }:
                    yield file_path, cast(dict, abi_object)


def event_abi_to_event_inventory_record(
    file_path: str, event_abi: EventABI
) -> EventInventoryRecord:
    parent_path, file_name = Path(file_path).parent, Path(file_path).name

    return EventInventoryRecord(
        event_signature_hash=to_hex(event_abi_to_log_topic(event_abi)).casefold(),
        event_signature=_abi_to_signature(event_abi),
        event_topic_count=sum(1 for i in event_abi['inputs'] if i['indexed']) + 1,
        event_name=event_abi['name'],
        namespace=str(parent_path),
        contract_name=file_name[:-5],
        event_abi_json=json.dumps(event_abi, separators=(',', ':')),
    )


def load_abis_to_event_inventory(
    clickhouse_url: str,
    ABIs_dir_path: str,
    dry_run: bool = True,
):
    event_abis = get_event_abis_with_file_paths(read_event_abis(ABIs_dir_path))
    event_registry_records = [
        event_abi_to_event_inventory_record(file_path, event_abi)
        for file_path, event_abi in event_abis
    ]
    with clickhouse_connect.create_client(**parse_clickhouse_url(clickhouse_url)) as client:
        if not dry_run:
            client.insert(
                'event_inventory_src',
                event_registry_records,
                column_names=EventInventoryRecord._fields,
            )
    # for e in event_registry_records:
    #     print(f"{e.namespace}:{e.contract_name} : {e.event_signature}")


@click.command()
@click.option(
    '-u', '--clickhouse-url', required=True, type=str, default=os.getenv('CLICKHOUSE_URL')
)
@click.option(
    '-a',
    '--abi-dir',
    required=True,
    type=str,
    default=Path(__file__).parent.parent / 'service' / 'dex',
)
@click.option('-n', '--dry-run', is_flag=True)
def cli(clickhouse_url, abi_dir, dry_run):
    load_abis_to_event_inventory(clickhouse_url, abi_dir, dry_run)


# cli.callback(
#     clickhouse_url='http://username:password@localhost:8123/eth',
#     abi_dir=Path(__file__).parent.parent / 'service' / 'dex',
#     dry_run=False,
# )
#
if __name__ == '__main__':
    cli()

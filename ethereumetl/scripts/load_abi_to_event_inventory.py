import json
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
    abi_type: str
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
    file_path: FilePath, event_abi: EventABI
) -> EventInventoryRecord:
    return EventInventoryRecord(
        event_signature_hash=to_hex(event_abi_to_log_topic(event_abi)).casefold(),
        event_signature=_abi_to_signature(event_abi),
        event_topic_count=sum(1 for i in event_abi['inputs'] if i['indexed']) + 1,
        event_name=event_abi['name'],
        abi_type=str(file_path),
        event_abi_json=json.dumps(event_abi, separators=(',', ':')),
    )


def load_abis_to_event_inventory(chain_id, clickhouse_url, ABIs_dir_path, dry_run=True):
    event_abis = get_event_abis_with_file_paths(read_event_abis(ABIs_dir_path))
    event_registry_records = [
        event_abi_to_event_inventory_record(file_path, event_abi)
        for file_path, event_abi in event_abis
    ]
    with clickhouse_connect.create_client(**parse_clickhouse_url(clickhouse_url)) as client:
        if not dry_run:
            client.insert(
                f'{chain_id}_event_inventory_src',
                event_registry_records,
                column_names=EventInventoryRecord._fields,
            )
    for e in event_registry_records:
        print(f"{e.abi_type}: {e.event_signature}")


@click.command()
@click.option('-c', '--chain-id', required=True, type=int)
@click.option('-u', '--clickhouse-url', required=True, type=str)
@click.option('-a', '--abi-dir', required=True, type=str)
@click.option('-n', '--dry-run', is_flag=True)
def cli(chain_id, clickhouse_url, abi_dir, dry_run):
    load_abis_to_event_inventory(chain_id, clickhouse_url, abi_dir, dry_run)


if __name__ == '__main__':
    cli()

# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import contextlib
import logging
import os
import time
from abc import ABC, abstractmethod
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from clickhouse_sqlalchemy.types import UInt32

from blockchainetl.file_utils import smart_open
from blockchainetl.streaming.streamer_adapter_stub import StreamerAdapterStub
from ethereumetl.utils import timestamp_now


class Streamer:
    def __init__(
        self,
        chain_id,
        blockchain_streamer_adapter: StreamerAdapterStub,
        last_synced_block_provider_uri='file://last_synced_block.txt',
        lag=0,
        start_block=None,
        end_block=None,
        period_seconds=10,
        block_batch_size=10,
        retry_errors=True,
        pid_file=None,
        verifier_enabled=False,
        skip_cycle=True,
    ):
        self.chain_id = chain_id
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_provider = LastSyncedBlockProvider.from_uri(
            last_synced_block_provider_uri, chain_id
        )
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block
        self.period_seconds = period_seconds
        self.block_batch_size = block_batch_size
        self.retry_errors = retry_errors
        self.pid_file = pid_file
        self.verifier_enabled = verifier_enabled
        self.skip_cycle = skip_cycle

        if self.start_block == 'latest':
            self.start_block = self.blockchain_streamer_adapter.get_current_block_number()
        if self.start_block is not None:
            init_last_synced_block_provider(
                (self.start_block or 0) - 1, self.last_synced_block_provider
            )
        self.last_synced_block = self.last_synced_block_provider.get_last_synced_block()

    def stream(self):
        try:
            if self.pid_file is not None:
                logging.info(f'Creating pid file {self.pid_file}')
                write_to_file(self.pid_file, str(os.getpid()))
            self.blockchain_streamer_adapter.open()
            self._do_stream()
        finally:
            self.blockchain_streamer_adapter.close()
            if self.pid_file is not None:
                logging.info(f'Deleting pid file {self.pid_file}')
                delete_file(self.pid_file)

    def _do_stream(self):
        while True and (self.end_block is None or self.last_synced_block < self.end_block):
            synced_blocks = 0

            try:
                synced_blocks = self._sync_cycle()
            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logging.exception('An exception occurred while syncing block data: %s', e)
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                logging.info(f'Nothing to sync. Sleeping for {self.period_seconds} seconds...')
                time.sleep(self.period_seconds)

    def _sync_cycle(self):
        current_block = self.blockchain_streamer_adapter.get_current_block_number()

        target_block = self._calculate_target_block(current_block, self.last_synced_block)
        blocks_to_sync = max(target_block - self.last_synced_block, 0)

        logging.info(
            'Current block %s, target block %s, last synced block %s, blocks to sync %s',
            current_block,
            target_block,
            self.last_synced_block,
            blocks_to_sync,
            extra={
                'current_block': current_block,
                'target_block': target_block,
                'last_synced_block': self.last_synced_block,
                'blocks_to_sync': blocks_to_sync,
            },
        )
        if self.skip_cycle and self._need_to_skip_cycle(current_block):
            return 1

        if blocks_to_sync != 0:
            self.blockchain_streamer_adapter.export_all(self.last_synced_block + 1, target_block)
            logging.info(f'Writing last synced block {target_block}')
            self.last_synced_block_provider.set_last_synced_block(target_block)
            self.last_synced_block = target_block

        return blocks_to_sync

    def _need_to_skip_cycle(self, current_block: int) -> bool:
        """
        Checks if the difference between current block and last synced block is too big.
        If so, skips the sync.
        """
        diff_blocks = current_block - self.last_synced_block - self.lag
        if not self.verifier_enabled and diff_blocks > self.block_batch_size * 1.5:
            logging.warning(
                f'The difference between current block and last synced block is too big: {diff_blocks}. Skipping sync.'
            )
            self.last_synced_block_provider.set_last_synced_block(current_block)
            self.last_synced_block = current_block
            return True
        return False

    def _calculate_target_block(self, current_block, last_synced_block):
        target_block = current_block - self.lag
        target_block = min(target_block, last_synced_block + self.block_batch_size)
        target_block = (
            min(target_block, self.end_block) if self.end_block is not None else target_block
        )
        return target_block


def delete_file(file):
    with contextlib.suppress(OSError):
        os.remove(file)


def write_last_synced_block(file, last_synced_block):
    write_to_file(file, str(last_synced_block) + '\n')


def init_last_synced_block_provider(start_block, provider):
    last_synced_block = provider.get_last_synced_block()
    if last_synced_block and start_block > last_synced_block:
        provider.set_last_synced_block(start_block)
        #
        # raise ValueError(
        #     f'Last synced block number {last_synced_block} is less then --start-block {start_block}. '
        #     'Either remove the last synced block data or the respecify --start-block option.'
        # )
    if last_synced_block and last_synced_block > start_block:
        logging.info(
            f'Last synced block number {last_synced_block} is greater then --start-block. '
            f'Using last synced block number as start block.'
        )
    else:
        provider.set_last_synced_block(start_block)


def read_last_synced_block(file):
    with smart_open(file, 'r') as last_synced_block_file:
        return int(last_synced_block_file.read())


def write_to_file(file, content):
    with smart_open(file, 'w') as file_handle:
        file_handle.write(content)


class LastSyncedBlockProvider(ABC):
    @abstractmethod
    def get_last_synced_block(self): ...

    @abstractmethod
    def set_last_synced_block(self, last_synced_block): ...

    @classmethod
    def from_uri(cls, uri, chain_id):
        if uri.startswith('file://'):
            return LastSyncedBlockProviderFile(uri.removeprefix('file://'))

        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        table_name = query_params.pop('table_name', ['last_synced_block'])[0]
        redis_key = query_params.pop('key', ['last_synced_block'])[0]
        query = urlencode(query_params, encoding='utf-8')
        uri = parsed_uri._replace(query=query)
        uri = urlunparse(uri)

        if uri.startswith('redis://'):
            # 'redis://localhost:6379/0?key=last_synced_block'
            return LastSyncedBlockProviderRedis(uri, f'{redis_key}:{chain_id}')

        return LastSyncedBlockProviderSQL(uri, chain_id, table_name=table_name)


class LastSyncedBlockProviderSQL(LastSyncedBlockProvider):
    def __init__(self, connection_string, chain_id, table_name):
        from sqlalchemy import Column, Integer, create_engine
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.orm import sessionmaker

        self.engine = create_engine(connection_string)
        self.session = sessionmaker(bind=self.engine)
        self.table_name = table_name
        self.chain_id = chain_id

        Base = declarative_base()

        class LastSyncedBlock(Base):  # type: ignore
            __tablename__ = self.table_name
            chain_id = Column(UInt32, primary_key=True)
            block_number = Column(Integer)
            indexed_ts = Column(Integer)

            if 'clickhouse' in connection_string:
                from clickhouse_sqlalchemy import engines

                __table_args__ = (
                    engines.ReplacingMergeTree(order_by=['chain_id', 'block_number']),
                )

        self.LastSyncedBlock = LastSyncedBlock

        Base.metadata.create_all(self.engine)

    def get_last_synced_block(self):
        with self.session() as session:
            last_synced_block = (
                session.query(self.LastSyncedBlock)
                .filter_by(chain_id=self.chain_id)
                .order_by(self.LastSyncedBlock.block_number.desc())
                .first()
            )
        if last_synced_block is None:
            return 0
        return last_synced_block.block_number

    def set_last_synced_block(self, last_synced_block):
        last_synced_block_record = self.LastSyncedBlock(
            chain_id=self.chain_id, block_number=last_synced_block, indexed_ts=timestamp_now()
        )
        last_synced_block_record.block_number = last_synced_block
        with self.session() as session:
            session.add(last_synced_block_record)
            session.commit()


class LastSyncedBlockProviderRedis(LastSyncedBlockProvider):
    def __init__(self, redis_url, key):
        import redis

        self.redis = redis.from_url(redis_url)
        self.key = key

    def get_last_synced_block(self):
        last_synced_block = self.redis.get(self.key)
        if last_synced_block is None:
            return 0
        return int(last_synced_block)

    def set_last_synced_block(self, last_synced_block):
        self.redis.set(self.key, last_synced_block)


class LastSyncedBlockProviderFile(LastSyncedBlockProvider):
    def __init__(self, file):
        self.file = file

    def get_last_synced_block(self):
        try:
            return read_last_synced_block(self.file)
        except FileNotFoundError:
            return 0

    def set_last_synced_block(self, last_synced_block):
        write_last_synced_block(self.file, last_synced_block)

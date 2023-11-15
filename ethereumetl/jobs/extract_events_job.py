from collections.abc import Iterable

from blockchainetl.jobs.base_job import BaseJob


# Extract events from logs
class PrepareForEventsJob(BaseJob):
    def __init__(
        self,
        logs: Iterable[dict],
        token_transfers: Iterable[dict],
        transactions: Iterable[dict],
        batch_size,
        batch_web3_provider,
        max_workers,
        item_exporter,
    ):
        self.logs = logs
        self.token_transfers = token_transfers
        self.transactions = sorted(transactions, key=lambda x: x['block_number'])
        self.item_exporter = item_exporter

    def _start(self):
        self.item_exporter.open()

    def _end(self):
        self.item_exporter.close()

    def _export(self):
        """
        Output:
            [
                {
                    'logs': [logs of block 1],
                    'transfers': [transfers of block 1],
                    'transactions': [transactions of block 1],
                },
                {
                    'logs': [logs of block 2],
                    'transfers': [transfers of block 2],
                    'transactions': [transactions of block 2],
                }
            ].
        """
        txns_for_block: list[dict] = []
        for transaction in self.transactions:
            block_number = transaction['block_number']
            if len(txns_for_block) == 0:
                txns_for_block.append(transaction)
            elif txns_for_block[0]['block_number'] == block_number:
                txns_for_block.append(transaction)
            else:
                item = self._make_item(txns_for_block)
                self.item_exporter.export_item(item)
                txns_for_block = [transaction]
        if len(txns_for_block) > 0:
            item = self._make_item(txns_for_block)
            self.item_exporter.export_item(item)

    def _make_item(self, transactions):
        return {
            'logs': [
                log for log in self.logs if log['block_number'] == transactions[0]['block_number']
            ],
            'transfers': [
                transfer
                for transfer in self.token_transfers
                if transfer['block_number'] == transactions[0]['block_number']
            ],
            'transactions': transactions,
            'type': 'pre_event',
            'id': f'event_{transactions[0]["block_number"]}',
        }

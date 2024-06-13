from typing import Protocol


class StreamerAdapterStub(Protocol):
    def open(self):
        pass

    def get_current_block_number(self) -> int:
        pass

    def export_all(self, start_block, end_block):
        pass

    def close(self):
        pass

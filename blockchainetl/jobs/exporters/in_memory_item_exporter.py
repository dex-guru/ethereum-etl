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


from blockchainetl.exporters import BaseItemExporter


class InMemoryItemExporter(BaseItemExporter):
    def __init__(self, item_types):
        super().__init__()
        self.item_types = item_types
        self.items = {}

    def open(self):
        for item_type in self.item_types:
            self.items[item_type] = []

    def export_item(self, item):
        item_type = item.get('type', None)
        if item_type is None:
            raise ValueError(f'type key is not found in item {item!r}')

        self.items[item_type].append(item)

    def export_items(self, items):
        for item in items:
            self.export_item(item)

    def close(self):
        pass

    def get_items(self, item_type):
        return self.items[item_type]

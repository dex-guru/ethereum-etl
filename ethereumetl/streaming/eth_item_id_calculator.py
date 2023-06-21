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

import json
import logging
import uuid

from ethereumetl.enumeration.entity_type import EntityType


class EthItemIdCalculator:
    ID_FIELDS: dict[EntityType, tuple[str, ...]] = {
        EntityType.BLOCK: ('hash',),
        EntityType.TRANSACTION: ('hash',),
        EntityType.LOG: ('transaction_hash', 'log_index'),
        EntityType.TOKEN_TRANSFER: ('transaction_hash', 'log_index'),
        EntityType.TOKEN_BALANCE: ('block_number', 'token_address', 'holder_address', 'token_id'),
        EntityType.TRACE: ('trace_id',),
        EntityType.CONTRACT: ('block_number', 'address'),
        EntityType.TOKEN: ('block_number', 'address'),
        EntityType.ERROR: ('block_number', uuid.uuid4().hex),
        EntityType.GETH_TRACE: ('block_number', 'transaction_hash'),
        EntityType.INTERNAL_TRANSFER: ('block_number', 'transaction_hash', 'id'),
    }

    def calculate(self, item):
        if item is None or not isinstance(item, dict):
            return None

        item_type = item.get('type')
        fields = self.ID_FIELDS.get(EntityType(item_type))

        if fields:
            values = [item.get(field) for field in fields]
            if values:
                # Special handling for 'error' type, which generates a random UUID
                if item_type == EntityType.ERROR:
                    values.append(uuid.uuid4().hex)
                return '_'.join(map(str, (item_type, *values)))

        logging.warning('item_id for item %s is None', json.dumps(item))
        return None

import dataclasses
import json

from ethereumetl.domain.error import EthError
from ethereumetl.enumeration.entity_type import EntityType


class EthErrorMapper:
    ERROR_ITEM_FIELDS: set[str] = {
        *(f.name for f in dataclasses.fields(EthError) if f.name != 'data'),
        'data_json',
        'type',
    }

    @staticmethod
    def error_to_dict(error: EthError) -> dict[str, int | str | None]:
        res = dataclasses.asdict(error)
        res['type'] = EntityType.ERROR.value
        res['data_json'] = json.dumps(res.pop('data'), separators=(',', ':'), check_circular=False)
        return res

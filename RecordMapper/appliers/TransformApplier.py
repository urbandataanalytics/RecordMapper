import re
from typing import Callable

from RecordMapper.types import Record, Variables, NoDefault

class TransformApplier(object):

    def __init__(self, schema: dict):

        self.transform_dict = self.create_transform_dict(schema)

    def apply(self, record: Record) -> Record:

        new_record = {}
        for key, value in record.items():
            if key in self.transform_dict:
                transform_function = self.transform_dict[key]
                new_record[key] = transform_function(value, record)
            else:
                new_record[key] = value
        return new_record

    @staticmethod
    def create_transform_dict(schema: dict) -> dict:

        transform_dict = {}
        for field in schema[Variables.FIELDS]:
            if Variables.TRANSFORM in field:
                field_name = field[Variables.NAME]
                transform_name = field[Variables.TRANSFORM]
                transform_dict[field_name] = TransformApplier.get_transform_function(transform_name)
        return transform_dict

    @staticmethod
    def get_transform_function(transform_name: str) -> Callable[[object, dict], object]:

        selected_function = None
        if transform_name == "int2boolean":
            def int2boolean(value: int, record: dict) -> bool:
                if value is None:
                    return None
                else:
                    return int(value) > 0
            selected_function = int2boolean
        elif re.match("copyFrom\(\w+\)", transform_name):
            copied_field = re.match("copyFrom\((\w+)\)", transform_name)[1]

            def copyFrom(value: int, record: dict) -> bool:
                if copied_field in record:
                    return record[copied_field]
                else:
                    raise RuntimeError("Invalid field in copyFrom")

            selected_function = copyFrom
        # If there is not an available function, raise Exception
        if selected_function is None:
            raise RuntimeError(f"Invalid or unsupported name for a transform function: '{transform_name}'")
        return selected_function
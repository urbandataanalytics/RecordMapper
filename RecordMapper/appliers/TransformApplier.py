import re
from inspect import getmembers, isfunction
from typing import Callable

from RecordMapper.types import Record, Variables, NoDefault
from RecordMapper.appliers import transform_functions

class TransformApplier(object):

    def __init__(self, schema: dict):

        self.transforms_schema = self.create_transform_schema(schema)
        self.max_phases = max([len(transforms_list) for _, transforms_list in self.transforms_schema.items()])

    # def apply(self, record: Record) -> Record:

    #     new_record = {}
    #     for key, value in record.items():
    #         if key in self.transform_dict:
    #             transform_function = self.transform_dict[key]
    #             new_record[key] = transform_function(value, record)
    #         else:
    #             new_record[key] = value
    #     return new_record

    @staticmethod
    def create_transform_schema(schema: dict) -> dict:

        transform_schema = {}
        schema_name = schema["name"]
        for field in schema[Variables.FIELDS]:
            field_name = field[Variables.NAME]
            if Variables.TRANSFORM in field:
                transform_list = field[Variables.TRANSFORM]
                if isinstance(transform_list, str):
                    print("transform_list:", transform_list)
                    transform_list = [transform_list]
                print("list:", transform_list)
                transform_schema[field_name] = [TransformApplier.get_transform_function(transform_name) for transform_name in transform_list]
            else:
                transform_schema[field_name] = []
        return schema_name, transform_schema

    @staticmethod
    def get_transform_function(transform_name: str) -> Callable[[object, dict], object]:

        print("transform_name:", transform_name)

        parsed_transform_name = re.match("^(\w+)(?:\(([\w|,]+)\))?$", transform_name)
        if not parsed_transform_name:
            raise RuntimeError(f"Invalid name for a transform function: '{transform_name}'")

        function_name, args = parsed_transform_name.groups()

        possible_function = [obj for name, obj in getmembers(transform_functions) if name == function_name and isfunction(obj)]

        if len(possible_function) == 1:

            args_list = str(args).split(",") if args is not None else []
            return possible_function[0](*args_list)


 
        raise RuntimeError(f"Unsupported name for a transform function: '{transform_name}'")
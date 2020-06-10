import re
import importlib
from collections import namedtuple
from inspect import getmembers, isfunction
from typing import Callable, List

from RecordMapper.types import Record, Variables, NoDefault, InvalidTransformException
from RecordMapper.appliers import transform_functions

TransformTuple = namedtuple("TransformTuple", ["functions_list", "field_type"])

class TransformApplier(object):

    def __init__(self, *schemas: List[dict]):

        self.transforms_schema = dict([self.create_transform_schema(schema) for schema in schemas])

    def apply(self, base_schema: str, record: Record) -> Record:

        complete_transform_schema = self.get_complete_transform_schema(base_schema)

        max_phases = max([len(transform_tuple.functions_list) for _, transform_tuple in complete_transform_schema.items()])

        # Initial copy of the record
        new_record = dict([((key,), value) for key, value in record.items()])

        # Iterate over phases and get the transform_functions
        for phase_index in range(max_phases):
            new_values_in_this_phase = {}
            transforms_by_key = [
               (field_key, field_function_list[phase_index])
               for field_key, (field_function_list, field_type) in complete_transform_schema.items()
               if phase_index < len(field_function_list) and field_function_list[phase_index] is not None
            ] 

            for key, transform_function in transforms_by_key:
                new_values_in_this_phase[key] = self.get_new_value_from_record(new_record, key, transform_function, complete_transform_schema)
            
            new_record = {**new_record, **new_values_in_this_phase}

        return TransformApplier.flat_record_to_tree_record(new_record)

    def get_new_value_from_record(self, record, composed_key: tuple, transform_function, complete_schema):

        prev_value = record.get(composed_key, None)
        return transform_function(prev_value, record, complete_schema)

    def get_complete_transform_schema(self, base_schema_name: str):

        base_transform_schema = self.transforms_schema[base_schema_name]

        final_complete_schema = {**base_transform_schema}

        for super_key, (function_list, field_type_list) in base_transform_schema.items():

            # If there is a nested schema, add it to the final_complete_schema
            nested_schema_names = [field_type for field_type in field_type_list if field_type in self.transforms_schema.keys()]
            if len(nested_schema_names) == 1:
                nested_schema = self.transforms_schema[nested_schema_names[0]]
                for nested_key, (nested_function_list, nested_field_type) in nested_schema.items():
                    final_complete_schema[super_key + nested_key] = TransformTuple(nested_function_list, nested_field_type)
            elif len(nested_schema_names) > 1:
                raise RuntimeError(f"A key has several nested record types! (Not supported) -> {super_key}")

        return final_complete_schema

    @staticmethod
    def create_transform_schema(schema: dict) -> dict:

        transform_schema = {}
        schema_name = schema["name"]
        for field in schema[Variables.FIELDS]:
            field_name = field[Variables.NAME]
            field_type = field[Variables.TYPE]
            # Get type as list if it is a string
            type_list = field_type if not isinstance(field_type, str) else [field_type]

            if Variables.TRANSFORM in field:
                transform_list = field[Variables.TRANSFORM]
                if isinstance(transform_list, str):
                    transform_list = [transform_list]
                transform_function_list = [
                    TransformApplier.get_transform_function(transform_name) 
                    if transform_name is not None else None 
                    for transform_name in transform_list
                ]
                transform_schema[(field_name,)] = TransformTuple(transform_function_list, type_list)
            else:
                transform_schema[(field_name,)] = TransformTuple([], type_list)

        return schema_name, transform_schema



    @staticmethod
    def flat_record_to_tree_record(flat_record: dict) -> Record:

        print("flaat:", flat_record)
        res_record = {}
        for composed_key, value in flat_record.items():
            print("res_record:", res_record)
            if len(composed_key) == 1:
                res_record[composed_key[0]] = value

            elif len(composed_key) == 2:
                super_key, nested_key = composed_key
                if super_key not in res_record:
                    res_record[super_key] = {}
                
                if isinstance(res_record[super_key], dict):
                    res_record[super_key][nested_key] = value
                else:
                    raise RuntimeError(f"Nested key in a non-dict field: {composed_key}")
            else:
                raise NotImplementedError(f"Three-depth record level is not supported -> {composed_key}")
        
        return res_record

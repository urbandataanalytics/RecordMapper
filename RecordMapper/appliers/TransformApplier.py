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
        new_record = dict([((key,), transform_tuple) for key, transform_tuple in record.items()])

        # Iterate over phases and get the transform_functions
        for phase_index in range(max_phases):
            print("new_record:", phase_index, new_record)
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

        for super_key, (function_list, field_type) in base_transform_schema.items():
            # If there is a nested schema, add it to the final_complete_schema
            if field_type in self.transforms_schema.keys():
                nested_schema = self.transforms_schema[field_type]
                for nested_key, (nested_function_list, nested_field_type) in nested_schema.items():
                    final_complete_schema[super_key + nested_key] = TransformTuple(nested_function_list, nested_field_type)

        return final_complete_schema

    @staticmethod
    def create_transform_schema(schema: dict) -> dict:

        transform_schema = {}
        schema_name = schema["name"]
        for field in schema[Variables.FIELDS]:
            field_name = field[Variables.NAME]
            field_type = field[Variables.TYPE]

            if Variables.TRANSFORM in field:
                transform_list = field[Variables.TRANSFORM]
                if isinstance(transform_list, str):
                    transform_list = [transform_list]
                transform_function_list = [
                    TransformApplier.get_transform_function(transform_name) 
                    if transform_name is not None else None 
                    for transform_name in transform_list
                ]
                transform_schema[(field_name,)] = TransformTuple(transform_function_list, field_type)
            else:
                transform_schema[(field_name,)] = TransformTuple([], field_type)

        return schema_name, transform_schema

    @staticmethod
    def get_transform_function(transform_name: str) -> Callable[[object, dict], object]:

        parsed_transform_name = re.match("^([\.\w]+)(?:\(([\w|,]+)\))?$", transform_name)
        if not parsed_transform_name:
            raise RuntimeError(f"Invalid name for a transform function: '{transform_name}'")

        function_name, args = parsed_transform_name.groups()
        args_list = str(args).split(",") if args is not None else []

        # Check if it is a built-in function
        builtin_function = TransformApplier.get_builtin_transform_function(function_name, args_list) 

        if builtin_function is not None:
            return builtin_function

        # Get it as custom function
        return TransformApplier.get_custom_transform_function(function_name, args_list)
 

    @staticmethod
    def get_builtin_transform_function(function_name: str, args_list: List[str]):
        # Check if it is a built-in function
        possible_function = [obj for name, obj in getmembers(transform_functions) if name == function_name and isfunction(obj)]

        if len(possible_function) == 1:
            return possible_function[0](*args_list)
        else:
            return None

    @staticmethod
    def get_custom_transform_function(function_name: str, args_list: List[str]):

        parts = function_name.split(".")
        module_path = ".".join(parts[:-1])
        function_name = parts[-1]
        try:
            mod = importlib.import_module(module_path)
            transform_function = getattr(mod, function_name)
        except ModuleNotFoundError:
            raise InvalidTransformException(f"Invalid module for a custom transform function: '{function_name}'")
        except ValueError:
            raise InvalidTransformException(f"Invalid module for a custom transform function: '{function_name}'")
        except AttributeError:
            raise InvalidTransformException(f"Invalid name for a custom transform function: '{function_name}'")

        return transform_function(*args_list)

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

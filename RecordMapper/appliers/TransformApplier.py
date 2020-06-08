import re
import importlib
from inspect import getmembers, isfunction
from typing import Callable, List

from RecordMapper.types import Record, Variables, NoDefault, InvalidTransformException
from RecordMapper.appliers import transform_functions

class TransformApplier(object):

    def __init__(self, *schemas: List[dict]):

        self.transforms_schema = dict([self.create_transform_schema(schema) for schema in schemas])
        self.max_phases = max([len(transforms_list) for _, transforms_list in self.transforms_schema.items()])

    def apply(self, base_schema: str, record: Record) -> Record:

        base_transform_schema = self.transforms_schema[base_schema]

        # Initial copy of the record
        new_record = {**record}

        # Iterate over phases and get the transform_functions
        for phase_index in range(self.max_phases):
            new_values_in_this_phase = {}
            transforms_by_key = [
               (key, transform_list[phase_index])
               for key, transform_list in base_transform_schema.items()
               if phase_index < len(transform_list) and transform_list[phase_index] is not None
            ] 
            for key, transform_function in transforms_by_key:
                prev_value = record.get(key, None)
                new_value = transform_function(prev_value, new_record, base_schema)
                new_values_in_this_phase[key] = new_value
            
            new_record = {**new_record, **new_values_in_this_phase}

        return new_record

    

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
                transform_schema[field_name] = [
                    TransformApplier.get_transform_function(transform_name) if transform_name is not None else None 
                    for transform_name in transform_list
                ]
            else:
                transform_schema[field_name] = []
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
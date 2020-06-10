from typing import Union, List
from collections import namedtuple

from RecordMapper.builders import FunctionBuilder

FieldData = namedtuple("FieldData", ["types", "aliases", "transforms", "selector"])

class FlatSchemaBuilder(object):

    @staticmethod
    def get_flat_schema(schema: dict) -> dict:

        schema_fields = schema["fields"]

        flat_schema = {}

        for field in schema_fields:

            field_name = field["name"]
            field_types = field["type"] if not isinstance(field["type"], str) else [field["type"]]
            field_aliases = field["aliases"] if "aliases" in field else []
            field_transforms = FlatSchemaBuilder.get_transform_functions_from_field(field.get("transform", None)) 
            field_selector = FlatSchemaBuilder.get_selector_function(field.get("nestedSchemaSelector", None))

            field_data = FieldData(field_types, field_aliases, field_transforms, field_selector) 

            # Key as tuple
            flat_schema[(field_name,)] = field_data
        
        return flat_schema

    @staticmethod
    def get_transform_functions_from_field(transform_field: Union[str, List[str]]) -> list:

        # First, get a string list 
        function_str_list = None
        if transform_field is None:
            function_str_list = []
        elif isinstance(transform_field, str):
            function_str_list = [transform_field]
        else:
            function_str_list = transform_field

        return [FunctionBuilder.parse_function_str(function_str) for function_str in function_str_list]

    @staticmethod
    def get_selector_function(selector_field: str) -> 'function':

        if selector_field is None:
            return None
        else:
            return FunctionBuilder.parse_function_str(selector_field)

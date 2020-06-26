from typing import Union, List, Callable
from collections import namedtuple

from RecordMapper.builders import FunctionBuilder

FieldData = namedtuple("FieldData", ["types", "aliases", "transforms", "selector"])

class FlatSchemaBuilder(object):
    """A builder class that creates a FlatSchema.
    """

    @staticmethod
    def get_flat_schema(schema: dict) -> dict:
        """Creates a FlatSchema from a normal avro Schema.

        A FlatSchema is a dict where the keys are tuples and the values "FieldData" objects.

        :param schema: Normal Avro schema. 
        :type schema: dict
        :return: A FlatSchema.
        :rtype: dict
        """

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
    def get_transform_functions_from_field(transform_field: Union[str, List[str]]) -> List[Callable]:
        """Generate a list of transform functions from a list of string values (or a single string value).

        :param transform_field: A list of string values (or a single string value) that reference one or several transform functions.
        :type transform_field: Union[str, List[str]]
        :return: List of transform functions.
        :rtype: List[Callable]
        """

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
    def get_selector_function(selector_field: str) -> Callable:
        """Generates a Selector function from a string.

        :param selector_field: A string that represents a Selector function.
        :type selector_field: str
        :return: A Selector function.
        :rtype: Callable
        """

        if selector_field is None:
            return None
        else:
            return FunctionBuilder.parse_function_str(selector_field)

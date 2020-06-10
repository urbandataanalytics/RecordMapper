from typing import Union


class FlatSchemaBuilder(object):

    def __init__(schemas: List[dict]):

        self.schemas = dict([(schema["name"], self.get_flat_schema(schema)) for schema in schemas]

    @staticmethod
    def get_flat_schema(schema: dict) -> dict:

        schema_fields = schema["fields"]

        for field in schema_fields:

            field_name = field["name"]
            field_types = field["type"] if not isinstance(field["type"], str) else [field["type"]]
            field_aliases = field["aliases"] if "aliases" in field else []
            field_transforms = field
    


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


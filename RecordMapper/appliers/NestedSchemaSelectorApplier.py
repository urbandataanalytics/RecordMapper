import re
import importlib
from collections import namedtuple
from inspect import getmembers, isfunction
from typing import Callable, List, Dict


class NestedSchemaSelectorApplier(object):

    def __init__(self, flat_schemas: Dict[str, dict]):

        self.flat_schemas = flat_schemas

    def apply(self, record: dict, base_flat_schema: dict) -> (dict, dict):

        # The value that will be returned with the record
        complete_flat_schema = {**base_flat_schema}

        for field_key, field_data in base_flat_schema.items():

            if field_data.selector is not None:
                complete_flat_schema = self.select_nested_schema_and_add_their_fields(record, complete_flat_schema, field_key, field_data)
                # Remove the "super key" to avoid problems
                del complete_flat_schema[field_key]

        return (record, complete_flat_schema)

    def select_nested_schema_and_add_their_fields(self, record: dict, flat_schema: dict, field_key: str, field_data: tuple):

                nested_schema_name = field_data.selector(field_key, record)

                # Not schema selected (this field will be null)
                if nested_schema_name is None:
                    return flat_schema

                # Schema selected. Add their fields to the complete schema
                elif nested_schema_name in self.flat_schemas:

                    nested_schema = self.flat_schemas[nested_schema_name]

                    fields_to_add = dict(
                        [
                            (field_key+nested_key, nested_field_data)
                            for nested_key, nested_field_data in nested_schema.items()
                        ]
                    )
                    
                    return {**flat_schema, **fields_to_add} 
                else:
                    raise RuntimeError(f"Invalid nested schema name: {nested_schema_name}")
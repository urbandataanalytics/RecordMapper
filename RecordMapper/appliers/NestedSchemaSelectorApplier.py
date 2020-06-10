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
                nested_schema_name = field_data.selector(field_key, record)
                if nested_schema_name in self.flat_schemas:
                    print("nested_schema_name:", nested_schema_name)

                    nested_schema = self.flat_schemas[nested_schema_name]

                    for nested_key, nested_field_data in nested_schema.items():
                        complete_flat_schema[field_key+nested_key] = nested_field_data

                    # Remove the "super key" to avoid problems
                    del complete_flat_schema[field_key]
                else:
                    raise RuntimeError(f"Invalid nested schema name: {nested_schema_name}")

        return (record, complete_flat_schema)
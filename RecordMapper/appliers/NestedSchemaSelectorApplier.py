import re
import importlib
from collections import namedtuple
from inspect import getmembers, isfunction
from typing import Callable, List

from RecordMapper.types import Record, Variables, NoDefault, InvalidTransformException
from RecordMapper.appliers import transform_functions

TransformTuple = namedtuple("TransformTuple", ["functions_list", "field_type"])

class NestedSchemaSelectorApplier(object):

    def __init__(self, *schemas: List[dict]):

        self.schemas = schemas

    def apply(self, base_schema_name: str, available_nested_schemas: List[str], record: Record) -> Record

        res_available_nested_schemas = []

        base_schema = self.schemas[base_schema_name]

        for field in base_schema[Variables.FIELDS]:
            if Variables.SELECTOR in field:
                type_list = field["type"] if not isinstance(field["type"], str) else None
                selected_schema = NestedSchemaSelectorApplier.execute_selector_function(field[Variables.SELECTOR], type_list, record)
                # Add to available schemas if 
                res_available_nested_schemas.append(selected_schema) if selected_schema is not None else None
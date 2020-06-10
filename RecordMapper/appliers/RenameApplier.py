from typing import List, Union

from collections import namedtuple

from RecordMapper.types import Record, Variables, NoDefault

RenameTuple = namedtuple("RenameTuple", ['new_key', 'types'])

class RenameApplier(object):

    def __init__(self, *schemas: List[dict]):

        self.rename_schemas = dict([self.create_rename_dict(schema) for schema in schemas])

    @staticmethod
    def create_rename_dict(schema: dict) -> (str, dict):

        schema_name = schema["name"]
        rename_dict = {}

        for field in schema[Variables.FIELDS]:
            field_name = field[Variables.NAME]
            field_type = field[Variables.TYPE]
            print("field_type:", field_type)
            type_list = field_type if isinstance(field_type, list) else [field_type]
            print("type_list:", type_list)
            # Rename field_name to itself
            rename_dict[field_name] = RenameTuple(field_name, type_list)
            # rename aliases
            if Variables.ALIASES in field:
                for alias in field[Variables.ALIASES]:
                    rename_dict[alias] = RenameTuple(field_name, type_list)


        return schema_name, rename_dict

    def apply(self, base_schema: str, record: Record) -> Record:
        print("rename_schema:", self.rename_schemas)

        base_rename_schema = self.rename_schemas[base_schema]

        new_record = {}

        # Rename each file of the original record
        for key, value in record.items():

            # It gets the renameTuple or get a "dummy" one if it does not exist
            renameTuple = base_rename_schema[key] if key in base_rename_schema else RenameTuple(key, [])

            nested_record_types = [
                nested_type
                for nested_type in renameTuple.types
                if nested_type in self.rename_schemas
            ]

            print("nested_record_types:", nested_record_types)
            # It is not a nested record.
            if len(nested_record_types) == 0:
                new_value = value 
            # It is a nested record. We will get the value recursively.
            elif len(nested_record_types) == 1:
                new_value = self.apply(nested_record_types[0], value)
            # It is not a recognizable nested record
            else:
                raise RuntimeError(f"A key has several nested record types! (Not supported) -> {key}")

            new_record[renameTuple.new_key] = new_value

        return new_record
    
    def is_a_nested_record(self, type_field: Union[str, list]) -> str:
        if type_field is str:
            return type_field if type_field in self.rename_schemas.keys() else None
        if type_field is list:
            record_types = [type_value for type_value in type_field if type_value in self.rename_schemas.keys()]
            return record_types[0]

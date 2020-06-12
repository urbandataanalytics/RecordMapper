import re
from typing import List, Iterable, BinaryIO


from RecordMapper.appliers  import NestedSchemaSelectorApplier
from RecordMapper.appliers import RenameApplier
from RecordMapper.appliers import TransformApplier
from RecordMapper.appliers import CleanApplier

from RecordMapper.builders import FlatSchemaBuilder
from RecordMapper.builders import FlatRecordBuilder

from RecordMapper.utils    import chain_functions
from RecordMapper.avro.AvroWriter import AvroWriter
from RecordMapper.avro.AvroReader import AvroReader
from RecordMapper.csv.CSVWriter import CSVWriter
from RecordMapper.csv.CSVReader import CSVReader


class RecordMapper(object):
    def __init__(self, base_schema: dict, nested_schemas: List[dict] = []):
        self.original_base_schema = base_schema
        self.original_nested_schemas = nested_schemas

        self.flat_schemas = dict(
            [
                (schema["name"], FlatSchemaBuilder.get_flat_schema(schema))
                for schema in ([self.original_base_schema] + self.original_nested_schemas)
            ]
        )

        self.selector_applier = NestedSchemaSelectorApplier(self.flat_schemas)
        self.rename_applier = RenameApplier()
        self.transform_applier = TransformApplier()
        self.clean_applier = CleanApplier()
    
    def execute(self, input_format: str, input_file_path: str, paths_to_write: dict,
        input_opts: dict = {}, base_schema_to_write: dict = None, nested_schemas_to_write : List[dict] = None):

        read_records = self.read_records(input_format, input_file_path, input_opts)
        transformed_records = self.transform_records(read_records)
        self.write_records(transformed_records, paths_to_write, base_schema_to_write, nested_schemas_to_write)


    def transform_records(self, record_list: List[dict]) -> Iterable:

        for record in record_list:
            yield self.transform_record(record)

    def transform_record(self, record: dict) -> dict:

        base_schema_name = self.original_base_schema["name"]

        base_flat_schema = self.flat_schemas[base_schema_name]

        flat_record = FlatRecordBuilder.get_flat_record_from_normal_record(record)

        all_functions = chain_functions(
            self.selector_applier.apply,
            self.rename_applier.apply,
            self.transform_applier.apply,
            self.clean_applier.apply
        )
        
        transformed_record, transformed_flat_schema = all_functions(flat_record, base_flat_schema)

        normal_record = FlatRecordBuilder.get_normal_record_from_flat_record(transformed_record)

        return normal_record
    
    def read_records(self, input_format: str, path_to_read: str, opts : dict = {}):

        if input_format == "avro":
            reader_object = AvroReader(path_to_read)
        elif input_format == "csv":
            reader_object = CSVReader(path_to_read)
        else:
            raise RuntimeError(f"Invalid input format: {input_format}")

        for record in reader_object.read_records():
            yield record
        
        reader_object.close()

    def write_records(self, records_list: Iterable, paths_to_write: dict, base_schema_to_write: dict=None, nested_schemas_to_write : List[dict] = None):

        base_schema_to_write = self.original_base_schema if base_schema_to_write is None else base_schema_to_write
        nested_schemas_to_write = self.original_nested_schemas if nested_schemas_to_write is None else nested_schemas_to_write

        if "avro" not in paths_to_write:
            raise RuntimeError("It is necessary a path to write an Avro File")

        # Avro writing
        writer = AvroWriter(
            paths_to_write["avro"],
            base_schema_to_write,
            nested_schemas_to_write
        )
        writer.write_records(records_list)
        writer.close()
        
        if "csv" in paths_to_write:

            fieldnames = [field["name"] for field in base_schema_to_write["fields"]]

            records = AvroReader(paths_to_write["avro"]).read_records()
            CSVWriter(paths_to_write["csv"], fieldnames).write_records(records)

    
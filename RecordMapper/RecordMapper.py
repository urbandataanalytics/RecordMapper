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

    def write_records(self, records_list: Iterable, paths_to_write: dict, base_schema_to_write: dict=None, nested_schemas_to_write : List[dict] = None):

        base_schema_to_write = self.original_base_schema if base_schema_to_write is None else base_schema_to_write
        nested_schemas_to_write = self.original_nested_schemas if nested_schemas_to_write is None else nested_schemas_to_write

        if "avro" not in paths_to_write:
            raise RuntimeError("It is necessary a path to write an Avro File")

        with open(paths_to_write["avro"], "wb") as f: 
            writer = AvroWriter(
                f,
                base_schema_to_write,
                nested_schemas_to_write
            )

            writer.write_records(records_list)
            
            writer.close()
        
        if "csv" in paths_to_write:

            fieldnames = [field["name"] for field in base_schema_to_write["fields"]]

            with open(paths_to_write["csv"], "w") as csv_f:
                with open(paths_to_write["avro"], "rb") as avro_f:
                    records = AvroReader(avro_f).read_records()
                    CSVWriter(csv_f, fieldnames).write_records(records)

    
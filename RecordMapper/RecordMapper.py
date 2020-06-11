import re
from typing import List
from RecordMapper.appliers  import NestedSchemaSelectorApplier
from RecordMapper.appliers import RenameApplier
from RecordMapper.appliers import TransformApplier
from RecordMapper.appliers import CleanApplier

from RecordMapper.builders import FlatSchemaBuilder
from RecordMapper.builders import FlatRecordBuilder

from RecordMapper.utils    import chain_functions


class RecordMapper(object):
    def __init__(self, schemas: List[dict]):
        self.original_schemas = schemas
        self.flat_schemas = dict([(schema["name"], FlatSchemaBuilder.get_flat_schema(schema)) for schema in self.original_schemas])

        self.selector_applier = NestedSchemaSelectorApplier(self.flat_schemas)
        self.rename_applier = RenameApplier()
        self.transform_applier = TransformApplier()
        self.clean_applier = CleanApplier()

    def transform_record(self, record: dict, schema_name: str) -> dict:

        base_flat_schema = self.flat_schemas[schema_name]

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

    
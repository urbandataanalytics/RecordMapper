import unittest

from RecordMapper.appliers import NestedSchemaSelectorApplier
from RecordMapper.builders.FlatSchemaBuilder import FieldData

class test_NestedSchemaSelectorApplier(unittest.TestCase):


    def test_apply(self):
        
        # Arrange
        def test_selector(key, record, custom_variables):

            return "NestSchema1"

        test_flat_schemas = {
            "base_schema": {
                ("field_1", ): FieldData(["string"], ["another_field"], [], None), 
                ("field_2", ): FieldData(["NestSchema1", "NestSchema2"], [], [], test_selector), 
            },
            "NestSchema1": {
                ("nested_field_1", ): FieldData(["int"], [], [], None) 
            },
            "NestedSchema2": {
                ("nested_field_2", ): FieldData(["int"], [], [], None) 
            }
        }

        test_input_record = {("field_1",): "hola"}

        # Act
        ne = NestedSchemaSelectorApplier(test_flat_schemas, {})
        res_record, res_complete_flat_schema = ne.apply(test_input_record, test_flat_schemas["base_schema"])

        # Assert
        self.assertDictEqual(res_record, test_input_record) # Without changes
        
        self.assertListEqual(list(res_complete_flat_schema.keys()), [("field_1",), ("field_2", "nested_field_1")])

    def test_apply_with_null_result_on_select(self):

        # Arrange
        def test_selector(key, record, custom_variables):

            return None

        test_flat_schemas = {
            "base_schema": {
                ("field_1", ): FieldData(["string"], ["another_field"], [], None), 
                ("field_2", ): FieldData(["NestSchema1", "NestSchema2", None], [], [], test_selector), 
            },
            "NestSchema1": {
                ("nested_field_1", ): FieldData(["int"], [], [], None) 
            },
            "NestedSchema2": {
                ("nested_field_2", ): FieldData(["int"], [], [], None) 
            }
        }

        test_input_record = {("field_1",): "hola"}

        # Act

        res_record, res_complete_flat_schema = NestedSchemaSelectorApplier(test_flat_schemas, {}).apply(test_input_record, test_flat_schemas["base_schema"])

        # Assert
        self.assertDictEqual(res_record, test_input_record) # Without changes
        
        self.assertListEqual(list(res_complete_flat_schema.keys()), [("field_1",) ])


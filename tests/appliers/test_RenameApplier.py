import unittest

from RecordMapper.appliers import RenameApplier
from RecordMapper.builders.FlatSchemaBuilder import FieldData


class test_RenameApplier(unittest.TestCase):

    def test_basic_renaming(self):

        # Arrange
        
        test_flat_schema = {
               ("field_1",): FieldData(["string"], [], [], []),
               ("field_renamed", ):FieldData(["string"], ["field_2"], [], []) 
        }

        input_record = {
            ("field_1",): "hola",
            ("field_2",): 56
        }

        expected_record = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_renamed",): 56 
        }

        # Act
        res_record, res_schema = RenameApplier({}).apply(input_record, test_flat_schema)
        
        # Assert
        self.assertDictEqual(res_record, expected_record)
        self.assertDictEqual(res_schema, test_flat_schema) # Without changes


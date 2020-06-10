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

        input_dict = {
            ("field_1",): "hola",
            ("field_2",): 56
        }

        expected_dict = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_renamed",): 56 
        }

        # Act
        res = RenameApplier().apply(input_dict, test_flat_schema)
        
        # Assert
        self.assertDictEqual(res, expected_dict)


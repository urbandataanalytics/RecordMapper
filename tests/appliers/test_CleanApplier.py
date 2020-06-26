import unittest

from RecordMapper.appliers import CleanApplier
from RecordMapper.builders.FlatSchemaBuilder import FieldData


class test_CleanApplier(unittest.TestCase):

    def test_apply(self):
        
        # Arrange
        test_flat_schema = {
               ("field_1",): FieldData(["string"], [], [], []),
               ("field_renamed", ):FieldData(["string"], [], [], []) 
        }

        input_record = {
            ("field_1",): "hola",
            ("field_3",): "adios"
        }

        expected_record = {
            ("field_1",): "hola"
        }

        # Act
        res_record, res_schema = CleanApplier({}).apply(input_record, test_flat_schema)

        # Assert
        self.assertEquals(res_record, expected_record)
        self.assertEquals(res_schema, test_flat_schema) # Without changes




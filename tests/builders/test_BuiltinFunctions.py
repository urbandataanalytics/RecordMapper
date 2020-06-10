import unittest

from RecordMapper.builders import BuiltinFunctions


class test_transform_functions(unittest.TestCase):

    def test_copyFrom(self):

        # Arrange
        input_record = {
            ("field_1",): 5,
            ("field_2",): "hola"
        }

        transform_function = BuiltinFunctions.copyFrom("field_2")

        # Act
        res = transform_function(None, input_record, None)

        # Assert
        self.assertEqual(res, "hola")

    def test_toNull(self):

        # Arrange
        input_record = {
            "field_1": 5,
            "field_2": "hola"
        }

        transform_function = BuiltinFunctions.toNull()

        # Act

        res = transform_function(None, input_record, None)

        # Assert
        self.assertEqual(res, None) 



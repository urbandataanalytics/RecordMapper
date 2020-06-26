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
        res = transform_function(None, input_record, None, {})

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

        res = transform_function(None, input_record, None, {})

        # Assert
        self.assertEqual(res, None) 
    
    def test_get_value_from_custom_variables(self):

        input_record = {
            "field_1": 5,
            "field_2": "hola"
        }

        transform_function = BuiltinFunctions.get_from_custom_variable("example_variable_1")

        # Act

        res = transform_function(None, input_record, None, {"example_variable_1": 5, "example_variable_2": 67})

        # Assert
        self.assertEqual(res, 5)





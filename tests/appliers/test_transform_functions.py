import unittest

from RecordMapper.appliers.TransformApplier import transform_functions


class test_transform_functions(unittest.TestCase):

    def test_copyFrom(self):

        # Arrange
        input_record = {
            "field_1": 5,
            "field_2": "hola"
        }

        transform_function = transform_functions.copyFrom("field_2")

        # Act

        res = transform_function(None, input_record, None)

        # Assert
        self.assertEqual(res, "field_2")

    def test_toNull(self):

        # Arrange
        input_record = {
            "field_1": 5,
            "field_2": "hola"
        }

        transform_function = transform_functions.toNull()

        # Act

        res = transform_function(None, input_record, None)

        # Assert
        self.assertEqual(res, None) 



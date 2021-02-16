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

    def test_toFloat(self):
        # Arrange
        input_record = {
            "field_1": "1.111",
            "field_2": "0",
            "field_3": "julian",
            "field_4": "1.123456789123456789123456789"
        }

        expected_results = {
            "res_1": 1.111,
            "res_2": 0.0,
            "res_3": None,
            "res_4": 1.1234567891234568
        }

        transform_function = BuiltinFunctions.toFloat()

        # Act

        res1 = transform_function(input_record["field_1"], input_record, None, {})
        res2 = transform_function(input_record["field_2"], input_record, None, {})
        res3 = transform_function(input_record["field_3"], input_record, None, {})
        res4 = transform_function(input_record["field_4"], input_record, None, {})

        # Assert
        self.assertEqual(expected_results["res_1"], res1)
        self.assertEqual(expected_results["res_2"], res2)
        self.assertEqual(expected_results["res_3"], res3)
        self.assertEqual(expected_results["res_4"], res4)

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

    def test_toDate_unsuccessful_unique_value(self):
        input_record = {
            "field_1": "2019-07-17 10:51:57.0"
        }

        try:
            transform_function = BuiltinFunctions.toDate("%Y-%m-%d %H:%M:%S")
            res = transform_function(input_record["field_1"], input_record, None, {})
        except:
            assert True
        else:
            assert False

    def test_toDate_successful_unique_value(self):
        input_record = {
            "field_1": "2019-07-17 10:51:57.0"
        }

        try:
            transform_function = BuiltinFunctions.toDate("%Y-%m-%d %H:%M:%S.%f")
            res = transform_function(input_record["field_1"], input_record, None, {})
            assert res == "2019-07-17 10:51:57"
        except:
            assert False
        else:
            assert True

    def test_toDate_successful_with_list(self):
        input_record = {
            "field_1": "2019-07-17 10:51:57.0"
        }

        try:
            transform_function = BuiltinFunctions.toDate("[%Y-%m-%d %H:%M:%S,%Y-%m-%d %H:%M:%S.%f]")
            res = transform_function(input_record["field_1"], input_record, None, {})
            assert str(res) == "2019-07-17 10:51:57"
        except:
            assert False
        else:
            assert True

    def test_toDate_unsuccessful_with_list(self):
        input_record = {
            "field_1": "2019-07-17"
        }

        try:
            transform_function = BuiltinFunctions.toDate("[%Y-%m-%d %H:%M:%S,%Y-%m-%d %H:%M:%S.%f]")
            res = transform_function(input_record["field_1"], input_record, None, {})
        except:
            assert True
        else:
            assert False

    def test_toDate_successful_with_a_date(self):
        input_record = {
            "field_1": "2019-07-17"
        }

        try:
            transform_function = BuiltinFunctions.toDate("[%Y-%m-%d %H:%M:%S,%Y-%m-%d %H:%M:%S.%f,%Y-%m-%d]")
            res = transform_function(input_record["field_1"], input_record, None, {})
            assert res == "2019-07-17 00:00:00"
        except:
            assert False
        else:
            assert True





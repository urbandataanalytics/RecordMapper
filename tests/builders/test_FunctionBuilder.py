import unittest

from RecordMapper.builders import FunctionBuilder
from RecordMapper.builders.FunctionBuilder import InvalidFunctionError

class test_FunctionBuilder(unittest.TestCase):

    def test_builtin_function(self):

        # Arrange
        test_function_name = "copyFrom" 
        args_list = ["field_2"]

        input_record = {
            ("field_1", ): "hola",
            ("field_2", ): 56
        }

        expected_result = 56

        # Act 
        parsed_function = FunctionBuilder.get_builtin_function(test_function_name, args_list)

        
        # Assert
        self.assertEqual(parsed_function(None, input_record, None, {}), expected_result)


    def test_custom_function(self):

        # Arrange
        test_function_name = "tests.custom_functions_for_tests.sum"
        args_list = [5]
        input_record = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_3",): 7
        }

        expected_res = 12

        # Act 
        parsed_function = FunctionBuilder.get_custom_function(test_function_name, args_list)

        # Assert
        self.assertEqual(parsed_function(7, input_record, None, {}), expected_res)

    def test_parsed_function_str(self):

        builtin_function_test = "copyFrom(field_to_copy)"
        custom_function_test = "tests.custom_functions_for_tests.sum(2)"

        input_record = {
            ("field_1",): "hola",
            ("field_to_copy",): 42,
            ("field_3",): 7
        }

        expected_builtin_res = 42
        expected_custom_res = 9

        # Act 
        parsed_builtin_function = FunctionBuilder.parse_function_str(builtin_function_test)
        parsed_custom_function = FunctionBuilder.parse_function_str(custom_function_test)

        # Assert
        self.assertEqual(parsed_builtin_function(7, input_record, None, {}), expected_builtin_res)
        self.assertEqual(parsed_custom_function(7, input_record, None, {}), expected_custom_res)


    
    def test_invalid_function_name(self):

        # Arrange/Assert/act
        with self.assertRaises(InvalidFunctionError) as context:

            FunctionBuilder.parse_function_str("tests.custom_functions_for_tests.nope(5)")
        
        # Assert
        self.assertTrue("Invalid name for a custom function" in str(context.exception))

    def test_invalid_module_name(self):

        # Arrange/Assert/act
        with self.assertRaises(InvalidFunctionError) as context:

            FunctionBuilder.parse_function_str("tests.custom_functions_for_tests_nope.sum(5)")
        
        # Assert
        self.assertTrue("Invalid module for a custom function" in str(context.exception))

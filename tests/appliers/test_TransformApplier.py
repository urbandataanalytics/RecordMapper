import unittest

from RecordMapper.appliers import TransformApplier
from RecordMapper.appliers.TransformApplier import transform_functions
from RecordMapper.types import InvalidTransformException

class test_TransformApplier(unittest.TestCase):
    def test_get_transform_function(self):

        # Arrange
        example_record = {
            "example": 1
        }
        expected_1 = transform_functions.copyFrom("example")(None, example_record, None)
        expected_2 = transform_functions.toNull()(None, example_record, None)

        # Act
        res_1 = TransformApplier.get_transform_function("copyFrom(example)")(None, example_record, None)
        res_2 = TransformApplier.get_transform_function("toNull")(None, example_record, None)

        # Assert
        self.assertEqual(res_1, expected_1)
        self.assertEqual(res_2, expected_2)

    def test_create_transform_schema(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "Example",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_2", "type": "int"},
               {"name": "field_3", "type": "int", "transform": "copyFrom(field_2)"},
               {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]},
               {"name": "field_5", "type": "int", "transform": ["copyFrom(field_2)", "toNull"]}
            ]
        }

        # Act
        (schema_name, parsed_transform_schema) = TransformApplier.create_transform_schema(test_schema)

        # Assert
        without_functions = [
            (field_key, len(function_list), field_type)
            for field_key, (function_list, field_type) in parsed_transform_schema.items()
        ]

        self.assertEqual(schema_name, "Example")
        self.assertListEqual(without_functions, [
           (("field_1",), 0, "string"), 
           (("field_2",), 0, "int"), 
           (("field_3",), 1, "int"), 
           (("field_4",), 1, "int"), 
           (("field_5",), 2, "int")
        ])

    def test_create_transform_schema_with_nested_schemas(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "Example",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_2", "type": "int"},
               {"name": "field_3", "type": "int", "transform": "copyFrom(field_2)"},
               {"name": "field_4", "type": "ExampleNested"}
            ]
        }

        test_nested_schema = {
            "type": "record",
            "name": "ExampleNested",
            "fields": [
               {"name": "nested_field_1", "type": "string"},
               {"name": "nested_field_2", "type": "int", "transform": "copyFrom(field_2)"},
            ]
        }

        # Act
        parsed_transform_schema = TransformApplier(test_schema, test_nested_schema).get_complete_transform_schema("Example")

        without_functions = [
            (field_key, len(function_list), field_type)
            for field_key, (function_list, field_type) in parsed_transform_schema.items()
        ]

        # Assert
        self.assertListEqual(without_functions, [
           (("field_1",), 0, "string"), 
           (("field_2",), 0, "int"), 
           (("field_3",), 1, "int"), 
           (("field_4",), 0, "ExampleNested"), 
           (("field_4", "nested_field_1"), 0, "string"),
           (("field_4", "nested_field_2"), 1, "int")
        ])


    def test_transform_with_one_phase(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_2", "type": "int"},
               {"name": "field_3", "transform": "copyFrom(field_2)", "type": "int"}
            ]
        }

        input_record = {
            "field_1": "hola",
            "field_2": 56
        }

        expected_record = {
            "field_1": "hola",
            "field_2": 56,
            "field_3": 56
        }

        # Act
        res = TransformApplier(test_schema).apply("ExampleBaseSchema", input_record)
        
        # Assert
        self.assertDictEqual(res, expected_record)


    def test_custom_transform_with_one_phase(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_2", "type": "int"},
               {"name": "field_3", "transform": "tests.custom_functions_for_tests.sum(5)", "type": "int"}
            ]
        }

        input_record = {
            "field_1": "hola",
            "field_2": 56,
            "field_3": 7
        }

        expected_record = {
            "field_1": "hola",
            "field_2": 56,
            "field_3": 12
        }

        # Act
        res = TransformApplier(test_schema).apply("ExampleBaseSchema", input_record)
        
        # Assert
        self.assertDictEqual(res, expected_record)

    def test_invalid_transform_function_name(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_3", "transform": "tests.custom_functions_for_tests.nope(5)", "type": "int"}
            ]
        }

        # Assert/act
        with self.assertRaises(InvalidTransformException) as context:

            TransformApplier(test_schema)
        
        self.assertTrue("Invalid name for a custom transform function" in str(context.exception))

    def test_invalid_transform_module_name(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_3", "transform": "tests.custom_functions_for_tests_nope.sum(5)", "type": "int"}
            ]
        }

        with self.assertRaises(InvalidTransformException) as context:

            TransformApplier(test_schema)

    def test_invalid_transform_module_name(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_3", "transform": "sum(5)", "type": "int"}
            ]
        }

        with self.assertRaises(InvalidTransformException) as context:

            TransformApplier(test_schema)
        
        # Assert
        self.assertTrue("Invalid module for a custom transform function" in str(context.exception))

    def test_several_transform_phases(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_2", "type": "int", "transform": [None, "copyFrom(field_3)"]},
               {"name": "field_3", "type": "int", "transform": ["tests.custom_functions_for_tests.sum(1)", "copyFrom(field_2)"]},
               {"name": "field_4", "transform": [None, None, "toNull"], "type": "int"},
               {"name": "field_5", "transform": [None, None, "copyFrom(field_4)"], "type": "int"},
               {"name": "field_6", "transform": "tests.custom_functions_for_tests.sum(4)", "type": "int"}
            ]
        }

        input_record = {
            "field_1": "hola",
            "field_2": 56,
            "field_3": 7,
            "field_4": 78,
            "field_6": 12
        }

        expected_record = {
            "field_1": "hola",
            "field_2": 8,
            "field_3": 56,
            "field_4": None,
            "field_5": 78,
            "field_6": 16 
        }

        # Act
        res = TransformApplier(test_schema).apply("ExampleBaseSchema", input_record)
        
        # Assert
        self.assertDictEqual(res, expected_record)

    def test_transform_with_nested_schemas(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_2", "type": "int", "transform": [None, "copyFrom(field_3)"]},
               {"name": "field_3", "type": "int", "transform": ["tests.custom_functions_for_tests.sum(1)", "copyFrom(field_2)"]},
               {"name": "field_4", "type": "ExampleNestedSchema"}
            ]
        }

        test_nested_schema = {
            "type": "record",
            "name": "ExampleNestedSchema",
            "fields": [
               {"name": "nested_field_1", "type": "string"},
               {"name": "nested_field_2", "type": "int", "transform": [None, "copyFrom(field_3)"]},
               {"name": "nested_field_3", "type": "int", "transform": [None, None, "copyFrom(field_3)", "tests.custom_functions_for_tests.sum(1)"]},
               {"name": "nested_field_4", "type": "int", "transform": ["toNull"]}
            ]
        }

        input_record = {
            "field_1": "hola",
            "field_2": 56,
            "field_3": 7
        }

        expected_record = {
            "field_1": "hola",
            "field_2": 8,
            "field_3": 56,
            "field_4": {
                "nested_field_2": 8,
                "nested_field_3": 57,
                "nested_field_4": None
            }
        }

        # Act
        res = TransformApplier(test_schema, test_nested_schema).apply("ExampleBaseSchema", input_record)
        print("res:", res)
        
        # Assert
        self.assertDictEqual(res, expected_record)


    #     # Arrange
    #     test_schema = {
    #         "type": "record",
    #         "name": "ExampleBaseSchema",
    #         "fields": [
    #            {"name": "field_1", "type": "string"},
    #            {"name": "field_renamed", "aliases": ["field_2"], "type": "int"}
    #         ]
    #     }

    #     input_dict = {
    #         "field_1": "hola",
    #         "field_2": 56,
    #         "unknown_field": "dummyValue"
    #     }
    #     expected_dict = {
    #         "field_1": "hola",
    #         "field_renamed": 56,
    #         "unknown_field": "dummyValue"
    #     }

    #     # Act
    #     res = RenameApplier(test_schema).apply("ExampleBaseSchema", input_dict)
        
    #     # Assert
    #     self.assertDictEqual(res, expected_dict)

    # def test_nested_renaming(self):

    #     # Arrange
    #     test_schema = {
    #         "type": "record",
    #         "name": "TestRecord",
    #         "fields": [
    #            {"name": "field_1", "type": "string"},
    #            {"name": "field_renamed", "aliases": ["field_2"], "type": "int"},
    #            {"name": "nested_record", "type": "TestNestedRecord"}
    #         ]
    #     }

    #     test_nested_schema = {
    #         "type": "record",
    #         "name": "TestNestedRecord",
    #         "fields": [
    #            {"name": "inner_field_1", "type": "string"},
    #            {"name": "inner_field_renamed", "aliases": ["inner_field_2"], "type": "int"}
    #         ]
    #     }

    #     input_dict = {
    #         "field_1": "hola",
    #         "field_2": 56,
    #         "nested_record": {
    #             "inner_field_1": "adios", 
    #             "inner_field_2" : 32
    #         }
    #     }

    #     expected_dict = {
    #         "field_1": "hola",
    #         "field_renamed": 56,
    #         "nested_record": {
    #             "inner_field_1": "adios", 
    #             "inner_field_renamed" : 32
    #         }
    #     }

    #     # Act
    #     res = RenameApplier(test_schema, test_nested_schema).apply("TestRecord", input_dict) 

    #     # Assert
    #     self.assertDictEqual(res, expected_dict)
    
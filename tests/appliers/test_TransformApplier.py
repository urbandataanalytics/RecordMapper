import unittest

from RecordMapper.appliers import TransformApplier
from RecordMapper.appliers.TransformApplier import transform_functions

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
               {"name": "field_3", "transform": "copyFrom(field_2)"},
               {"name": "field_4", "transform": ["copyFrom(field_2)"]},
               {"name": "field_5", "transform": ["copyFrom(field_2)", "toNull"]}
            ]
        }

        # Act
        (schema_name, parsed_dict) = TransformApplier.create_transform_schema(test_schema)

        # Assert
        self.assertEqual(schema_name, "Example")
        self.assertEqual(len(parsed_dict["field_1"]), 0)
        self.assertEqual(len(parsed_dict["field_2"]), 0)
        self.assertEqual(len(parsed_dict["field_3"]), 1)
        self.assertEqual(len(parsed_dict["field_4"]), 1)
        self.assertEqual(len(parsed_dict["field_5"]), 2)


    # def test_basic_renaming(self):

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
    #         "field_2": 56
    #     }
    #     expected_dict = {
    #         "field_1": "hola",
    #         "field_renamed": 56 
    #     }

    #     # Act
    #     res = RenameApplier(test_schema).apply("ExampleBaseSchema", input_dict)
        
    #     # Assert
    #     self.assertDictEqual(res, expected_dict)

    # def test_retain_fields_that_are_not_in_the_schema(self):

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
    
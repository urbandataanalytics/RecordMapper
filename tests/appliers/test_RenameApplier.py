import unittest

from RecordMapper.appliers import RenameApplier


class test_RenameApplier(unittest.TestCase):

    def test_create_rename_dict(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "Example",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_renamed", "aliases": ["field_2"], "type": "int"}
            ]
        }

        expected_dict = {
            "field_1": ("field_1", "string"),
            "field_2": ("field_renamed", "int"),
            "field_renamed": ("field_renamed", "int")
        }

        # Act
        (schema_name, parsed_dict) = RenameApplier.create_rename_dict(test_schema)

        print("parsed_dict:", parsed_dict)

        # Assert
        self.assertEqual(schema_name, "Example")
        self.assertDictEqual(parsed_dict, expected_dict)


    def test_basic_renaming(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_renamed", "aliases": ["field_2"], "type": "int"}
            ]
        }

        input_dict = {
            "field_1": "hola",
            "field_2": 56
        }
        expected_dict = {
            "field_1": "hola",
            "field_renamed": 56 
        }

        # Act
        res = RenameApplier(test_schema).apply("ExampleBaseSchema", input_dict)
        
        # Assert
        self.assertDictEqual(res, expected_dict)

    def test_retain_fields_that_are_not_in_the_schema(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "ExampleBaseSchema",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_renamed", "aliases": ["field_2"], "type": "int"}
            ]
        }

        input_dict = {
            "field_1": "hola",
            "field_2": 56,
            "unknown_field": "dummyValue"
        }
        expected_dict = {
            "field_1": "hola",
            "field_renamed": 56,
            "unknown_field": "dummyValue"
        }

        # Act
        res = RenameApplier(test_schema).apply("ExampleBaseSchema", input_dict)
        
        # Assert
        self.assertDictEqual(res, expected_dict)

    def test_nested_renaming(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
               {"name": "field_1", "type": "string"},
               {"name": "field_renamed", "aliases": ["field_2"], "type": "int"},
               {"name": "nested_record", "type": "TestNestedRecord"}
            ]
        }

        test_nested_schema = {
            "type": "record",
            "name": "TestNestedRecord",
            "fields": [
               {"name": "inner_field_1", "type": "string"},
               {"name": "inner_field_renamed", "aliases": ["inner_field_2"], "type": "int"}
            ]
        }

        input_dict = {
            "field_1": "hola",
            "field_2": 56,
            "nested_record": {
                "inner_field_1": "adios", 
                "inner_field_2" : 32
            }
        }

        expected_dict = {
            "field_1": "hola",
            "field_renamed": 56,
            "nested_record": {
                "inner_field_1": "adios", 
                "inner_field_renamed" : 32
            }
        }

        # Act
        res = RenameApplier(test_schema, test_nested_schema).apply("TestRecord", input_dict) 

        # Assert
        self.assertDictEqual(res, expected_dict)





    

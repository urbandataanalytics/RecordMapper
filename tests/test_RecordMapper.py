import unittest

from RecordMapper import RecordMapper


class test_RecordMapper(unittest.TestCase):

    def test_transform_record(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
               {"name": "field_1", "type": "string", "aliases": ["another_field"]},
               {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"]},
               {"name": "field_3", "type": ["string", "null"]},
               {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]},
               {"name": "field_5", "type": "int", "transform": ["copyFrom(field_2)", "toNull"]}
            ]
        }

        input_record = {
            "field_auxiliar" : 21
        }

        expected_record = {
            "field_2": 21,
            "field_4": 21,
            "field_5": None
        }

        res_record = RecordMapper([test_schema]).transform_record(input_record, "TestSchema")

        self.assertDictEqual(res_record, expected_record)

class test_RecordMapper_with_nested_schema(unittest.TestCase):

    def test_transform_record(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
               {"name": "field_1", "type": "string", "aliases": ["another_field"]},
               {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"]},
               {"name": "field_3", "type": ["string", "null"]},
               {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]},
               {"name": "field_5", "type": "TestNestedSchema", "nestedSchemaSelector": "tests.custom_functions_for_tests.selectSchema(TestNestedSchema)"}
            ]
        }

        test_nested_schema =  {
            "type": "record",
            "name": "TestNestedSchema",
            "fields": [
               {"name": "nested_field_1", "type": "string"},
               {"name": "nested_field_2", "type": ["string", "null"], "transform": "copyFrom(field_3)"}
            ]
        }

        input_record = {
            "field_auxiliar" : 21,
            "field_3": "hola"
        }

        expected_record = {
            "field_2": 21,
            "field_3": "hola",
            "field_4": 21,
            "field_5": {
                "nested_field_2": "hola"
            }
        }

        res_record = RecordMapper([test_schema, test_nested_schema]).transform_record(input_record, "TestSchema")

        self.assertDictEqual(res_record, expected_record)
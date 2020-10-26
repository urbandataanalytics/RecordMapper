import unittest
import tempfile
import os

from RecordMapper.avro import AvroWriter
from RecordMapper.avro import AvroReader

class test_AvroWriter(unittest.TestCase):


    def test_record_with_a_simple_schema(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
               {"name": "field_1", "type": ["string", "null"], "aliases": ["another_field"]},
               {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"]},
               {"name": "field_3", "type": ["string", "null"]},
               {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]}
            ]
        }

        test_records = [
            {"field_1": "campo 11", "field_2": 10, "field_3": "campo_3", "field_4": 5},
            {"field_2": 10, "field_3": "campo_32", "field_4": 5},
            {"field_1": "campo 1", "field_3": "campo_3", "field_4": 5},
        ]

        expected_records = [
            {"field_1": "campo 11", "field_2": 10, "field_3": "campo_3", "field_4": 5},
            {"field_1": None, "field_2": 10, "field_3": "campo_32", "field_4": 5},
            {"field_1": "campo 1", "field_2": None, "field_3": "campo_3", "field_4": 5}
        ]

        # act 
        temp_file = tempfile.NamedTemporaryFile("w", delete=False)

        writer = AvroWriter(temp_file.name, test_schema)
        output_opts = {}
        writer.write_records(test_records, output_opts)
        writer.close()

        reader = AvroReader(temp_file.name)
        res_records = [record for record in reader.read_records()]
        reader.close()
        
        os.remove(temp_file.name)

        # Assert
        self.assertListEqual(res_records, expected_records)

    def test_record_with_a_complex_schema(self):

        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
               {"name": "field_1", "type": ["string", "null"], "aliases": ["another_field"]},
               {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"]},
               {"name": "field_3", "type": ["string", "null"]},
               {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]},
               {"name": "field_with_nested_schema", "type": ["NestedSchema", "null"]}
            ]
        }

        test_nested_schema =  {
            "type": "record",
            "name": "NestedSchema",
            "fields": [
               {"name": "nested_field_1", "transform": [None, "copyFrom(field_4)"], "type": ["int", "null"]},
               {"name": "nested_field_2",  "type": ["int", "null"]}
            ]
        }

        test_records = [
            {
                "field_1": "campo 11",
                "field_2": 10,
                "field_3": "campo_3",
                "field_4": 5
            },
            {
                "field_2": 10,
                "field_3": "campo_32",
                "field_4": 5
            },
            {
                "field_1": "campo 1",
                "field_3": "campo_3",
                "field_4": 5,
                "field_with_nested_schema":{
                    "nested_field_1": 5

                }
            }
        ]

        expected_records = [
            {
                "field_1": "campo 11",
                "field_2": 10,
                "field_3": "campo_3",
                "field_4": 5,
                "field_with_nested_schema": None
            },
            {
                "field_1": None,
                "field_2": 10,
                "field_3": "campo_32",
                "field_4": 5,
                "field_with_nested_schema": None
            },
            {
                "field_1": "campo 1",
                "field_2": None,
                "field_3": "campo_3",
                "field_4": 5,
                "field_with_nested_schema":
                {
                    "nested_field_1": 5,
                    "nested_field_2": None
                }
            }
        ]

        # act 
        temp_file = tempfile.NamedTemporaryFile("w", delete=False)

        writer = AvroWriter(temp_file.name, test_schema, [test_nested_schema])
        output_opts = {"merge_schemas": True}
        writer.write_records(test_records, output_opts)
        writer.close()

        reader = AvroReader(temp_file.name)
        res_records = [record for record in reader.read_records()]
        reader.close()
        
        os.remove(temp_file.name)

        # Assert
        self.assertListEqual(res_records, expected_records)

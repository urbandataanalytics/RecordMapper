import unittest
import tempfile
import os

import fastavro

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

        with open(temp_file.name, "wb") as f:
            writer = AvroWriter(f,test_schema)
            writer.write_records(test_records) 
            writer.close()

        with open(temp_file.name, "rb") as f:
            reader = AvroReader(f)
            res_records = [record for record in reader.read_records()]
        
        os.remove(temp_file.name)

        # Assert
        self.assertListEqual(res_records, expected_records)

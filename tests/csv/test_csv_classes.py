import unittest
import tempfile
import os

import fastavro

from RecordMapper.csv import CSVWriter
from RecordMapper.csv import CSVReader

class test_AvroWriter(unittest.TestCase):


    def test_record_with_a_simple_schema(self):

        test_records = [
            {"field_1": "campo 11", "field_2": 10, "field_3": "campo_3", "field_4": 5},
            {"field_2": 10, "field_3": "campo_32", "field_4": 5},
            {"field_1": "campo 1", "field_3": "campo_3", "field_4": 5},
        ]

        # act 
        temp_file = tempfile.NamedTemporaryFile("w", delete=False)

        with open(temp_file.name, "w") as f:
            writer = CSVWriter(f, fieldnames=["field_1", "field_2", "field_3", "field_4"])
            writer.write_records(test_records) 
            writer.close()

        with open(temp_file.name, "r") as f:
            reader = CSVReader(f)
            res_records = [record for record in reader.read_records()]
        
        os.remove(temp_file.name)

        # Assert
        self.assertListEqual(test_records, test_records) # Without changes

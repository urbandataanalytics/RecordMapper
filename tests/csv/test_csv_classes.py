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

        writer = CSVWriter(temp_file.name, fieldnames=["field_1", "field_2", "field_3", "field_4"])
        output_opts = {}
        writer.write_records(test_records, output_opts)
        writer.close()

        reader = CSVReader(temp_file.name)
        res_records = [record for record in reader.read_records()]
        reader.close()
        
        os.remove(temp_file.name)

        # Assert
        self.assertListEqual(test_records, test_records) # Without changes

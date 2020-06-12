import csv
import json
from typing import BinaryIO, List

from RecordMapper.common import Writer

class CSVWriter(Writer):

    def __init__(self, object_to_write: object, fieldnames: List[str]):

        super().__init__(object_to_write)
        self.fieldnames = fieldnames

    def write_records_to_output(self, records: List[dict], output: BinaryIO):

        csv_writer = csv.DictWriter(output, fieldnames=self.fieldnames)
        csv_writer.writeheader()

        for record in records:
            csv_writer.writerow(self.format_record(record))
    
    def format_record(self, record: dict):

        record_to_write = {**record}

        for key, value in record.items():
            if isinstance(value, dict):
                record_to_write[key] = json.dumps(value)

        return record_to_write

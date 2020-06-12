import csv
import json
from typing import BinaryIO, List


class CSVWriter(object):

    def __init__(self, output_stream: BinaryIO, fieldnames: List[str]):

        self.csv_writer = csv.DictWriter(output_stream, fieldnames=fieldnames)
        self.csv_writer.writeheader()

    def write_records(self, record_list: List[dict]):

        for record in record_list:
            self.write_record(record)
    
    def write_record(self, record: dict):

        record_to_write = {**record}


        for key, value in record.items():
            if isinstance(value, dict):
                record_to_write[key] = json.dumps(value)

        self.csv_writer.writerow(record_to_write)
    
    def close(self):
        pass
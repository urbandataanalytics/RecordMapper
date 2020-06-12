import csv
from typing import BinaryIO


from RecordMapper.common import Reader

class CSVReader(Reader):

    def __init__(self, input: object):

        super().__init__(input)
        self.filepath = input

    def read_records_from_input(self, input_stream: BinaryIO):

        self.reader = csv.DictReader(input_stream)

        for record in self.reader:
            formatted_record = dict([(key, value if value != '' else None) for key, value in record.items()])
            yield formatted_record

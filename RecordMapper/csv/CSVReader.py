import csv
from typing import BinaryIO

class CSVReader(object):

    def __init__(self, input_stream: BinaryIO):

        self.reader = csv.DictReader(input_stream)

    def read_records(self):

        for record in self.reader:
            yield record

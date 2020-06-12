from typing import BinaryIO

import fastavro

class AvroReader(object):

    def __init__(self, input_stream: BinaryIO):

        self.reader = fastavro.reader(input_stream)

    def read_records(self):

        for record in self.reader:
            yield record


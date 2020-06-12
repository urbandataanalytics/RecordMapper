from typing import BinaryIO

import fastavro

from RecordMapper.common import Reader

class AvroReader(Reader):

    def __init__(self, path_to_read: BinaryIO):

        super().__init__(path_to_read)

        self.read_options ="rb"

    def read_records_from_input(self, input_stream: BinaryIO):

        for record in fastavro.reader(input_stream):
            yield record


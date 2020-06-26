from typing import BinaryIO, Iterator

import fastavro

from RecordMapper.common import Reader

class AvroReader(Reader):
    """A Record reader from Avro format.
    """

    def __init__(self, path_to_read: str):
        """Constructor of the AvroRecord.

        :param path_to_read: Path of the file to read.
        :type path_to_read: string
        """

        super().__init__(path_to_read)

        self.read_options ="rb"

    def read_records_from_input(self, input_stream: BinaryIO) -> Iterator[dict]:
        """Read records from an input stream in avro format.

        :param input_stream: The input stream.
        :type input_stream: BinaryIO
        :yield: A record.
        :rtype: Iterator[dict]
        """

        for record in fastavro.reader(input_stream):
            yield record


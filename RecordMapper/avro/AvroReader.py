from typing import BinaryIO, Iterator

import fastavro

from RecordMapper.common import Reader


class AvroReader(Reader):
    """A Record reader for Avro format."""

    def __init__(self, file_path: str):
        """Constructor of the AvroRecord.

        :param file_path: Path of the file to read.
        :type file_path: string
        """

        super().__init__(file_path)
        self.read_options = "rb"
        self.reader = None

    def read_records_from_input(self, input_stream: BinaryIO) -> Iterator[dict]:
        """Read records from an input stream in avro format.

        :param input_stream: The input stream.
        :type input_stream: BinaryIO
        :yield: A record.
        :rtype: Iterator[dict]
        """

        self.reader = fastavro.reader(input_stream)

        for record in self.reader:
            yield record


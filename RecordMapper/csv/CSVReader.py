import csv
from typing import BinaryIO, Iterator

from RecordMapper.common import Reader


csv.field_size_limit(100_000_000)


class CSVReader(Reader):
    """A Record reader for csv format."""

    def __init__(self, file_path: str):
        """The constructor of the CSVReader.

        :param file_path: Path of the file to read
        :type file_path: str
        """

        super().__init__(file_path)
        self.reader = None

    def read_records_from_input(self, input_stream: BinaryIO) -> Iterator[dict]:
        """Read records from an input stream in csv format.

        :param input_stream: The input stream of the records.
        :type input_stream: BinaryIO
        :yield: A record.
        :rtype: Iterator[dict]
        """

        self.reader = csv.DictReader(input_stream)

        for record in self.reader:
            formatted_record = dict([(key, value if value != '' else None) for key, value in record.items()])
            yield formatted_record

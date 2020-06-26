import csv
from typing import BinaryIO, Iterator

from RecordMapper.common import Reader

class CSVReader(Reader):
    """A object that reads records form an input csv file.
    """

    def __init__(self, file_path: str):
        """The constructor of the CSVReader.

        :param input: The path of the file.
        :type input: str
        """

        super().__init__(file_path)
        self.filepath = file_path

    def read_records_from_input(self, input_stream: BinaryIO) -> Iterator[dict]:
        """The function that reads the records from the input stream.

        :param input_stream: The input stream of the records.
        :type input_stream: BinaryIO
        :yield: dict
        :rtype: 
        """

        self.reader = csv.DictReader(input_stream)

        for record in self.reader:
            formatted_record = dict([(key, value if value != '' else None) for key, value in record.items()])
            yield formatted_record

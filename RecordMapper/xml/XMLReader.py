from typing import BinaryIO, Iterator

from defusedxml.ElementTree import parse as parse_xml

from RecordMapper.common import Reader


class XMLReader(Reader):
    """A Record reader for xml format."""

    def __init__(self, file_path: str):
        """The constructor of the XMLReader.

        :param file_path: Path of the file to read.
        :type file_path: str
        """

        super().__init__(file_path)
        self.reader = None

    def read_records_from_input(self, input_stream: BinaryIO) -> Iterator[dict]:
        """Read records from an input stream in xml format.

        :param input_stream: The input stream of the records.
        :type input_stream: BinaryIO
        :yield: A record.
        :rtype: Iterator[dict]
        """

        self.reader = parse_xml(input_stream).getroot()

        for record in self.reader:
            yield {attribute.tag: attribute.text for attribute in record}

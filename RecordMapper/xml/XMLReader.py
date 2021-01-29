from typing import BinaryIO, Iterator

from defusedxml.ElementTree import parse as parse_xml

from RecordMapper.common import Reader

class XMLReader(Reader):
    """A object that reads records form an input xml file.
    """

    def __init__(self, file_path: str):
        """The constructor of the XMLReader.

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

        self.reader = parse_xml(input_stream).getroot()

        for record in self.reader:
            yield {attribute.tag : attribute.text for attribute in record}

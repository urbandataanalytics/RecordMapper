from typing import BinaryIO, Iterator


class Reader(object):
    """A generic Reader object."""

    def __init__(self, file_path: str):
        """The constructor method.

        :param file_path: Path of the file to read.
        :type file_path: str
        """

        self.file_path = file_path
        self.read_options = "r"
        self.input_stream = None

    def read_records(self) -> Iterator[dict]:
        """Open the file and read the records.  

        :return: Returns a generator of records.
        :rtype: Iterator[dict] 
        """

        self.input_stream = open(self.file_path, self.read_options)

        return self.read_records_from_input(self.input_stream)

    def read_records_from_input(self, input_stream: BinaryIO) -> Iterator[dict]:
        """Read from the input object and return a generator of records.

        This method has to be implemented by a child class.

        :param input_stream: An input stream.
        :type input_stream: BinaryIO.
        :raises NotImplementedError: The method has to be implemented by a child class.
        :yield: A read record.
        :rtype: Iterator[dict]
        """

        raise NotImplementedError("This method has to be implemented to read formatted records!")

    def close(self):
        """Close the input stream."""

        self.input_stream.close()

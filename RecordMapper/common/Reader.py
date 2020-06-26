from typing import BinaryIO, Iterator


class Reader(object):
    """A generic Reader object.
    """

    def __init__(self, path_to_read: str):
        """The constructor function.

        :param path_to_read: The path to read. 
        :type path_to_read: str
        """

        self.file_path = path_to_read
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
        """This function reads from the input object and returns a generator of records.
        This has to be implemented by a child class.

        :param input_stream: An input stream.
        :type input_stream: BinaryIO.
        :raises NotImplementedError: This function has to be implemented by a child class.
        :yield: A read record.
        :rtype: Iterator[dict]
        """

        raise NotImplementedError("This method has to be implemented to read formatted records!")

    def close(self):
        """This method close the input stream.
        """

        self.input_stream.close()
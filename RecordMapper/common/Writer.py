from typing import BinaryIO, Iterable


class Writer(object):
    """The generic writer class.
    """

    def __init__(self, file_path: str):
        """The constructor function.

        :param file_path: The path of the input file. 
        :type file_path: str
        """

        self.file_path = file_path
        self.write_options = "w"

        self.output_stream = None

    def write_records(self, records: Iterable, output_opts: dict):
        """This function writes the records to the output file.

        :param records: An iterable of records.
        :type records: Iterable
        :return: A generator of records.
        :rtype: Iterator[dict]
        """

        self.output_stream = open(self.file_path, self.write_options)

        return self.write_records_to_output(records, self.output_stream, output_opts)

    def write_records_to_output(self, records: Iterable, output: BinaryIO, output_opts: dict):
        """Write the records to an output file. This function has to be implemented by a
        child class.

        :param records: An iterable of records.
        :type records: Iterable
        :param output: An output stream.
        :type output: BinaryIO
        :raises NotImplementedError: This function has to be implemented by a child class.
        """

        raise NotImplementedError("This method has to be implemented to write formatted records!")

    def close(self):
        """This function close the output stream.
        """

        self.output_stream.close()

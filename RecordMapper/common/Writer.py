from typing import BinaryIO, Iterable


class Writer(object):
    """The generic writer class."""

    def __init__(self, file_path: str):
        """The constructor method.

        :param file_path: Path of the input file.
        :type file_path: str
        """

        self.file_path = file_path
        self.write_options = "w"
        self.output_stream = None

    def write_records(self, records: Iterable, output_opts: dict):
        """Write the records to the output file.

        Include a set of options to modify the writing process.

        :param records: An iterable of records.
        :type records: Iterable
        :param output_opts: A dict-like set of options to be able to handle the
            behaviour of the output.
        :type output_opts: dict
        :return: A generator of records.
        :rtype: Iterator[dict]
        """

        self.output_stream = open(self.file_path, self.write_options)

        return self.write_records_to_output(records, self.output_stream, output_opts)

    def write_records_to_output(self, records: Iterable, output: BinaryIO, output_opts: dict):
        """Write the records to an output file.

        This method has to be implemented by a child class.

        :param records: An iterable of records.
        :type records: Iterable
        :param output: An output stream.
        :type output: BinaryIO
        :param output_opts: A dict-like set of options to be able to handle the
            behaviour of the output.
        :type output_opts: dict
        :raises NotImplementedError: The method has to be implemented by a child class.
        """

        raise NotImplementedError("This method has to be implemented to write formatted records!")

    def close(self):
        """Close the output stream."""

        self.output_stream.close()

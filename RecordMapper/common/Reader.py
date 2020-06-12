from typing import BinaryIO


class Reader(object):

    def __init__(self, path_to_read: str):

        self.file_path = path_to_read
        self.read_options = "r"
        self.input_stream = None

    def read_records(self):

        self.input_stream = open(self.file_path, self.read_options)

        return self.read_records_from_input(self.input_stream)

    def read_records_from_input(self, input_stream: BinaryIO):

        raise NotImplementedError("This method has to be implemented to read formatted records!")

    def close(self):

        self.input_stream.close()
from typing import BinaryIO,Iterable


class Writer(object):

    def __init__(self, object_to_write: object):

        self.file_path = object_to_write
        self.write_options = "w"

        self.output_stream = None

    def write_records(self, records: Iterable):

        self.output_stream = open(self.file_path, self.write_options)

        return self.write_records_to_output(records, self.output_stream)

    def write_records_to_output(self, records: Iterable, output: BinaryIO):

        raise NotImplementedError("This method has to be implemented to write formatted records!")

    def close(self):

        self.output_stream.close()


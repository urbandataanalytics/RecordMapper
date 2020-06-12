from typing import BinaryIO, List, Iterable

import fastavro


class AvroMatchingException(Exception):
    """
        Exception when a row is writed to an avro file and it doesn't match with the schema.
    """
    pass


class AvroWriter(object):
    """This object create a writer that writes avro data into a file-like object.
    """

    def __init__(self, output_stream: BinaryIO, base_schema: dict, nested_schemas: List[dict] = []):
        """AvroWriter constructor
        
        :param output_stream: The file-like object where data will be writed.
        :type output_stream: file
        :param avro_schema: A valid avro schema as a dict.
        :type avro_schema: dict
        """

        self.parsed_nested_schemas = [fastavro.parse_schema(schema) for schema in nested_schemas]
        self.parsed_base_schema = fastavro.parse_schema(base_schema) 

        self.writer = fastavro.write.Writer(output_stream, self.parsed_base_schema)

    def write_record(self, record: dict):
        """A method to write an Avro row to the output_stream.
        
        :param row: A row of data that matchs with the current schema.
        :type row: dict
        :raises AvroMatchingException: When the provided row doesn't match with the current schema.
        """

        try:
            self.writer.write(record)
        except ValueError as ex:
            raise AvroMatchingException(f"Exception: {ex} for row -> {record}")

    def write_records(self, record_list: Iterable):

        for record in record_list:
            self.write_record(record)

    def close(self):
        """Sends the buffer reamining data and closes the output stream
        """

        self.writer.flush()
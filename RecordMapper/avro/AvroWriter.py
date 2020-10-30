from typing import BinaryIO, List, Iterable

import fastavro

from RecordMapper.common import Writer


class AvroMatchingException(Exception):
    """
        Exception when a row is writed to an avro file and it doesn't match with the schema.
    """
    pass


class AvroWriter(Writer):
    """This object create a writer that writes avro data into a file-like object.
    """

    def __init__(self, obj_to_write: object, base_schema: dict, nested_schemas: List[dict] = [], output_opts: dict = {}):
        """AvroWriter constructor

        :param output_stream: The file-like object where data will be writed.
        :type output_stream: file
        :param avro_schema: A valid avro schema as a dict.
        :type avro_schema: dict
        """

        super().__init__(obj_to_write)
        self.write_options = "wb"
        self.write_count = 0

        self.parsed_nested_schemas = [fastavro.parse_schema(schema) for schema in nested_schemas]

        if output_opts.get('merge_schemas', False):
            self.parsed_base_schema = self.merge_schemas(base_schema, nested_schemas)

        else:
            self.parsed_base_schema = fastavro.parse_schema(base_schema)



    def write_records_to_output(self, record_list: Iterable, output: BinaryIO, output_opts: dict):

        # As fastavro is not able to use nested schemas, we must combine the base schema and the nested schema
        self.writer = fastavro.write.Writer(output, self.parsed_base_schema)

        for record in record_list:
            try:
                self.writer.write(record)
                self.write_count += 1
            except ValueError as ex:
                raise AvroMatchingException(f"Exception: {ex} for row -> {record}")

    def close(self):
        """Sends the buffer reamining data and closes the output stream
        """

        self.writer.flush()
        super().close()

    def merge_schemas(self, base_schema: dict, nested_schemas: List[dict]):

        nested_schemas_dict = {nested_schema['name']: nested_schema for nested_schema in nested_schemas}

        res_schema = {**base_schema}

        for field in res_schema['fields']:
            type_list = field['type'] if isinstance(field['type'], list) else [field['type']]

            field['type'] = [nested_schemas_dict[value] if value in nested_schemas_dict else value
                             for value in type_list]

        return res_schema
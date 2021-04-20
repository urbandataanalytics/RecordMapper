from typing import BinaryIO, List, Iterable

import fastavro

from RecordMapper.common import Writer


class AvroMatchingException(Exception):
    """
        Exception launched when a row is written to an avro file and it doesn't match the schema.
    """
    pass


class AvroWriter(Writer):
    """A Record writer for Avro format."""

    def __init__(self, obj_to_write: object, base_schema: dict, nested_schemas: List[dict] = [], output_opts: dict = {}):
        """AvroWriter constructor

        :param obj_to_write: The file-like object where data will be written.
        :type obj_to_write: file
        :param base_schema: A valid avro schema as a dict.
        :type base_schema: dict
        :param nested_schemas: A valid avro schema as a dict.
        :type nested_schemas: dict
        :param output_opts: A dict-like set of options to be able to handle the
            behaviour of the output.
        :type output_opts: dict
        """

        super().__init__(obj_to_write)
        self.write_options = "wb"
        self.write_count = 0
        self.parsed_nested_schemas = [fastavro.parse_schema(schema) for schema in nested_schemas]
        self.writer = None

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

    def merge_schemas(self, base_schema: dict, nested_schemas: List[dict]):

        nested_schemas_dict = {nested_schema['name']: nested_schema for nested_schema in nested_schemas}

        res_schema = {**base_schema}

        for field in res_schema['fields']:
            type_list = field['type'] if isinstance(field['type'], list) else [field['type']]

            field['type'] = [nested_schemas_dict[value] if value in nested_schemas_dict else value
                             for value in type_list]

        return res_schema

    def close(self):
        """Send the buffer remaining data and close the output stream."""

        self.writer.flush()
        super().close()

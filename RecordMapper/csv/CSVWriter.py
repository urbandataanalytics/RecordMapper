import csv
import json
from typing import BinaryIO, List, Iterable

from RecordMapper.common import Writer


class CSVWriter(Writer):
    """A Record reader for csv format."""

    def __init__(self, file_path: str, fieldnames: List[str]):
        """The constructor of the CSVWriter.

        :param file_path: Path to write.
        :type file_path: str
        :param fieldnames: List of names of the columns. Their order will
            be preserved in the results file.
        :type fieldnames: List[str]
        """

        super().__init__(file_path)
        self.fieldnames = fieldnames
        self.write_count = 0

    def write_records_to_output(self, records: Iterable[dict], output: BinaryIO, output_opts: dict):
        """Write the records to the output file.

        As output options, we can include the flatting of the nested schemas.

        :param records: An iterable of records.
        :type records: List[dict]
        :param output: The object to write.
        :type output: BinaryIO
        :param output_opts: A dict-like set of options to be able to handle the
            behaviour of the output.
        :type output_opts: dict
        """

        fields_to_flat = set(output_opts.get("flat_nested_schema_on_csv", dict()).keys())

        available_fieldnames = [
            fieldname
            for fieldname in self.fieldnames
            if fieldname not in fields_to_flat
        ]

        csv_writer = csv.DictWriter(output, fieldnames=available_fieldnames)
        csv_writer.writeheader()

        for record in records:
            csv_writer.writerow(self.format_record(record, fields_to_flat))
            self.write_count += 1

    def format_record(self, record: dict, fields_to_flat: set) -> dict:
        """Format a record to be written.

        For example, dicts will be serialized to JSON strings.

        :param record: A input record.
        :type record: dict
        :param fields_to_flat: A set of fields that will be flattened.
        :type fields_to_flat: set
        :return: A formatted record to be written.
        :rtype: dict
        """

        if fields_to_flat:
            record = self.prepare_nested_fields_to_flatten(record, fields_to_flat)

        record_to_write = {**record}

        for key, value in record.items():
            if isinstance(value, dict):
                if key in fields_to_flat:
                    record_to_write = {**record_to_write, **value}
                    del record_to_write[key]
                else:
                    record_to_write[key] = json.dumps(value)

        return record_to_write

    def prepare_nested_fields_to_flatten(self, record: dict, fields_to_flat: set) -> dict:
        """Prepare the nested fields to be flattened in record formatting.

        To properly format a record with nested fields, they must
        be Python dictionary.

        :param record: The input record.
        :type record: dict
        :param fields_to_flat: A set of fields that will be flattened.
        :type fields_to_flat: set
        :return: The record after corrections.
        :rtype: dict
        """

        for field_to_flat in fields_to_flat:
            if field_to_flat in record:
                record[field_to_flat] = record[field_to_flat] if record[field_to_flat] else {}

        return record

import csv
import json
from typing import BinaryIO, List, Iterable

from RecordMapper.common import Writer


class CSVWriter(Writer):
    """The object that writes records to a csv file.
    """

    def __init__(self, file_path: str, fieldnames: List[str]):
        """The constructor of the CSVWriter.

        :param file_path: The path to write.
        :type file_path: object.
        :param fieldnames: The list of names of the columns.
            Their order will be preserved in the result file.
        :type fieldnames: List[str]
        """

        super().__init__(file_path)
        self.fieldnames = fieldnames
        self.write_count = 0

    def write_records_to_output(self, records: Iterable[dict], output: BinaryIO, output_opts: dict):
        """Writes the records to an output file.

        :param records: An iterable of records.
        :type records: List[dict]
        :param output: The object to write.
        :type output: BinaryIO
        """

        csv_writer = csv.DictWriter(output, fieldnames=self.fieldnames)
        csv_writer.writeheader()

        for record in records:
            csv_writer.writerow(self.format_record(record, output_opts))
            self.write_count += 1

    def format_record(self, record: dict, output_opts: dict) -> dict:
        """Format a record to be written. For example, dicts will be serialized to
        JSON strings.

        :param record: A input record.
        :type record: dict
        :param flatten: Indicates if a dictionary should be writen as regluar fields of the record.
        :type flatten: boolean
        :return: A formatted record to be written.
        :rtype: dict
        """

        record_to_write = {**record}

        fields_to_flat = list(output_opts.get("flat_nested_schema_on_csv", dict()).keys())

        for key, value in record.items():
            if isinstance(value, dict):
                if key in fields_to_flat:
                    record_to_write = {**record_to_write, **value}
                    del record_to_write[key]
                else:
                    record_to_write[key] = json.dumps(value)

        return record_to_write

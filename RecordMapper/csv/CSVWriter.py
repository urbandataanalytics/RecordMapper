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

        flatten = output_opts["flatten_csv"]
        excluded_field_from_flattening = output_opts["excluded_fields_from_flattening"]

        for key, value in record.items():
            if isinstance(value, dict):
                if not flatten or key in excluded_field_from_flattening:
                    record_to_write[key] = json.dumps(value)
                else:
                    record_to_write[key] = None
                    for key2 in value:
                        record_to_write[key2] = value[key2]

        return record_to_write

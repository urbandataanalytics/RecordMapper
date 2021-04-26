import itertools
from typing import List, Iterable, Iterator

from RecordMapper.appliers import NestedSchemaSelectorApplier, RenameApplier, TransformApplier, CleanApplier
from RecordMapper.avro.AvroReader import AvroReader
from RecordMapper.avro.AvroWriter import AvroWriter
from RecordMapper.builders import FlatSchemaBuilder, FlatRecordBuilder
from RecordMapper.csv.CSVReader import CSVReader
from RecordMapper.csv.CSVWriter import CSVWriter
from RecordMapper.xml.XMLReader import XMLReader
from RecordMapper.utils import chain_functions


class RecordMapper(object):
    """The main class of this package.

    Include methods to transform data using an Avro schema and custom functions.
    """

    def __init__(self, base_schema: dict, nested_schemas: List[dict] = [], custom_variables: dict = {}):
        """Constructor method of RecordMapper.

        Initialize the base and nested schemas, and the custom variables.
        Initialize the RecordMapper stats and construct a flattened version
          of the different schemas loaded.
        Initialize a set of appliers that will transform the records:
          - The selector applier, which will modify the base schema if
            there exist nested schemas to consider.
          - The rename applier, which will develop a renaming process using
            the aliases included in the schema.
          - The transform applier which will apply the record transformations
            given the transforming functions.
          - The clean applier, which will filter the output fields to
            keep only the ones given in the output schema.


        :param base_schema: The base schema in Avro format to transform the records.
        :type base_schema: dict
        :param nested_schemas: The schemas of the nested record types. Defaults to [].
        :type nested_schemas: List[dict], optional
        :param custom_variables: A dict of custom variables that will be accessible for the appliers.
        """

        # Load the base and nested schemas, and the custom variables.
        self.original_base_schema = base_schema
        self.original_nested_schemas = nested_schemas
        self.custom_variables = custom_variables

        # Initialize the stats of the Record Mapper.
        self.stats = {}

        # Load flattened schemas for both, base and nested schemas.
        self.flat_schemas = dict(
            [
                (schema["name"], FlatSchemaBuilder.get_flat_schema(schema))
                for schema in ([self.original_base_schema] + self.original_nested_schemas)
            ]
        )

        # Load the appliers.
        self.selector_applier = NestedSchemaSelectorApplier(self.flat_schemas, self.custom_variables)
        self.rename_applier = RenameApplier(self.custom_variables)
        self.transform_applier = TransformApplier(self.custom_variables)
        self.clean_applier = CleanApplier(self.custom_variables)

    def execute(self, input_format: str, input_file_path: str, paths_to_write: dict, input_opts: dict = {},
                base_schema_to_write: dict = None, nested_schemas_to_write: List[dict] = None,
                output_opts: dict = {}):
        """Read, transform and write the data from a format to another one.

        The following input formats are allowed to be read: csv, avro, xml
        The following output formats are allowed to be written: csv, avro

        :param input_format: The input format (csv, avro, xml)
        :type input_format: str
        :param input_file_path: The path of the input file.
        :type input_file_path: str
        :param paths_to_write: A dict with the output formats (avro, csv) and their configuration.
        :type paths_to_write: dict
        :param input_opts: Some special options of the input process. Defaults to {}.
        :type input_opts: dict, optional
        :param base_schema_to_write: A base schema used to write. Defaults to None.
        :type base_schema_to_write: dict, optional
        :param nested_schemas_to_write: A list of nested record schemas used to write. Defaults to None.
        :type nested_schemas_to_write: List[dict], optional
        :param output_opts: A dict-like set of options to be able to handle the behaviour of the output.
        :type output_opts: dict
        """
        
        # Reset the RecordMapper stats.
        self.stats = {}

        # Read, transform and write records.
        read_records = self.read_records(input_format, input_file_path, input_opts)
        transformed_records = self.transform_records(read_records)
        self.write_records(transformed_records, paths_to_write, base_schema_to_write,
                           nested_schemas_to_write, output_opts)

    def transform_records(self, record_list: Iterable[dict]) -> Iterator[dict]:
        """Transform file records using different appliers.

        The method is a generator of transformed records.

        :param record_list: An iterable of records.
        :type record_list: Iterable[dict]
        :yield: A transformed record.
        :rtype: Iterator[dict]
        """

        for record in record_list:
            yield self.transform_record(record)

    def transform_record(self, record: dict) -> dict:
        """Apply transformations on a single record.

        The transforming process includes:
          - Loading the flattened schema.
          - Converting a given record to a convenient flat format.
          - Constructing the transforming pipeline chaining the appliers functions.
          - Applying the record transformation.
          - De-converting the transformed record from the flat format to the original one.
          - Returning the transformed record.

        Each transforming step is defined in its correspondent Applier object.

        :param record: An input record.
        :type record: dict
        :return: A transformed record.
        :rtype: dict
        """

        # Load the flattened schema.
        base_schema_name = self.original_base_schema["name"]
        base_flat_schema = self.flat_schemas[base_schema_name]

        # Convert the record to a flat format.
        flat_record = FlatRecordBuilder.get_flat_record_from_normal_record(record)

        # Construct the transforming pipeline.
        all_functions = chain_functions(
            self.selector_applier.apply,
            self.rename_applier.apply,
            self.transform_applier.apply,
            self.clean_applier.apply
        )

        # Apply the record transformations.
        transformed_record, transformed_flat_schema = all_functions(flat_record, base_flat_schema)

        # Convert the transformed record to the original format.
        normal_record = FlatRecordBuilder.get_normal_record_from_flat_record(transformed_record)

        return normal_record

    def read_records(self, input_format: str, path_to_read: str, opts: dict = {}) -> Iterator[dict]:
        """Read records from an input file.

        :param input_format: The format of the file.
        :type input_format: str
        :param path_to_read: The path of the file.
        :type path_to_read: str
        :param opts: Some special options in the read process. Defaults to {}.
        :type opts: dict, optional
        :raises RuntimeError: The format is not supported by the RecordMapper.
        :yield:
        :rtype: Iteraror[dict]
        """

        self.stats["read_count"] = 0

        if input_format == "avro":
            reader_object = AvroReader(path_to_read)
        elif input_format == "csv":
            reader_object = CSVReader(path_to_read)
        elif input_format == "xml":
            reader_object = XMLReader(path_to_read)
        else:
            raise RuntimeError(f"Invalid input format: {input_format}")

        for record in reader_object.read_records():
            self.stats["read_count"] += 1
            yield record

        reader_object.close()

    def write_records(self, records_list: Iterable, paths_to_write: dict, base_schema_to_write: dict = None,
                      nested_schemas_to_write: List[dict] = None, output_opts: dict = {}):
        """Write transformed records to one or several formats.

        IMPORTANT NOTE:
          The Record Mapper is able to write in both, csv and avro formats.
          But it is always necessary writing an avro file at least.

        :param records_list: An iterable of records.
        :type records_list: Iterable
        :param paths_to_write: A dict with the formats and the paths that will be written.
            For example, {"csv": "example.csv", "avro": "example.avro"}.
        :type paths_to_write: dict
        :param base_schema_to_write: A base schema used in the writing process, different
            from the one used in the transform process. Defaults to None.
        :type base_schema_to_write: dict, optional
        :param nested_schemas_to_write: A list of nested schemas used in the write process,
            different from the one used in the transform process. Defaults to None.
        :type nested_schemas_to_write: List[dict], optional
        :param output_opts: A dict-like set of options to be able to handle the behaviour
            of the output.
        :type output_opts: dict, optional
        :raises RuntimeError: Raises and error if there is not a specified path for the avro format.
        """

        self.stats["write_count"] = {}

        # Define the output schema.
        base_schema_to_write = self.original_base_schema if not base_schema_to_write \
            else base_schema_to_write
        nested_schemas_to_write = self.original_nested_schemas if not nested_schemas_to_write \
            else nested_schemas_to_write

        if "avro" not in paths_to_write:
            raise RuntimeError("It is necessary a path to write an Avro File!")

        # Write the Avro file (mandatory).
        writer_avro = AvroWriter(
            paths_to_write["avro"],
            base_schema_to_write,
            nested_schemas_to_write,
            output_opts
        )
        writer_avro.write_records(records_list, output_opts)
        self.stats["write_count"]["avro"] = writer_avro.write_count
        writer_avro.close()

        # Write the csv file (optional).
        if "csv" in paths_to_write:
            fieldnames = [field["name"] for field in base_schema_to_write["fields"]]

            # If requested, flatten the nested schemas.
            if output_opts.get("flat_nested_schema_on_csv") is not None:
                keys = output_opts["flat_nested_schema_on_csv"].values()
                fieldnames_nested = [nested_schema['fields'] for nested_schema in nested_schemas_to_write if
                                     nested_schema['name'] in keys]
                fields_concatenated = list(itertools.chain.from_iterable(fieldnames_nested))
                fieldnames_concatenated = [entry['name'] for entry in fields_concatenated]
                fieldnames += fieldnames_concatenated

            records = AvroReader(paths_to_write["avro"]).read_records()
            writer_csv = CSVWriter(paths_to_write["csv"], fieldnames)
            writer_csv.write_records(records, output_opts)
            self.stats["write_count"]["csv"] = writer_csv.write_count

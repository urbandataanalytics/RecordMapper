import os
import tempfile
import unittest

from RecordMapper import RecordMapper
from RecordMapper.avro.AvroReader import AvroReader
from RecordMapper.csv.CSVReader import CSVReader


class test_RecordMapper(unittest.TestCase):

    def test_transform_record(self):
        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
                {"name": "field_1", "type": "string", "aliases": ["another_field"]},
                {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"]},
                {"name": "field_3", "type": ["string", "null"]},
                {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]},
                {"name": "field_5", "type": "int", "transform": ["copyFrom(field_2)", "toNull"]}
            ]
        }

        input_record = {
            "field_auxiliar": 21
        }

        expected_record = {
            "field_2": 21,
            "field_4": 21,
            "field_5": None
        }

        res_record = RecordMapper(test_schema).transform_record(input_record)

        self.assertDictEqual(res_record, expected_record)

    def test_transform_record(self):
        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
                {"name": "field_1", "type": "string", "aliases": ["another_field"]},
                {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"]},
                {"name": "field_3", "type": ["string", "null"]},
                {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]},
                {"name": "field_5", "type": ["TestNestedSchema"],
                 "nestedSchemaSelector": "tests.custom_functions_for_tests.selectSchema(TestNestedSchema)"}
            ]
        }

        test_nested_schema = {
            "type": "record",
            "name": "TestNestedSchema",
            "fields": [
                {"name": "nested_field_1", "type": "string"},
                {"name": "nested_field_2", "type": ["string", "null"], "transform": "copyFrom(field_3)"}
            ]
        }

        input_record = {
            "field_auxiliar": 21,
            "field_3": "hola"
        }

        expected_record = {
            "field_2": 21,
            "field_3": "hola",
            "field_4": 21,
            "field_5": {
                "nested_field_2": "hola"
            }
        }
        recordMapper = RecordMapper(test_schema, [test_nested_schema])
        res_record = recordMapper.transform_record(input_record)

        self.assertDictEqual(res_record, expected_record)

    def test_write_with_schemas_for_write(self):
        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
                {"name": "field_1", "type": ["string", "null"]},
                {"name": "field_2", "type": ["int", "null"]},
                {"name": "field_3", "type": ["TestNestedSchema", "null"]},
            ]
        }

        test_nested_schema = {
            "type": "record",
            "name": "TestNestedSchema",
            "fields": [
                {"name": "nested_field_1", "type": ["string", "null"]},
                {"name": "nested_field_2", "type": ["string", "null"]}
            ]
        }

        test_schema_for_write = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
                {"name": "field_1", "type": ["string", "null"]},
                {"name": "field_3", "type": ["TestNestedSchema", "null"]},
            ]
        }

        test_nested_schema_for_write = {
            "type": "record",
            "name": "TestNestedSchema",
            "fields": [
                {"name": "nested_field_1", "type": ["string", "null"]},
                {"name": "nested_field_2", "type": ["string", "null"]},
                {"name": "nested_field_3", "type": ["string", "null"]}
            ]
        }

        input_records = [
            {
                "field_1": "example_1",
                "field_2": 5,
                "field_3": {
                    "nested_field_1": "nested_example_11",
                    "nested_field_2": "nested_example_21"
                }
            },
            {
                "field_1": "example_2",
                "field_2": 7,
                "field_3": {
                    "nested_field_1": "nested_example_12",
                    "nested_field_2": "nested_example_22"
                }
            }
        ]

        expected_records = [
            {
                "field_1": "example_1",
                "field_3": {
                    "nested_field_1": "nested_example_11",
                    "nested_field_2": "nested_example_21",
                    "nested_field_3": None
                }
            },
            {
                "field_1": "example_2",
                "field_3": {
                    "nested_field_1": "nested_example_12",
                    "nested_field_2": "nested_example_22",
                    "nested_field_3": None
                }
            }
        ]

        # Act
        avro_temp_file = tempfile.NamedTemporaryFile(delete=False)

        record_mapper = RecordMapper(test_schema, [test_nested_schema])

        record_mapper.write_records(input_records, {
            "avro": avro_temp_file.name
        }, test_schema_for_write, [test_nested_schema_for_write])

        # Assert
        avro_reader = AvroReader(avro_temp_file.name)
        res_records = list(avro_reader.read_records())
        avro_reader.close()
        self.assertListEqual(res_records, expected_records)

        os.remove(avro_temp_file.name)

    def test_execute_from_csv(self):
        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
                {"name": "field_1", "type": ["string", "null"], "aliases": ["another_field"]},
                {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"], "transform": "toInt"},
                {"name": "field_3", "type": ["string", "null"]},
                {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)", "toInt"]},
                {"name": "field_5", "type": "TestNestedSchema",
                 "nestedSchemaSelector": "tests.custom_functions_for_tests.selectSchema(TestNestedSchema)"}
            ]
        }

        test_nested_schema = {
            "type": "record",
            "name": "TestNestedSchema",
            "fields": [
                {"name": "nested_field_1", "type": ["string", "null"]},
                {"name": "nested_field_2", "type": ["string", "null"], "transform": "copyFrom(field_3)"}
            ]
        }

        expected_avro_records = [
            {
                "field_1": None,
                "field_2": 21,
                "field_3": "example_1",
                "field_4": 21,
                "field_5": {
                    "nested_field_1": None,
                    "nested_field_2": "example_1"
                }
            },
            {
                "field_1": None,
                "field_2": 54,
                "field_3": "example_2",
                "field_4": 54,
                "field_5": {
                    "nested_field_1": None,
                    "nested_field_2": "example_2"
                }
            },
        ]

        expected_csv_records = [
            {
                "field_1": None,
                "field_2": '21',
                "field_3": "example_1",
                "field_4": '21',
                "field_5": """{"nested_field_1": null, "nested_field_2": "example_1"}"""
            },
            {
                "field_1": None,
                "field_2": '54',
                "field_3": "example_2",
                "field_4": '54',
                "field_5": """{"nested_field_1": null, "nested_field_2": "example_2"}"""
            }
        ]
        # Act
        avro_temp_file = tempfile.NamedTemporaryFile(delete=False)
        csv_temp_file = tempfile.NamedTemporaryFile(delete=False)

        record_mapper = RecordMapper(test_schema, [test_nested_schema])

        record_mapper.execute("csv", "tests/files/test_1.csv", {
            "avro": avro_temp_file.name,
            "csv": csv_temp_file.name},
                              output_opts={
                                  "flat_nested_schema_on_csv": {},
                                  "merge_schemas": True
                              }
                              )

        # Assert
        avro_reader = AvroReader(avro_temp_file.name)
        avro_records = list(avro_reader.read_records())
        avro_reader.close()
        self.assertListEqual(avro_records, expected_avro_records)

        csv_reader = CSVReader(csv_temp_file.name)
        csv_records = list(csv_reader.read_records())
        csv_reader.close()
        self.assertListEqual([dict(x) for x in csv_records], expected_csv_records)

                # Check stats
        self.assertTrue(
            record_mapper.stats["read_count"] == \
            record_mapper.stats["write_count"]["csv"] == \
            record_mapper.stats["write_count"]["avro"]
        )

        os.remove(avro_temp_file.name)
        os.remove(csv_temp_file.name)

    def test_execute_from_csv_and_flat(self):
        # Arrange
        test_schema = {
            "type": "record",
            "name": "TestSchema",
            "fields": [
                {"name": "field_1", "type": ["string", "null"], "aliases": ["another_field"]},
                {"name": "field_2", "aliases": ["field_auxiliar"], "type": ["int", "null"], "transform": "toInt"},
                {"name": "field_3", "type": ["string", "null"]},
                {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)", "toInt"]},
                {"name": "field_5", "type": "TestNestedSchema",
                 "nestedSchemaSelector": "tests.custom_functions_for_tests.selectSchema(TestNestedSchema)"}
            ]
        }

        test_nested_schema = {
            "type": "record",
            "name": "TestNestedSchema",
            "fields": [
                {"name": "nested_field_1", "type": ["string", "null"]},
                {"name": "nested_field_2", "type": ["string", "null"], "transform": "copyFrom(field_3)"}
            ]
        }

        expected_avro_records = [
            {
                "field_1": None,
                "field_2": 21,
                "field_3": "example_1",
                "field_4": 21,
                "field_5": {
                    "nested_field_1": None,
                    "nested_field_2": "example_1"
                }
            },
            {
                "field_1": None,
                "field_2": 54,
                "field_3": "example_2",
                "field_4": 54,
                "field_5": {
                    "nested_field_1": None,
                    "nested_field_2": "example_2"
                }
            },
        ]

        expected_csv_records = [
            {
                "field_1": None,
                "field_2": '21',
                "field_3": "example_1",
                "field_4": '21',
                "field_5": None,
                "nested_field_1": None,
                "nested_field_2": "example_1"
            },
            {
                "field_1": None,
                "field_2": '54',
                "field_3": "example_2",
                "field_4": '54',
                "field_5": None,
                "nested_field_1": None,
                "nested_field_2": "example_2"
            }
        ]
        # Act
        avro_temp_file = tempfile.NamedTemporaryFile(delete=False)
        csv_temp_file = tempfile.NamedTemporaryFile(delete=False)

        record_mapper = RecordMapper(test_schema, [test_nested_schema])

        record_mapper.execute("csv", "tests/files/test_1.csv", {
            "avro": avro_temp_file.name,
            "csv": csv_temp_file.name},
                output_opts={
                  "flat_nested_schema_on_csv": {"field_5": "TestNestedSchema"},
                  "merge_schemas": True
                }
        )

        # Assert
        csv_reader = CSVReader(csv_temp_file.name)
        csv_records = list(csv_reader.read_records())
        csv_reader.close()
        self.assertListEqual([dict(x) for x in csv_records], expected_csv_records)

        # Check stats
        self.assertTrue(
            record_mapper.stats["read_count"] == \
            record_mapper.stats["write_count"]["csv"] == \
            record_mapper.stats["write_count"]["avro"]
        )

        os.remove(avro_temp_file.name)
        os.remove(csv_temp_file.name)

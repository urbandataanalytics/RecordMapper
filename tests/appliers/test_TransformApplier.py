import unittest

from RecordMapper.appliers import TransformApplier

from RecordMapper.builders.FlatSchemaBuilder import FieldData
from RecordMapper.builders.BuiltinFunctions import copyFrom, toNull, get_from_custom_variable

from tests import custom_functions_for_tests 

# FieldData = namedtuple("FieldData", ["types", "aliases", "transforms", "selector"])

class test_TransformApplier(unittest.TestCase):

    def test_transform_with_one_phase(self):

        # Arrange
        test_schema = {
            ("field_1", ) : FieldData(["string"], [], [], None),
            ("field_2", ) : FieldData(["int"], [], [], None),
            ("field_3", ) : FieldData(["int"], [], [copyFrom("field_2")], None),
        }

        input_record = {
            ("field_1",): "hola",
            ("field_2",): 56
        }

        expected_record = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_3",): 56
        }

        # Act
        res_record, res_schema = TransformApplier({}).apply(input_record, test_schema)
        
        # Assert
        self.assertDictEqual(res_record, expected_record)
        self.assertDictEqual(res_schema, test_schema) # Without changes


    def test_custom_transform_with_one_phase(self):

        # Arrange
        test_schema = {
            ("field_1", ) : FieldData(["string"], [], [], None),
            ("field_2", ) : FieldData(["int"], [], [], None),
            ("field_3", ) : FieldData(["int"], [], [custom_functions_for_tests.sum(5)], None),
        }

        input_record = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_3",): 7
        }

        expected_record = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_3",): 12
        }

        # Act
        res_record, res_schema = TransformApplier({}).apply(input_record, test_schema)
        
        # Assert
        self.assertDictEqual(res_record, expected_record)
        self.assertDictEqual(res_schema, test_schema) # Without Changes

    def test_several_transform_phases(self):

        test_schema = {
            ("field_1", ) : FieldData(["string"], [], [], None),
            ("field_2", ) : FieldData(["int"], [], [None, copyFrom("field_3")], None),
            ("field_3", ) : FieldData(["int"], [], [custom_functions_for_tests.sum(1), copyFrom("field_2")], None),
            ("field_4", ) : FieldData(["int"], [], [None, None, toNull()], None),
            ("field_5", ) : FieldData(["int"], [], [None, None, copyFrom("field_4")], None),
            ("field_6", ) : FieldData(["int"], [], [custom_functions_for_tests.sum(4)], None)
        }

        input_record = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_3",): 7,
            ("field_4",): 78,
            ("field_6",): 12
        }

        expected_record = {
            ("field_1",): "hola",
            ("field_2",): 8,
            ("field_3",): 56,
            ("field_4",): None,
            ("field_5",): 78,
            ("field_6",): 16 
        }

        # Act
        res_record, res_schema = TransformApplier({}).apply(input_record, test_schema)
        
        # Assert
        self.assertDictEqual(res_record, expected_record)
        self.assertDictEqual(res_schema, test_schema)

    def test_transform_with_nested_schemas(self):
        # Arrange
        test_schema = {
            ("field_1", ) : FieldData(["string"], [], [], None),
            ("field_2", ) : FieldData(["int"], [], [None, copyFrom("field_3")], None),
            ("field_3", ) : FieldData(["int"], [], [custom_functions_for_tests.sum(1), copyFrom("field_2")], None),
            ("field_4", ) : FieldData(["ExampleNestedSchema"], [], [], None),
            ("field_4", "nested_field_1") : FieldData(["int"], [], [], None),
            ("field_4", "nested_field_2") : FieldData(["int"], [], [None, copyFrom("field_3")], None),
            ("field_4", "nested_field_3") : FieldData(["int"], [], [None, None, copyFrom("field_3"), custom_functions_for_tests.sum(1)], None),
            ("field_4", "nested_field_4") : FieldData(["int"], [], [toNull()], None),
            ("field_4", "nested_field_5") : FieldData(["int"], [], [get_from_custom_variable("example_variable_2")], None)

        }

        input_record = {
            ("field_1",): "hola",
            ("field_2",): 56,
            ("field_3",): 7
        }

        expected_record = {
            ("field_1",): "hola",
            ("field_2",): 8,
            ("field_3",): 56,
            ("field_4", "nested_field_2"): 8,
            ("field_4", "nested_field_3"): 57,
            ("field_4", "nested_field_4"): None,
            ("field_4", "nested_field_5"): 9
        }

        # Act
        res_record, res_schema = TransformApplier({
            "example_variable_1": 5,
            "example_variable_2": 9,
            "example_variable_3": 1
        }).apply(input_record, test_schema)
        
        # Assert
        self.assertDictEqual(res_record, expected_record)
        self.assertDictEqual(res_schema, test_schema)
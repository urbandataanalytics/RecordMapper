import unittest
from RecordMapper.builders import FlatSchemaBuilder
from RecordMapper.builders.FunctionBuilder import InvalidFunctionError

class test_FlatSchemaBuilder(unittest.TestCase):

    def test_get_transform_functions_from_field_in_string_format(self):

        # Arrange
        test_type_field = "toNull()"

        # Act
        res = FlatSchemaBuilder.get_transform_functions_from_field(test_type_field)

        # Assert
        self.assertEqual(len(res), 1)

    def test_get_transform_functions_from_field_in_list_format(self):

        # Arrange
        test_type_field = ["toNull()", "copyFrom(field_x)"]

        # Act
        res = FlatSchemaBuilder.get_transform_functions_from_field(test_type_field)

        # Assert
        self.assertEqual(len(res), 2)

    def test_get_transform_functions_from_field_with_invalid_input(self):

        # Arrange
        test_type_field = ["toNuNope()", "copyFrom(field_x)"]

        # Act
        with self.assertRaises(InvalidFunctionError) as context:
            FlatSchemaBuilder.get_transform_functions_from_field(test_type_field)

        # Assert
        self.assertTrue("Invalid module for a custom function" in str(context.exception))
    
    def test_get_selector_function(self):

        # This is not a real selector function, but it has to work
        test_type = "toNull"

        res = FlatSchemaBuilder.get_selector_function(test_type)

        self.assertIsNotNone(res)

    def test_get_selector_function_invalid(self):

        # This is not a real selector function, but it has to work
        test_type = "toNuNope"

        with self.assertRaises(InvalidFunctionError) as context:
            FlatSchemaBuilder.get_selector_function(test_type)

        self.assertTrue("Invalid module for a " in str(context.exception))

    def test_get_flat_schema(self):
        # Arrange
        test_schema = {
            "type": "record",
            "name": "Example",
            "fields": [
               {"name": "field_1", "type": "string", "aliases": ["another_field"]},
               {"name": "field_2", "type": ["int", "null"]},
               {"name": "field_3", "type": ["Schema1", "Schema2"], "nestedSchemaSelector": "toNull"},
               {"name": "field_4", "type": "int", "transform": ["copyFrom(field_2)"]},
               {"name": "field_5", "type": "int", "transform": ["copyFrom(field_2)", "toNull"]}
            ]
        }

        # Act
        res = FlatSchemaBuilder.get_flat_schema(test_schema)

        # Arrange
        # Converted to a less complicated format to check
        parsed_res = [
            (key, types, aliases, len(transforms), True if selector is not None else False)
            for key, (types, aliases, transforms, selector) in res.items()
        ] 

        self.assertListEqual(parsed_res, [
            (("field_1",), ["string"], ["another_field"], 0, False),
            (("field_2",), ["int", "null"], [], 0, False),
            (("field_3",), ["Schema1", "Schema2"], [], 0, True),
            (("field_4",), ["int"], [], 1, False),
            (("field_5",), ["int"], [], 2, False)
        ])


from typing import Dict
from RecordMapper.builders.FlatSchemaBuilder import FieldData

class NestedSchemaSelectorApplier(object):
    """An applier that modifies a FlatSchema selecting multiple nested schemas using
       the nestedSchemaSelector function.
    """

    def __init__(self, flat_schemas: Dict[str, dict], custom_variables: dict):
        """The constructor function of this class.

        :param flat_schemas: A list of FlatSchemas.
        :type flat_schemas: Dict[str, dict]
        :param custom_variables: A dict of custom variables.
        :type custom_variables: dict
        """

        self.flat_schemas = flat_schemas
        self.custom_variables = custom_variables

    def apply(self, record: dict, base_flat_schema: dict) -> (dict, dict):
        """Execute the function of this applier. If a field has several nested schemas as type, this applier
        creates new base_flat_schema selecting one of the nested schemas (adding their fields to the resulting flat schema)
        using the function defined in the value "nestedSchemaSelector".

        :param record: The input record. 
        :type record: dict
        :param base_flat_schema: The input base flat schema. 
        :type base_flat_schema: dict. 
        :return: The record (unmodified) and the flat schema (modified with the selected nested schemas).
        :rtype: (dict, dict)
        """

        # The value that will be returned with the record
        complete_flat_schema = {**base_flat_schema}

        for field_key, field_data in base_flat_schema.items():

            if field_data.selector is not None:
                complete_flat_schema = self.select_nested_schema_and_add_their_fields(record, complete_flat_schema,
                                                                                      field_key, field_data)
                # Remove the "super key" to avoid problems
                del complete_flat_schema[field_key]

        return (record, complete_flat_schema)

    def select_nested_schema_and_add_their_fields(self, record: dict, flat_schema: dict, field_key: str,
                                                  field_data: FieldData) -> dict:
        """Select a nested schema for a field (using the defined function in "selector" value of the FieldData) and add their fields
        to the resulting flat schema.

        :param record: The input record. It will be used as argument
        :type record: dict
        :param flat_schema: The input flat schema.
        :type flat_schema: dict
        :param field_key: The key to "analyze".
        :type field_key: str
        :param field_data: The FieldData object of the previous key.
        :type field_data: tuple
        :raises RuntimeError: This function will raise if the selected nested schema name is not valid. 
        :return: The modified flat Schema.
        :rtype: dict
        """

        nested_schema_name = field_data.selector(field_key, record, self.custom_variables) \
            if field_data.selector != None and field_data.selector(field_key, record, self.custom_variables) != None else None

        # Not schema selected (this field will be null)
        if nested_schema_name == None:
            return flat_schema

        # Schema selected. Add their fields to the complete schema
        elif nested_schema_name in self.flat_schemas:

            nested_schema = self.flat_schemas[nested_schema_name]

            fields_to_add = dict(
                [
                    (field_key + nested_key, nested_field_data)
                    for nested_key, nested_field_data in nested_schema.items()
                ]
            )

            return {**flat_schema, **fields_to_add}
        else:
            raise RuntimeError(f"Invalid nested schema name: {nested_schema_name}")

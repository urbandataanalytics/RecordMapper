from typing import List, Union

from collections import namedtuple

class RenameApplier(object):
    """An applier that applies a renaming process using the defined aliases on the schema
    using the avro convention"""

    def __init__(self, custom_variables: dict):
        """The constructor of the applier.
        :param custom_variables: A dict of custom variables.
        :type custom_variables: dict
        """

        self.custom_variables = custom_variables

    def apply(self, flat_record: dict, flat_schema: dict) -> (dict, dict):
        """Executes the function of this applier. It renames the fields of the input record
        using the "aliases" value of each field and following the Avro convention.
        IMPORTANT: The "rename process" creates new fields, but it doesn't remove the old ones.
        So, if a record is {'field_1': 5} and the schema has the definition
        {"name": "field_2", "aliases": ["field_1"], "type": "int"}, the resulting record will be
        {"field_1": 5, "field_2": 5}.

        :param flat_record: The input flat record.
        :type flat_record: dict
        :param flat_schema: The input flat schema.
        :type flat_schema: dict
        :return: The record with the "renamed" fields.
        :rtype: dict
        """

        new_record = {**flat_record}

        # Rename each file of the original record
        for key, field_data in flat_schema.items():

            for alias in field_data.aliases:
                # As tuple key
                if (alias,) in new_record:
                    new_record[key] = new_record[(alias,)]

        return (new_record, flat_schema)

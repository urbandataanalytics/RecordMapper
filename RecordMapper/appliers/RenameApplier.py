from typing import List, Union

from collections import namedtuple


class RenameApplier(object):
    """An applier that applies a renaming process using the defined aliases
    on the schema using the avro convention.
    """

    def __init__(self, custom_variables: dict):
        """The constructor of the applier.

        :param custom_variables: A dict of custom variables.
        :type custom_variables: dict
        """

        self.custom_variables = custom_variables

    def apply(self, flat_record: dict, flat_schema: dict) -> (dict, dict):
        """Execute the function of this applier.

        Rename the fields of the input record using the "aliases" value of
        each field and following the Avro convention. The renaming process
        creates new fields, but it does not remove the old ones.

        For example, if a record is:
            {'field_1': 5}
        and the schema has the definition:
            {"name": "field_2", "aliases": ["field_1"], "type": "int"}
        the resulting record will be:
            {"field_1": 5, "field_2": 5}

        As an applier, the output is the transformed record and schema.
        In RenameApplier, only the record is transformed.

        :param flat_record: The input flat record.
        :type flat_record: dict
        :param flat_schema: The input flat schema.
        :type flat_schema: dict
        :return: The record with the "renamed" fields.
        :rtype: dict
        """

        new_record = {**flat_record}

        # Rename each field from the original record (if corresponds).
        for field_key, field_data in flat_schema.items():

            for alias in field_data.aliases:
                # The identifier keys in records are tuples.
                if (alias,) in new_record:
                    new_record[field_key] = new_record[(alias,)]

        return new_record, flat_schema

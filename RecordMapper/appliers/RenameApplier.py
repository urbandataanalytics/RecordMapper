from typing import List, Union

from collections import namedtuple

class RenameApplier(object):

    def apply(self, flat_record: dict, flat_schema: dict) -> dict:

        new_record = {**flat_record}

        # Rename each file of the original record
        for key, field_data in flat_schema.items():

            for alias in field_data.aliases:
                # As tuple key
                if (alias,) in new_record:
                    new_record[key] = new_record[(alias,)]

        return new_record

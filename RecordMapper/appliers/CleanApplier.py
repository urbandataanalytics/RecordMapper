


class CleanApplier(object):
    """A applier that removes the unused files of a record (using a schema as a reference).
    """

    def apply(self, flat_record: dict, flat_schema: dict) -> (dict, dict):
        """The apply of the CleanApplier. Removed unused files in a FlatRecord using
        a FlatSchema as reference (remove the fields that are not in the schema).

        :param flat_record: The input FlatRecord.
        :type flat_record: dict
        :param flat_schema: A FlatSchema. 
        :type flat_schema: dict
        :return: A clean FlatRecord and the original FlatSchema.
        :rtype: (dict,dict)
        """
        
        new_record = {}

        for key, field_data in flat_schema.items():

            if key in flat_record:
                new_record[key] = flat_record[key]
        
        return new_record, flat_schema
 
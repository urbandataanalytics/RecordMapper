

class CleanApplier(object):
    """An applier that removes the unused files of a record (using a schema as a reference).
    """

    def __init__(self, custom_variables: dict):
        """The constructor of the applier.

        :param custom_variables: A dict of custom variables.
        :type custom_variables: dict
        """

        self.custom_variables = custom_variables

    def apply(self, flat_record: dict, flat_schema: dict) -> (dict, dict):
        """Execute the function of this applier.

        Remove unused fields in a FlatRecord using a FlatSchema as
        reference. The applier only keeps the fields that are in the
        given schema, filtering the rest.

        As an applier, the output is the transformed record and schema.
        In CleanApplier, only the record is transformed.

        :param flat_record: The input FlatRecord.
        :type flat_record: dict
        :param flat_schema: A FlatSchema. 
        :type flat_schema: dict
        :return: A clean FlatRecord and the original FlatSchema.
        :rtype: (dict,dict)
        """
        
        new_record = {}

        for field_key, field_data in flat_schema.items():

            if field_key in flat_record:
                new_record[field_key] = flat_record[field_key]
        
        return new_record, flat_schema

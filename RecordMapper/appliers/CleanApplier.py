


class CleanApplier(object):

    def apply(self, flat_record: dict, flat_schema: dict) -> dict:
        
        new_record = {}

        for key, field_data in flat_schema.items():

            if key in flat_record:
                new_record[key] = flat_record[key]
        
        return new_record, flat_schema
 
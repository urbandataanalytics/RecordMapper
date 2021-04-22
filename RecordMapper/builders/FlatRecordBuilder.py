

class FlatRecordBuilder(object):
    """A builder of FlatRecords.
    
    A 'FlatRecord' is a record whose keys are tuples.
    The keys of nested keys will be flattened.

    For example, for a normal record which look like this:
      {
        "field_1": 5,
        "field_2": {
          "field_x": 7,
          "field_y": 9
        }
      }

    The FlatRecord will be:
      {
        ("field_1",): 5,
        ("field_2","field_x"): 7,
        ("field_2","field_y"): 9
      }
    """

    @staticmethod
    def get_flat_record_from_normal_record(record: dict) -> dict:
        """Flatten a record.

        Iterate over a record fields to return a flattened version
        of the record. Apply recurrently this method to each nested
        field inside the original field.

        :param record: The input record.
        :type record: dict
        :return: The flattened record.
        :rtype: dict
        """

        flat_record = {}
        for key, value in record.items():
            flat_key = (key,)
            if isinstance(value, dict):
                subrecord = FlatRecordBuilder.get_flat_record_from_normal_record(value)
                flat_record.update({flat_key+flat_subkey: subvalue
                                    for flat_subkey, subvalue in subrecord.items()})
            else:
                flat_record.update({flat_key: value})

        return flat_record
    
    @staticmethod
    def get_normal_record_from_flat_record(flat_record: dict) -> dict:
        """Return a normal record from a FlatRecord.

        Convert a FlatRecord object, a tuple-key dictionary,
        into a nested-key dictionary record.

        :param flat_record: The flat record.
        :type flat_record: dict
        :return: The normal record.
        :rtype: dict
        """

        res_record = {}
        for composed_key, value in flat_record.items():
            if len(composed_key) == 1:
                res_record[composed_key[0]] = value
            elif len(composed_key) == 2:
                super_key, nested_key = composed_key
                if super_key not in res_record:
                    res_record[super_key] = {}
                if isinstance(res_record[super_key], dict):
                    res_record[super_key][nested_key] = value
                else:
                    raise RuntimeError(f"Nested key in a non-dict field: {composed_key}")
            else:
                raise NotImplementedError(f"Three-depth record level is not supported -> {composed_key}")
        
        return res_record

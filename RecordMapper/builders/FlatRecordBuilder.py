

class FlatRecordBuilder(object):

    @staticmethod
    def get_flat_record_from_normal_record(record: dict) -> dict:
        return dict([((key,), value) for key, value in record.items()])
    
    @staticmethod
    def get_normal_record_from_flat_record(flat_record: dict) -> dict:

        print("flaat:", flat_record)

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
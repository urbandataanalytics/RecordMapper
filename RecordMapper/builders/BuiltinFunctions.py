
def copyFrom(path_to_copy_from: str):

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, is_nested_record: bool=False):
        composed_key = (path_to_copy_from,)
        return record.get(composed_key, None)

    return transform_function


def toNull():

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, is_nested_record: bool=False):
        return None

    return transform_function

def toInt():

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, is_nested_record: bool=False):
        if current_value is None:
            return None
        else:
            return int(current_value)
    
    return transform_function



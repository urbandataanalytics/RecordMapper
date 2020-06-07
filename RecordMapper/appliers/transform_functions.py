from RecordMapper.types import Record

def copyFrom(path_to_copy_from: str):

    def transform_function(current_value: object, record: Record, schema: dict):
        return record.get(path_to_copy_from, None)

    return transform_function


def toNull():

    def transform_function(current_value: object, record: Record, schema: dict):
        return None

    return transform_function


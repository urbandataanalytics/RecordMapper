from RecordMapper.types import Record

def sum(number_str: str):

    num_to_sum = int(number_str)

    def transform_function(current_value: object, record: Record, complete_transform_schema: dict, is_nested_record: bool =False):
        return int(current_value) + num_to_sum if current_value is not None else None

    return transform_function


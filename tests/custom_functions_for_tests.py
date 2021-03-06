
def sum(number_str: str):

    num_to_sum = int(number_str)

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict, is_nested_record: bool=False):
        return int(current_value) + num_to_sum if current_value is not None else None

    return transform_function

def collapse_values(current_field_name: str, target_dict_name: str):
    """Add the current field value to a dict. Changes the current value to None"""

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict, is_nested_record: bool=False):

        additional_values = {}
        target_dict = record.get((target_dict_name,), {})

        target_dict[current_field_name] = current_value
        additional_values[(target_dict_name,)] = target_dict

        return None, additional_values

    return transform_function

def selectSchema(schema_str: str):

    def selectFunction(key, record, custom_variables):
        return schema_str

    return selectFunction


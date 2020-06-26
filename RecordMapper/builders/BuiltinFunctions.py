"""
This module defines a set of built-in functions.
"""

def copyFrom(path_to_copy_from: str):
    """This built-in function returns the value of 'path_to_copy_from' key.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict, is_nested_record: bool=False):
        composed_key = (path_to_copy_from,)
        return record.get(composed_key, None)

    return transform_function


def toNull():
    """This returns always null.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict, is_nested_record: bool=False):
        return None

    return transform_function

def toInt():
    """This built-in function casts the current value to Int and returns the result.
    """


    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict, is_nested_record: bool=False):
        if current_value is None:
            return None
        else:
            return int(current_value)
    
    return transform_function

def toString():
    """This built-in function casts the current value to String and returns the result.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict, is_nested_record: bool=False):
        if current_value is None:
            return None
        else:
            return str(current_value)
    
    return transform_function

def get_from_custom_variable(variable_name: str):
    """This built-in function gets from custom_variables the value 'variable_name'.

    :param variable_name: The defined variable_name.
    :type variable_name: str
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict, is_nested_record: bool=False):
        return custom_variables[variable_name]

    return transform_function



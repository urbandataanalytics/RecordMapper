"""
This module defines a set of built-in functions.
"""

def copyFrom(path_to_copy_from: str):
    """This built-in function return the value of 'path_to_copy_from' key.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, is_nested_record: bool=False):
        composed_key = (path_to_copy_from,)
        return record.get(composed_key, None)

    return transform_function


def toNull():
    """This returns always null.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, is_nested_record: bool=False):
        return None

    return transform_function

def toInt():
    """This built-in function cast the current value to Int and returns the result.
    """


    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, is_nested_record: bool=False):
        if current_value is None:
            return None
        else:
            return int(current_value)
    
    return transform_function

def toString():
    """This built-in function cast the current value to String and returns the result.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, is_nested_record: bool=False):
        if current_value is None:
            return None
        else:
            return str(current_value)
    
    return transform_function




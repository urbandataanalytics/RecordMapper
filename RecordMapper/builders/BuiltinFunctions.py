"""
This module defines a set of built-in functions.
"""
import math
from datetime import datetime


def copyFrom(path_to_copy_from: str):
    """This built-in function returns the value of 'path_to_copy_from' key.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict,
                           custom_variables: dict):
        composed_key = (path_to_copy_from,)
        return record.get(composed_key, None)

    return transform_function


def toNull():
    """This returns always null.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict,
                           custom_variables: dict):
        return None

    return transform_function


def toInt():
    """This built-in function casts the current value to Int and returns the result.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict,
                           custom_variables: dict):
        value_to_return = None

        if current_value is not None:
            try:
                float_current_value = float(current_value)
                value_to_return = int(math.floor(float_current_value))
            except:
                value_to_return = None

        return value_to_return

    return transform_function


def toRoundedInt(use_floor: bool):
    """This built-in function casts the current value to Int and returns the result.
    :param floor: if use the floor method to truncate the value or to use the ceiling if floor is not given
    :type floor: str
   """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict,
                           custom_variables: dict):
        value_to_return = None

        if current_value is not None:
            try:
                float_current_value = float(current_value)
                value_to_return = int(math.floor(float_current_value)) if use_floor else int(math.ceil(float_current_value))
            except:
                value_to_return = None

        return value_to_return

    return transform_function


def toString():
    """This built-in function casts the current value to String and returns the result.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict,
                           is_nested_record: bool = False):
        if current_value is None:
            return None
        else:
            return str(current_value)

    return transform_function


def toBool(value_for_true: str):
    """This built-in function casts the current value to Bool and returns the result.
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict,
                           is_nested_record: bool = False):
        if current_value is None:
            return None
        else:
            return value_for_true == current_value

    return transform_function


def toDate(format: str):
    """This built-in function casts the current value to Date and returns the result.

    :param format: the passed format which will guide the parser to build the datetime correctly
    :type format: str
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict,
                           is_nested_record: bool = False):
        if current_value is None:
            return None
        else:
            date_time_obj = datetime.strptime(current_value, format)

            return str(date_time_obj)

    return transform_function


def timestampToDate(format: str):
    """This built-in function casts the current value to Date and returns the result.

    :param format: the passed format which will guide the parser to build the datetime correctly
    :type format: str
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict, custom_variables: dict,
                           is_nested_record: bool = False):
        if current_value is None:
            return None
        else:
            dt = datetime.fromtimestamp(float(current_value) / 1000)

            formatted_time = dt.strftime(f'{format}')[:-3]

            return str(formatted_time)

    return transform_function


def get_from_custom_variable(variable_name: str):
    """This built-in function gets from custom_variables the value 'variable_name'.

    :param variable_name: The defined variable_name.
    :type variable_name: str
    """

    def transform_function(current_value: object, record: dict, complete_transform_schema: dict,
                           custom_variables: dict):
        return custom_variables[variable_name]

    return transform_function

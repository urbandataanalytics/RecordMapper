"""
This module defines a set of built-in functions.
"""
import math
from datetime import datetime

import dateparser


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

            formatted_time = dt.strftime(f'{format}')

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


def transform_date_between_formats(input_format: str, output_format: str):
    """This built-in function transform given date as str from an input_format to an output_format.

    If given input date does not match input format, it will try to parse input date with dateparser.
    You can force dateparser parsing giving any incorrect input_format like "ignore", anyway  if
    a correct format does not match the given date, it will use dateparser.
    """

    def transform_function(current_value: str, record: dict, complete_transform_schema: dict, custom_variables: dict,
                           is_nested_record: bool = False):
        if current_value:
            try:
                datetime_before_transform = datetime.strptime(current_value, input_format)
            except ValueError:
                datetime_before_transform = dateparser.parse(current_value)

                if datetime_before_transform > datetime.now():
                    datetime_before_transform = datetime_before_transform.replace(year=datetime_before_transform.year-1)

            return datetime.strftime(datetime_before_transform, output_format)

    return transform_function

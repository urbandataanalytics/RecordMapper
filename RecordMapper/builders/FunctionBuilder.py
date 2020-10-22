import re
from typing import Callable, List
from inspect import getmembers, isfunction
import importlib

from RecordMapper.builders import BuiltinFunctions


class InvalidFunctionError(Exception):
    """An exception that represents an invalid string reference for a function"""
    pass


class FunctionBuilder(object):
    """A Builder class of functions"""

    @staticmethod
    def parse_function_str(function_str: str) -> Callable[[object, dict], object]:
        """
        This function parses a string and generates a function to be used as a transform function
        :param function_str: A string that represents a function.
        "type function_str: str
        :return: A transform function.
        :rtype: Callable[[object, dict], object]
        """
        parsed_function = re.match("^([\.\w]+)(?:\(([\w|,%\'-: ]*)\))?$", function_str)
        if not parsed_function:
            raise RuntimeError(f"Invalid name for a transform function: '{function_str}'")

        function_name, args = parsed_function.groups()
        args_list = str(args).split(",") if (args is not None and args != '') else []

        # Check if it is a built-in function
        builtin_function = FunctionBuilder.get_builtin_function(function_name, args_list)

        if builtin_function is not None:
            return builtin_function

        # Get it as custom function
        return FunctionBuilder.get_custom_function(function_name, args_list)

    @staticmethod
    def get_builtin_function(function_name: str, args_list: List[str]) -> Callable[[object, dict], object]:
        """Get a built-in function from a string.

        :param function_name: A string that represents a transform function.
        :type function_name: str
        :param args_list: Argument list for the function (that will return another function when it is executed).
        :type args_list: List[str]
        :return: A transform function.
        :rtype: Callable[[object, dict], object]

        """
        # Check if it is a built-in function
        possible_function = [obj for name, obj in getmembers(BuiltinFunctions) if
                             name == function_name and isfunction(obj)]

        if len(possible_function) == 1:
            return possible_function[0](*args_list)
        else:
            return None

    @staticmethod
    def get_custom_function(function_name: str, args_list: List[str]):
        """Get a custom function from a string. This string is a complete import path to the function.
        For example, example1.example2.example_function references the "example_function" in the module
        example2 inside the package example1.

        :param function_name: The name of the function (the complete import path).
        :type function_name: str
        :param args_list: Argument list for the function.
        :type args_list: List[str]
        :raises InvalidFunctionError: It raises an exception when the import path of the function is invalid.
        :return: A custom Transform function. 
        :rtype: Callable[[object, dict], object]
        """

        parts = function_name.split(".")
        module_path = ".".join(parts[:-1])
        function_name = parts[-1]
        try:
            mod = importlib.import_module(module_path)
            transform_function = getattr(mod, function_name)
        except ModuleNotFoundError:
            raise InvalidFunctionError(f"Invalid module for a custom function: '{function_name}'")
        except ValueError:
            raise InvalidFunctionError(f"Invalid module for a custom function: '{function_name}'")
        except AttributeError:
            raise InvalidFunctionError(f"Invalid name for a custom function: '{function_name}'")

        return transform_function(*args_list)
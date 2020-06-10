import re
from typing import Callable, List
from inspect import getmembers, isfunction
import importlib

from RecordMapper.builders import BuiltinFunctions

class InvalidFunctionError(Exception):
    pass

class FunctionBuilder(object):

    @staticmethod
    def parse_function_str(function_str: str) -> Callable[[object, dict], object]:

        parsed_function = re.match("^([\.\w]+)(?:\(([\w|,]+)\))?$", function_str)
        if not parsed_function:
            raise RuntimeError(f"Invalid name for a transform function: '{function_str}'")

        function_name, args = parsed_function.groups()
        args_list = str(args).split(",") if args is not None else []

        # Check if it is a built-in function
        builtin_function = FunctionBuilder.get_builtin_function(function_name, args_list) 

        if builtin_function is not None:
            return builtin_function

        # Get it as custom function
        return FunctionBuilder.get_custom_function(function_name, args_list)
 

    @staticmethod
    def get_builtin_function(function_name: str, args_list: List[str]):
        # Check if it is a built-in function
        possible_function = [obj for name, obj in getmembers(BuiltinFunctions) if name == function_name and isfunction(obj)]

        if len(possible_function) == 1:
            return possible_function[0](*args_list)
        else:
            return None

    @staticmethod
    def get_custom_function(function_name: str, args_list: List[str]):

        parts = function_name.split(".")
        module_path = ".".join(parts[:-1])
        function_name = parts[-1]
        try:
            mod = importlib.import_module(module_path)
            transform_function = getattr(mod, function_name)
        except ModuleNotFoundError:
            raise (f"Invalid module for a custom transform function: '{function_name}'")
        except ValueError:
            raise InvalidFunctionError(f"Invalid module for a custom transform function: '{function_name}'")
        except AttributeError:
            raise InvalidFunctionError(f"Invalid name for a custom transform function: '{function_name}'")

        return transform_function(*args_list)
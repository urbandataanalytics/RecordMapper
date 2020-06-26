from typing import Callable, List

def chain_functions(*functions_list: List[Callable]) -> Callable:
    """A helper function that creates another one that executes all the functions
    defined in functions_list in a waterfall way. 
    :param functions_list: A list of functions.
    """

    def wrapper_function(*input_args: List[object]) -> tuple:

        res = input_args

        for single_function in functions_list:
            args_as_list = object_as_tuple(res)
            res = single_function(*args_as_list) 

        return res
    
    return wrapper_function


def object_as_tuple(obj: object) -> tuple:
    """Transforms an object into a tuple (except if this object is already a tuple).

    :param obj: An object 
    :type obj: object
    :return: 
    :rtype: [type]
    """

    if isinstance(obj, tuple):
        return obj
    else:
        return (obj,)
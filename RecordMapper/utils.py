from typing import Callable, List


def chain_functions(*functions_list: List[Callable]) -> Callable:
    """Chain the given functions in a single pipeline.

    A helper function that creates another one that invoke
    all the given functions (defined in functions_list) in
    a waterfall way.

    The given functions should have the same input/output
    interface in order to run properly as a pipeline.

    :param functions_list: A list of functions.
    :type functions_list: List[Callable]
    :return:
    :rtype: Callable
    """

    def wrapper_function(*input_args: List[object]) -> tuple:

        res = input_args

        for single_function in functions_list:
            args_as_list = object_as_tuple(res)
            res = single_function(*args_as_list) 

        return res
    
    return wrapper_function


def object_as_tuple(obj: object) -> tuple:
    """Transform an object into a tuple.

    If the given objet is already a tuple, just return it.

    :param obj: A given object
    :type obj: object
    :return: The object as a tuple.
    :rtype: tuple
    """

    if isinstance(obj, tuple):
        return obj
    else:
        return (obj,)

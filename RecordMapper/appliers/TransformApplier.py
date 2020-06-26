import re
import importlib
from collections import namedtuple
from inspect import getmembers, isfunction
from typing import Callable, List


class TransformApplier(object):
    """This applier executes the defined transformations for each field.
    """

    def __init__(self, custom_variables: dict):
        """The constructor of the applier. It accepts a 'custom_variables' argument that its value
        will be passed to functions.
        :param custom_variables: A dict of custom variables.
        :type custom_variables: dict
        """

        self.custom_variables = custom_variables

    def apply(self, flat_record: dict, flat_schema: dict) -> (dict, dict):
        """Executes the function of this applier. It executes the defined functions in "transform"
        value in several phases. Each phase receives the resulting record of the previous one (except the first
        phase, that receives the initial record)-

        :param flat_record: The input record.
        :type flat_record: dict
        :param flat_schema: The input flat schema.
        :type flat_schema: dict
        return: The transformed record and the original flat schema.
        :rtype: (dict, dict)
        """

        # The maximum of transform phases to perform
        max_phases = max([len(field_data.transforms) for _, field_data in flat_schema.items()])
        # Initial copy of the record
        new_record = {**flat_record} 

        # Iterate over phases and get the transform_functions
        for phase_index in range(max_phases):
            new_values_in_this_phase = {}
            transforms_by_key = [
               (field_key, field_data.transforms[phase_index])
               for field_key, field_data in flat_schema.items()
               if phase_index < len(field_data.transforms) and field_data.transforms[phase_index] is not None
            ] 

            for key, transform_function in transforms_by_key:
                new_values_in_this_phase[key] = transform_function(new_record.get(key), new_record, flat_schema, self.custom_variables)
            
            new_record = {**new_record, **new_values_in_this_phase}

        return new_record, flat_schema 
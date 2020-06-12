import re
import importlib
from collections import namedtuple
from inspect import getmembers, isfunction
from typing import Callable, List



class TransformApplier(object):

    def apply(self, flat_record: dict, flat_schema: dict) -> (dict, dict):

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
                new_values_in_this_phase[key] = transform_function(new_record.get(key), new_record, flat_schema)
            
            new_record = {**new_record, **new_values_in_this_phase}

        return new_record, flat_schema 
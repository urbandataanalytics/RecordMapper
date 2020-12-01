import unittest
from RecordMapper.builders import FlatRecordBuilder

class test_FlatRecordBuilder(unittest.TestCase):

    def test_get_flat_record_from_normal_record(self):
        
        # Arrange
        input_record = {
            "field_1": 5,
            "field_2": "test",
            "field_3": {
                "a": 1,
                "b": 2
            }
        }

        # act
        res = FlatRecordBuilder.get_flat_record_from_normal_record(input_record)

        self.assertDictEqual(res, {
            ("field_1",): 5,
            ("field_2",): "test",
            ("field_3", "a"): 1,
            ("field_3", "b"): 2
        })

    def test_get_normal_record_from_flat_record(self):

        # Arrange
        input_record = {
            ("field_1",) : 1,
            ("field_2", ) :2,
            ("field_3", "nested_1") : 3,
            ("field_3", "nested_2"): 7
        }

        # Act
        res = FlatRecordBuilder.get_normal_record_from_flat_record(input_record)

        # Assert
        self.assertDictEqual(res, {
            "field_1": 1,
            "field_2": 2,
            "field_3": {
                "nested_1": 3,
                "nested_2": 7
            }
        })

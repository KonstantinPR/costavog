import unittest
from app.modules.pandas_handler import FALSE_LIST, replace_false_values, max_len_dc, df_merge_drop
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)


class TestReplaceFalseValues(unittest.TestCase):
    def test_replace_false_values(self):
        """
        Test that the function correctly replaces values in specified columns with an empty string
        when they match the false_list.
        """

        original_df = pd.DataFrame({
            'col1': ['NAN', 'Nan', 'nan'],
            'col2': ['None', True, False],
            'col3': [0, 'x', '0']
        })

        false_list = FALSE_LIST
        processed_df = replace_false_values(original_df, ['col2', 'col3'], false_list=false_list)

        expected_df = pd.DataFrame({
            'col1': ['NAN', 'Nan', 'nan'],
            'col2': ['', True, ''],
            'col3': ['', 'x', '']
        })

        self.assertTrue(processed_df.equals(expected_df))

        # Test with multiple columns
        original_df = pd.DataFrame({
            'col1': ['NAN', 'Nan', 'nan'],
            'col2': ['None', True, False],
            'col3': [0, 'x', '0']
        })

        false_list = FALSE_LIST
        processed_df = replace_false_values(original_df, ['col1', 'col2', 'col3'], false_list=false_list)

        expected_df = pd.DataFrame({
            'col1': ['', '', ''],
            'col2': ['', True, ''],
            'col3': ['', 'x', '']
        })

        self.assertTrue(processed_df.equals(expected_df))


class TestMaxLenDict(unittest.TestCase):
    def test_empty_dict(self):
        self.assertEqual(max_len_dc({}), 0)

    def test_single_element_dict(self):
        self.assertEqual(max_len_dc({'key': 'hello'}), len('hello'))

    def test_multiple_element_dict(self):
        dictionary = {'key1': 'hello', 'key2': 'world', 'key3': 'longer_string'}
        self.assertEqual(max_len_dc(dictionary), max(len(value) for value in dictionary.values()))

    def test_dict_with_non_string_values(self):
        with self.assertRaises(TypeError):
            max_len_dc({'key': 123, 'key2': 'hello'})


class TestDfMergeDrop(unittest.TestCase):
    def test_left_join(self):
        left_df = pd.DataFrame({'key': [1, 2], 'value': ['a', 'b']})
        right_df = pd.DataFrame({'key': [1, 2], 'other_value': ['x', 'y']})
        merged_df = df_merge_drop(left_df, right_df, 'key', 'key')
        self.assertEqual(merged_df.shape, (2, 3))

    def test_right_join(self):
        left_df = pd.DataFrame({'key': [1, 2], 'value': ['a', 'b']})
        right_df = pd.DataFrame({'key': [1, 2], 'other_value': ['x', 'y']})
        merged_df = df_merge_drop(left_df, right_df, 'key', 'key', how='right')
        self.assertEqual(merged_df.shape, (2, 3))

    def test_outer_join(self):
        left_df = pd.DataFrame({'key': [1, 2], 'value': ['a', 'b']})
        right_df = pd.DataFrame({'key': [1, 3], 'other_value': ['x', 'z']})
        merged_df = df_merge_drop(left_df, right_df, 'key', 'key', how='outer')
        self.assertEqual(merged_df.shape, (3, 3))

    def test_column_collision(self):
        left_df = pd.DataFrame({'key': [1, 2], 'other_value': ['x', 'y']})
        right_df = pd.DataFrame({'key': [1, 2], 'other_value': ['u', 'v']})
        merged_df = df_merge_drop(left_df, right_df, 'key', 'key')
        self.assertEqual(merged_df.shape, (2, 2))


if __name__ == '__main__':
    unittest.main()

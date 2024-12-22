import unittest

from app.modules.pandas_handler import FALSE_LIST, replace_false_values, max_len_dc, df_merge_drop
from app.modules.dfs_dynamic_module import abc_xyz
from app.modules.dfs_forming_module import zip_detail_V2

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


class TestAbcXyz(unittest.TestCase):
    def test_abc_xyz(self):
        # Create a sample DataFrame
        data = {
            'Артикул поставщика': ['A', 'B', 'C', 'D', 'E', 'F', 'G'],
            'Маржа-себест. 1': [-10000, -100, 300, '', 1000, 500, 1000],  # Including an empty string
            'Маржа-себест. 2': [-100, '', -2500, 0, '', 200, 10000],
            'Маржа-себест. 3': [-10, '', -500, 0, '', 20, 1000],
            'Ч. Продажа шт. 1': [10, '', 30, 40, 50, 10, 200],
            'Ч. Продажа шт. 2': [5, -1, 25, 35, 0, 2, 150],
            'Ч. Продажа шт. 4': [1, -1, 2, 4, 0, 1, 100],
        }

        merged_df = pd.DataFrame(data)

        # Call the function
        result_df = abc_xyz(merged_df)

        # Check the results

        self.assertEqual(result_df['Total_Margin'].tolist(), [-10110, -100, -2700, 0, 1000, 720, 12000],
                         "Total_Margin calculation is incorrect.")

        # ABC Classification checks
        self.assertEqual(result_df['ABC_Category'].tolist(), ['E', 'D', 'D', 'C', 'B', 'B', 'A'],
                         "ABC classification is incorrect.")

        # Check the CV calculation
        # Replace with expected values based on your logic
        expected_cv = [0.567, -0.894, 0.496, 0.47, 1.118, 0.786, 0.244]

        self.assertEqual(result_df['CV'].round(3).tolist(), [round(x, 5) for x in expected_cv],
                         "CV calculation is incorrect.")

        # Check the CV_mod values
        self.assertEqual(result_df['CV_mod'].round(3).tolist(), [0.567, 0.894, 0.496, 0.47, 1.118, 0.786, 0.244],
                         "CV_mod calculation is incorrect.")

        # Check XYZ Categories
        self.assertEqual(result_df['XYZ_Category'].tolist(), ['X', 'Z', 'X', 'X', 'Y', 'Y', 'W'],
                         "XYZ classification is incorrect.")


# To run the tests, use the command:
# pytest test_abc_xyz.py


if __name__ == '__main__':
    unittest.main()

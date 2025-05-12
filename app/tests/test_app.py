import unittest

from app.modules.pandas_handler import FALSE_LIST, replace_false_values, max_len_dc, df_merge_drop
from app.modules.dfs_dynamic_module import abc_xyz
from app.modules.dfs_process_module import concatenate_dfs
from pandas.testing import assert_frame_equal
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


class TestConcatenateDfPairs(unittest.TestCase):

    def test_even_number_of_dataframes(self):
        """Tests concatenation with an even number of DataFrames."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})
        df3 = pd.DataFrame({'A': [9, 10], 'B': [11, 12]})
        df4 = pd.DataFrame({'A': [13, 14], 'B': [15, 16]})
        df_list = [df1, df2, df3, df4]

        expected_df1_2 = pd.DataFrame({'A': [1, 2, 5, 6], 'B': [3, 4, 7, 8]})
        expected_df3_4 = pd.DataFrame({'A': [9, 10, 13, 14], 'B': [11, 12, 15, 16]})
        expected_result = [expected_df1_2, expected_df3_4]

        actual_result = concatenate_dfs(df_list)

        self.assertEqual(len(actual_result), len(expected_result), "Incorrect number of concatenated DataFrames")
        for i in range(len(expected_result)):
            assert_frame_equal(actual_result[i], expected_result[i], check_dtype=False)

    def test_odd_number_of_dataframes(self):
        """Tests concatenation with an odd number of DataFrames."""
        df1 = pd.DataFrame({'X': ['a', 'b'], 'Y': ['c', 'd']})
        df2 = pd.DataFrame({'X': ['e', 'f'], 'Y': ['g', 'h']})
        df3 = pd.DataFrame({'X': ['i', 'j'], 'Y': ['k', 'l']})
        df_list = [df1, df2, df3]

        expected_df1_2 = pd.DataFrame({'X': ['a', 'b', 'e', 'f'], 'Y': ['c', 'd', 'g', 'h']})
        expected_df3 = pd.DataFrame({'X': ['i', 'j'], 'Y': ['k', 'l']}) # The last one remains
        expected_result = [expected_df1_2, expected_df3]

        actual_result = concatenate_dfs(df_list)

        self.assertEqual(len(actual_result), len(expected_result), "Incorrect number of concatenated DataFrames")
        for i in range(len(expected_result)):
            assert_frame_equal(actual_result[i], expected_result[i], check_dtype=False)

    def test_empty_list(self):
        """Tests with an empty list of DataFrames."""
        df_list = []
        expected_result = []

        actual_result = concatenate_dfs(df_list)

        self.assertEqual(actual_result, expected_result, "Should return an empty list for an empty input")

    def test_single_dataframe(self):
        """Tests with a single DataFrame (odd number)."""
        df1 = pd.DataFrame({'P': [10], 'Q': [20]})
        df_list = [df1]
        expected_result = [df1] # Should return the single DataFrame as is

        actual_result = concatenate_dfs(df_list)

        self.assertEqual(len(actual_result), len(expected_result), "Incorrect number of concatenated DataFrames")
        assert_frame_equal(actual_result[0], expected_result[0], check_dtype=False)

    def test_dataframes_with_different_columns(self):
        """Tests concatenation with DataFrames having different columns."""
        df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        df2 = pd.DataFrame({'C': [5, 6], 'D': [7, 8]})
        df3 = pd.DataFrame({'A': [9, 10], 'B': [11, 12]})
        df4 = pd.DataFrame({'E': [13, 14], 'F': [15, 16]})
        df_list = [df1, df2, df3, df4]

        # pd.concat handles different columns by default (filling with NaN)
        expected_df1_2 = pd.DataFrame({'A': [1, 2, pd.NA, pd.NA],
                                       'B': [3, 4, pd.NA, pd.NA],
                                       'C': [pd.NA, pd.NA, 5, 6],
                                       'D': [pd.NA, pd.NA, 7, 8]})
        expected_df3_4 = pd.DataFrame({'A': [9, 10, pd.NA, pd.NA],
                                       'B': [11, 12, pd.NA, pd.NA],
                                       'E': [pd.NA, pd.NA, 13, 14],
                                       'F': [pd.NA, pd.NA, 15, 16]})
        expected_result = [expected_df1_2, expected_df3_4]

        actual_result = concatenate_dfs(df_list)

        self.assertEqual(len(actual_result), len(expected_result), "Incorrect number of concatenated DataFrames")
        for i in range(len(expected_result)):
            assert_frame_equal(actual_result[i], expected_result[i], check_dtype=False, check_column_order=False) # Allow different column order

    def test_dataframes_with_different_indices(self):
        """Tests concatenation with DataFrames having different indices."""
        df1 = pd.DataFrame({'A': [1, 2]}, index=['a', 'b'])
        df2 = pd.DataFrame({'A': [3, 4]}, index=[10, 20])
        df_list = [df1, df2]

        expected_df1_2 = pd.DataFrame({'A': [1, 2, 3, 4]}, index=['a', 'b', 10, 20])
        expected_result = [expected_df1_2]

        actual_result = concatenate_dfs(df_list)

        self.assertEqual(len(actual_result), len(expected_result), "Incorrect number of concatenated DataFrames")
        assert_frame_equal(actual_result[0], expected_result[0], check_dtype=False)

# To run the tests, use the command:
# pytest test_abc_xyz.py





if __name__ == '__main__':
    unittest.main()

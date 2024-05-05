import pandas as pd
import numpy as np
from random import randrange
from typing import Union
import logging

FALSE_LIST = [False, 0, 0.0, 'Nan', np.nan, pd.NA, None, '', 'Null', ' ', '\t', '\n']


def replace_false_values(df, columns, FALSE_LIST=None):
    """
    Replace values in specified columns that match FALSE_LIST or are strings with 0.

    Args:
    - df (DataFrame): The DataFrame containing the columns.
    - columns (list): List of column names to process.
    - FALSE_LIST (list, optional): List of values to be considered as false. Defaults to None.

    Returns:
    - DataFrame: The modified DataFrame.
    """

    if FALSE_LIST is None:
        FALSE_LIST = [False, 0, 0.0, 'Nan', np.nan, pd.NA, None, '', 'Null', ' ', '\t', '\n']

    for column in columns:
        # Replace values from FALSE_LIST
        df[column] = df[column].replace(FALSE_LIST, 0)
        # Attempt to convert remaining string values to numeric type
        print(f"column now is {column}")
        # df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)
    return df


def max_len_dc(dc, max_length: int = 0):
    for key, value in dc.items():
        length = len(value)
        if length > max_length:
            max_length = length
    return max_length


def dc_adding_empty(dc, max_length_dc, sep=''):
    for key, value in dc.items():
        if len(dc[key]) < max_length_dc:
            n = max_length_dc - len(dc[key])
            dc[key] = value + [sep for n in range(n)]
    return dc


def df_col_merging(df, df_from, col_name, random_suffix=f'_col_on_drop_{randrange(10)}', DEFAULT_FALSE_LIST=None):
    if DEFAULT_FALSE_LIST is None:
        DEFAULT_FALSE_LIST = FALSE_LIST
    df = df.merge(df_from, on=col_name, how='left', suffixes=("", random_suffix))
    for idx, col in enumerate(df.columns):
        if f'{col}{random_suffix}' in df.columns:
            for idj, val in enumerate(df[f'{col}{random_suffix}']):
                if not pd.isna(val) or not val in DEFAULT_FALSE_LIST:
                    df[col][idj] = val

    df = df.drop(columns=[x for x in df.columns if random_suffix in x]).fillna('')

    return df


def df_merge_drop(left_df, right_df, left_on, right_on):
    # Generate random suffixes

    left_suffix = f'_col_on_drop_x_{randrange(10)}'
    right_suffix = f'_col_on_drop_y_{randrange(10)}'

    # Merge dataframes
    merged_df = pd.merge(left_df, right_df, how='left', left_on=left_on, right_on=right_on,
                         suffixes=(left_suffix, right_suffix))

    # print(f"merged_df {merged_df}")

    # Rename columns containing '_col_on_drop_x' by removing that part
    merged_df.rename(columns=lambda x: x.replace(left_suffix, ''), inplace=True)

    # Drop columns with '_col_on_drop_y'
    columns_to_drop = [col for col in merged_df.columns if right_suffix in col]
    merged_df.drop(columns_to_drop, axis=1, inplace=True)

    return merged_df


def fill_empty_val_by(nm_columns: Union[list, str], df: pd.DataFrame, missing_col_name: str) -> pd.DataFrame:
    """
    Fill missing values in a DataFrame column with values from potential columns provided in nm_columns.

    Parameters:
        nm_columns (Union[list, str]): List of potential column names or a single column
        name to use for filling missing values.
        df (pd.DataFrame): DataFrame containing the data.
        missing_col_name (str): Name of the column with missing values to be filled.

    Returns:
        pd.DataFrame: DataFrame with missing values filled.
    """
    # Ensure nm_columns is a list
    if not isinstance(nm_columns, list):
        nm_columns = [nm_columns]

    # Iterate over each potential column name
    for nm_column in nm_columns:
        if nm_column in df.columns:
            # Fill missing values in 'missing_col_name' with values from the current column
            df[missing_col_name] = df[missing_col_name].fillna(df[nm_column])
            # Replace zeros in 'missing_col_name' with values from the current column
            df.loc[df[missing_col_name] == 0, missing_col_name] = df.loc[df[missing_col_name] == 0, nm_column]

    return df


def upper_case(df, name_columns):
    if not isinstance(name_columns, list): name_columns = [name_columns]
    for name_column in name_columns:
        if name_column in df:
            df[name_column] = [str(s).upper() if isinstance(s, str) else s for s in df[name_column]]
        else:
            print(f"Column {name_column} not found in the {df} dictionary.")
    return df


def first_letter_up(df, name_columns):
    if not isinstance(name_columns, list): name_columns = [name_columns]
    for name_column in name_columns:
        if name_column in df:
            df[name_column] = [str(s)[0].upper() + str(s)[1:] if isinstance(s, str) else s for s in df[name_column]]
        else:
            print(f"Column {name_column} not found in the {df} dictionary.")
    return df


def nmIDs_exclude(nmIDs, nmIDs_exclude):
    """exclude nmIDs_exclude from nmIDs"""

    nmIDs = list([int(nmID) for nmID in nmIDs])
    nmIDs_exclude = list([int(nmID) for nmID in nmIDs_exclude])
    # Exclude nmIDs present in nmIDs_exclude list
    nmIDs = [id for id in nmIDs if id not in nmIDs_exclude]
    logging.warning(f"Excluded list is got by. The number of elements is {len(nmIDs)}")
    return nmIDs

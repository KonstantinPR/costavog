import pandas as pd
import numpy as np
from random import randrange
from typing import Union

FALSE_LIST = [False, 0, 0.0, 'Nan', np.nan, None, '', 'Null']


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


def df_col_merging(df, random_suffix=f'_col_on_drop_{randrange(10)}', false_list=FALSE_LIST):
    for idx, col in enumerate(df.columns):
        if f'{col}{random_suffix}' in df.columns:
            for idj, val in enumerate(df[f'{col}{random_suffix}']):
                if not pd.isna(val) or not val in false_list:
                    df[col][idj] = val

    df = df_col_merging(df).df.drop(columns=[x for x in df.columns if random_suffix in x]).fillna('')

    return df


from typing import Union
import pandas as pd


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

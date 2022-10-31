import pandas as pd
import numpy as np
from random import randrange

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


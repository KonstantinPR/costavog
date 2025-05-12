from random import randrange
import pandas as pd


def fill_val_by_df(df_left, df_right, left_on="Артикул поставщика", right_on='vendoreCode', right_col='brand',
                   left_col='Бренд'):
    """
    Fills empty values in a specified column of df_left with values from df_right
    based on matching keys in the other DataFrames.

    Parameters:
    - df_left (pd.DataFrame): The left DataFrame to fill values in.
    - df_right (pd.DataFrame): The right DataFrame to source values from.
    - right_on (str): Column name in df_right to match on.
    - left_on (str): Column name in df_left to match on.
    - right_col (str): Column in df_right to use for filling values.
    - left_col (str): Column in df_left to fill values.

    Returns:
    - pd.DataFrame: The modified df_left with filled values.
    """
    # 1. Get unique rows based on 'vendoreCode' in df_right
    df_right_unique = df_right.drop_duplicates(subset=right_on)

    suffix = f'_col_on_drop_y_{randrange(10)}'

    # 2. Merge the DataFrames using unique values
    merged_df = df_left.merge(df_right_unique[[right_on, right_col]],
                              left_on=left_on,
                              right_on=right_on,
                              how='left',
                              suffixes=('', suffix))

    print(merged_df)

    # 3. Fill None or empty values in the specified left column
    col = right_col
    if suffix in merged_df.columns:
        col = f'{right_col}{suffix}'

    false_list = [False, 0, '0', 0.0, 'Nan', 'NAN', None, '', 'Null', ' ', '\t', '\n']
    merged_df[left_col] = merged_df[left_col].replace(false_list, None)  # Replace empty strings with None
    merged_df[left_col] = merged_df[left_col].fillna(merged_df[col])
    merged_df[left_col] = merged_df[left_col].replace(false_list, '')  # Replace empty strings with None

    # Optionally, drop the extra merged columns if needed
    merged_df = merged_df.drop(columns=[col for col in merged_df.columns if suffix in col])

    return merged_df  # Return the resulting DataFrame


# Sample DataFrames
df_left = pd.DataFrame({
    'Артикул поставщика': [101, 102, 103, 104, 105],
    'Бренд': [None, '', '', 0, ''],
})

df_right = pd.DataFrame({
    'vendoreCode': [100, 101, 101, 103, 105],
    'brand': ['BrandA', 'BrandC', 'ula', 'mula', '']
})

# Use the function and store the result
result_df = fill_val_by_df(df_left=df_left, df_right=df_right)
print(result_df)

from random import randrange

import pandas as pd
import numpy as np
import logging
import zipfile
import io
from app.modules import io_output, API_WB
from typing import Union, Optional, List

FALSE_LIST = [False, 0, '0', 0.0, 'Nan', 'NAN', 'nan', 'NaN', None, 'None', '', 'Null', ' ', '\t', '\n']
FALSE_LIST_2 = [False, 0, '0', 0.0, 'Nan', 'NAN', None, '', 'Null', ' ', '\t', '\n']

NAN_LIST = [np.nan, 'Nan', 'NAN', None, '', 'Null', ' ', '\t', '\n']
false_to_null = lambda x: 0 if pd.isna(x) or x in FALSE_LIST_2 else x

INF_LIST = [np.inf, -np.inf]
inf_to_null = lambda x: 0 if x in INF_LIST else x


def replace_false_values(df: pd.DataFrame, columns: Union[List[str], str],
                         false_list: Optional[List[Union[str, bool]]] = None, replace_to='') -> pd.DataFrame:
    """
    Replace values in specified columns that match FALSE_LIST or are strings with ''.

    Args:
    - df (pd.DataFrame): The DataFrame containing the columns.
    - columns (Union[List[str], str]): List of column names to process, or a single column name.
    - false_list (Optional[List[Union[str, bool]]], optional): List of values to be considered as false. Defaults to FALSE_LIST_2.

    Returns:
    - pd.DataFrame: The modified DataFrame.
    """

    if not isinstance(columns, (list, str)):
        raise ValueError("columns must be a list of strings or a single string")

    if false_list is None:
        false_list = FALSE_LIST_2  # Assuming FALSE_LIST_2 is defined globally

    if not isinstance(false_list, list):
        raise ValueError("FALSE_LIST must be a list of values")

    if isinstance(columns, str):
        columns = [columns]

    for col in columns:
        if col not in df.columns:
            logging.warning(f"Column '{col}' not found in DataFrame.")
            continue

    result_df = df.copy()
    for i, col in enumerate(columns):
        result_df[col] = result_df[col].apply(lambda x: replace_to if x in false_list else x)

    return result_df


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
        DEFAULT_FALSE_LIST = FALSE_LIST_2
    df = df.merge(df_from, on=col_name, how='left', suffixes=("", random_suffix))
    for idx, col in enumerate(df.columns):
        if f'{col}{random_suffix}' in df.columns:
            for idj, val in enumerate(df[f'{col}{random_suffix}']):
                if not pd.isna(val) or not val in DEFAULT_FALSE_LIST:
                    df[col][idj] = val

    df = df.drop(columns=[x for x in df.columns if random_suffix in x]).fillna('')

    return df


def df_merge_drop(left_df, right_df, left_on, right_on, how="left"):
    """Merge two DataFrames based on specified keys, handling type differences.

    Args:
        left_df (pd.DataFrame): The left DataFrame.
        right_df (pd.DataFrame): The right DataFrame.
        left_on (str): The column name in the left DataFrame to join on.
        right_on (str): The column name in the right DataFrame to join on.
        how (str): Type of merge to be performed (default is 'left').

    Returns:
        pd.DataFrame: The merged DataFrame.
    """

    # Convert both left and right keys to string to ensure matching even if types are different
    left_df = to_str(left_df, left_on)
    right_df = to_str(right_df, right_on)

    # Generate random suffixes for columns that might collide
    left_suffix = f'_col_on_drop_x_{randrange(10)}'
    right_suffix = f'_col_on_drop_y_{randrange(10)}'

    drop_suffix = left_suffix
    suffix = right_suffix
    if how == "left":
        drop_suffix = right_suffix
        suffix = left_suffix

    # Merge DataFrames
    merged_df = pd.merge(left_df, right_df, how=how, left_on=left_on, right_on=right_on,
                         suffixes=(left_suffix, right_suffix))

    # Rename columns to remove suffixes
    merged_df.rename(columns=lambda x: x.replace(suffix, ''), inplace=True)

    if how == 'outer':
        # Update the left_on column where values are missing
        condition = merged_df[left_on].isin(FALSE_LIST)  # Ensure FALSE_LIST is defined
        merged_df.loc[condition, left_on] = merged_df[right_on]

    # Drop columns from the right DataFrame that have the suffix
    columns_to_drop = [col for col in merged_df.columns if drop_suffix in col]
    merged_df.drop(columns_to_drop, axis=1, inplace=True)

    return merged_df


def fill_empty_val_by(nm_columns: Union[list, str], df: pd.DataFrame, col_name_with_missing: str) -> pd.DataFrame:
    """
    Fill missing values in a DataFrame column with values from potential columns provided in nm_columns.

    Parameters:
        nm_columns (Union[list, str]): List of potential column names or a single column
        name to use for filling missing values.
        df (pd.DataFrame): DataFrame containing the data.
        col_name_with_missing (str): Name of the column with missing values to be filled.

    Returns:
        pd.DataFrame: DataFrame with missing values filled.
    """
    # Ensure nm_columns is a list
    if not isinstance(nm_columns, list):
        nm_columns = [nm_columns]

    # Iterate over each potential column name
    for nm_column in nm_columns:
        if nm_column in df.columns:
            # Fill missing (NaN) values in 'missing_col_name' with values from the current column
            df[col_name_with_missing] = df[col_name_with_missing].fillna(df[nm_column])

            # Replace values in 'missing_col_name' that are in FALSE_LIST_2
            df.loc[df[col_name_with_missing].isin(FALSE_LIST_2), col_name_with_missing] = df.loc[
                df[col_name_with_missing].isin(FALSE_LIST_2), nm_column]

    return df


def upper_case(df_list, name_columns):
    if not isinstance(name_columns, list):
        name_columns = [name_columns]
    if not isinstance(df_list, list): df_list = [df_list]
    for d in df_list:
        for name_column in name_columns:
            if name_column in d:
                d[name_column] = [str(s).upper() if isinstance(s, str) else s for s in d[name_column]]
            else:
                print(f"Column {name_column} not found in the {d} dictionary.")
    return df_list


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
    print("nmIDs_exclude ...")

    nmIDs = list([int(nmID) for nmID in nmIDs])
    nmIDs_exclude = list([int(nmID) for nmID in nmIDs_exclude])
    # Exclude nmIDs present in nmIDs_exclude list
    nmIDs = [id for id in nmIDs if id not in nmIDs_exclude]
    print(f"Excluded list is got by. The number of elements is {len(nmIDs)}")
    return nmIDs


def round_df_if(df, half=10):
    # Function to format numeric values
    # If number more abs half then round, else don't
    def format_numeric(x):
        if isinstance(x, (int, float)):
            if np.isfinite(x):
                if abs(x) < half:
                    return round(x, 2)
                else:
                    return round(x)
            else:
                return x
        else:
            return x

    # Apply formatting function to all columns
    df = df.applymap(format_numeric)

    return df


def files_to_zip(list_files: list, list_names: list, zip_name='zip_files.zip'):
    print(f"files_to_zip...")
    # Check if list_files has exactly one non-None element
    valid_files = [file for file in list_files if file is not None]
    if len(valid_files) == 1:
        return io_output.io_output(valid_files[0]), list_names[0]  # Return the DataFrame directly

    # Create an in-memory zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for file, name in zip(list_files, list_names):
            if file is not None:
                df_output = io.BytesIO()
                with pd.ExcelWriter(df_output, engine='xlsxwriter') as writer:
                    file.to_excel(writer, index=False)
                df_output.seek(0)
                zip_file.writestr(name, df_output.getvalue())

    zip_buffer.seek(0)
    return zip_buffer, zip_name


def df_disc_template_create(df, df_promo, is_discount_template=False, default_discount=5, is_from_yadisk=True):
    if not is_discount_template:
        return pd.DataFrame

    if df_promo.empty:
        return pd.DataFrame

    # Fetch all cards and extract unique nmID values
    df_all_cards = API_WB.get_all_cards_api_wb(is_from_yadisk=is_from_yadisk)
    unique_nmID_values = df_all_cards["nmID"].unique()

    # Define the columns for the discount template
    df_disc_template_columns = [
        "Бренд", "Категория", "Артикул WB", "Артикул продавца",
        "Последний баркод", "Остатки WB", "Остатки продавца",
        "Оборачиваемость", "Текущая цена", "Новая цена",
        "Текущая скидка", "Новая скидка"
    ]

    # Create an empty DataFrame with the specified columns
    df_disc_template = pd.DataFrame(columns=df_disc_template_columns)

    # Populate "Артикул WB" with unique nmID values
    df_disc_template["Артикул WB"] = unique_nmID_values

    # If promo DataFrame is provided, merge and update "Новая скидка" by "new_discount" from df_promo
    if df_promo.empty:
        df_disc_template = df_merge_drop(df_disc_template, df, "Артикул WB", "nmId", how='outer')
    else:
        df_disc_template = df_merge_drop(df_disc_template, df_promo, "Артикул WB", "Артикул WB", how='outer')

    df_disc_template["Новая скидка"] = df_disc_template["new_discount"]

    # Ensure "Новая скидка" is filled with default_discount where NaN
    df_disc_template["Новая скидка"] = df_disc_template["Новая скидка"].fillna(default_discount)

    df_disc_template = df_disc_template.drop_duplicates(subset=["Артикул WB"])

    # Return the template DataFrame with the correct columns
    return df_disc_template[df_disc_template_columns]


def convert_to_dataframe(data, columns):
    """
    Convert the list of stock data (rows) from OZON API response to a Pandas DataFrame.

    Args:
        data (list): The 'rows' list from the OZON API response containing stock details.
        columns (list): List of column names that you want to include in the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with the relevant stock data.
    """
    if not data:
        return pd.DataFrame(columns=columns)  # Return an empty DataFrame with the specified columns

    # Convert the list of dictionaries to a DataFrame using the provided columns
    df = pd.DataFrame(data, columns=columns)

    return df


def to_str(df, columns):
    """
    Convert specified columns to strings, remove leading apostrophes,
    and remove '.0' from the end of the string if present.

    Args:
    - df (DataFrame): The DataFrame to process.
    - columns (list or str): The columns to convert to string.

    Returns:
    - DataFrame: The modified DataFrame.
    """
    # Ensure we are working with a copy of the DataFrame if necessary
    df = df.copy()  # This ensures you are working with a new DataFrame

    if not isinstance(columns, list):
        columns = [columns]

    for column in columns:
        if column in df.columns:  # Check if the column exists in the DataFrame
            try:
                # Convert the column to string
                df.loc[:, column] = df[column].astype(str)
                df.loc[:, column] = df[column].str.lstrip("'")

                # Remove '.0' only if it appears at the end of the string
                df.loc[:, column] = df[column].apply(lambda x: x[:-2] if x.endswith('.0') else x)

                # Optionally handle NaN values (convert 'nan' back to an empty string)
                df.loc[:, column] = df[column].replace('nan', '')

            except Exception as e:
                print(f"Error processing column '{column}': {e}")
        else:
            print(f"Warning: Column '{column}' not found in DataFrame.")

    return df


def csv_to_df(report_content):
    """
    Converts CSV content to DataFrame and handles specific columns as strings.
    :param report_content: Binary CSV content.
    :return: DataFrame or None if there is an error.
    """
    df = None
    try:
        if report_content:
            # Read the CSV content and treat specific columns as strings
            df = to_str(pd.read_csv(io.BytesIO(report_content), sep=';'), ["Артикул", "Barcode"])
            logging.info("DataFrame successfully created from report content.")
        else:
            logging.error("Report content is empty.")
    except Exception as e:
        logging.error(f"Error converting CSV content to DataFrame: {e}")

    return df


def keys_values_in_list_from_dict(dfs_dict, ext=''):
    filtered_dfs_list = []
    filtered_dfs_names_list = []

    for name, df in dfs_dict.items():
        if not df.empty:
            filtered_dfs_list.append(df)
            filtered_dfs_names_list.append(f"{name}{ext}")

    return filtered_dfs_list, filtered_dfs_names_list

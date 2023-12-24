import pandas as pd


def names_multiply_old(df, multiply, start_from=1):
    multiply_number = multiply + 1
    list_of_multy = []
    for art in df['Артикул']:
        for n in range(start_from, multiply_number):
            art_multy = f'{art}-{n}.JPG'
            if art_multy in list_of_multy:
                n = n + multiply
                art_multy = f'{art}-{n}.JPG'
            list_of_multy.append(art_multy)
    df = pd.DataFrame(list_of_multy)
    return df


# def names_multiply(df, multiply, col_multiply="Артикул", start_from = 1, step = 1):
#     """
#     Multiply strings based on the given input.
#
#     Parameters:
#     - input_str (str): Input string containing strings and numbers.
#
#     Returns:
#     - list: List of multiplied strings.
#     """
#     list_of_multy = []
#
#     for item in df[col_multiply]:
#         # Extract the string and number
#         parts = item.split('\t')
#         if len(parts) == 2:
#             string, number = parts
#             number = int(number)
#         else:
#             string, number = parts, multiply
#
#             # Create new strings by appending numbers from 1 to the given number
#             for n in range(start_from, number + step):
#                 list_of_multy.append(f'{string}-{n}.JPG')
#     df = pd.DataFrame(list_of_multy)
#     return df


def names_multiply(df, multiply, input_column="Артикул", start_from=1, step=1):
    """
    Multiply strings in a DataFrame column based on the given parameters.

    Parameters:
    - df (pd.DataFrame): DataFrame containing the input column.
    - multiply (int): The multiplication factor for each string.
    - input_column (str): The name of the column containing strings and numbers.
    - start_from (int): The starting number for the multiplication.
    - step (int): The step size for incrementing numbers.

    Returns:
    - pd.DataFrame: DataFrame with multiplied strings.
    """
    list_of_multy = []

    if input_column not in df.columns:
        raise ValueError(f"Column '{input_column}' not found in DataFrame.")

    for item in df[input_column]:
        # Extract the string and number
        parts = item.split('\t')

        if len(parts) == 2:
            string, number = parts
            if number:
                number = int(number)
            else:
                # If the number is not provided, use the default multiplication factor
                string, number = parts[0], multiply
        else:
            string, number = parts[0], multiply

        # Create new strings by appending numbers from start_from to the given number with the specified step
        for n in range(start_from, number + start_from, step):
            list_of_multy.append(f'{string}-{n}.JPG')

    df_result = pd.DataFrame(list_of_multy)

    return df_result

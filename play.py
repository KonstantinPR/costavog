from random import randrange
import pandas as pd

# Example data with various item codes
data = {
    'Артикул': [
        'TIE-12345-XYZ',  # starts with 'J', has hyphens
        'TIEIA-67890-ABC-',  # starts with 'IA', has hyphens
        'TS-54321',  # starts with 'TS', no second hyphen
        'Product-9876',  # no special prefix, one hyphen
        'Item-123',  # no special prefix, one hyphen
        'J-98765',  # starts with 'J', no second hyphen
        'OtherItem',  # no hyphen
        None,  # NaN value
        'TS-11111-XYZ-Extra',  # starts with 'TS', multiple hyphens
        'RandomCode'  # no hyphen
    ],
    'Income': [1000, 2000, 1500, 3000, 2500, 1800, 2200, 2400, 1600, 2000]
}

df_income = pd.DataFrame(data)


def item_code_without_sizes(df, art_col_name='Артикул', in_to_col='clear_sku'):
    def process_art_code(art_code):
        if pd.isna(art_code):
            return art_code  # Return NaN as is
        # Check if the art_code starts with 'j', 'ia', or 'ts'
        if art_code.startswith(('J', 'IA', 'TS')):
            # Find the last occurrence of "-"
            last_dash_index = art_code.rfind('-')
            return art_code[:last_dash_index]
        # For other cases, remove the last '-' if it exists
        if art_code[-1] == "-":
            return art_code[:-1]
        return art_code  # Return the original art_code if no changes are needed

    # Apply the function to the specified column and store the results in a new column
    df[in_to_col] = df[art_col_name].apply(process_art_code)
    return df


# Applying the function
result_df = item_code_without_sizes(df_income, art_col_name='Артикул', in_to_col='clear_sku')

print(result_df)

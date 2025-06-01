import pandas as pd
import numpy as np
from app.modules import pandas_handler

# Sample data based on your dataset
data = {
    'SKU': [317401841, 317433721],
    'product_id': [121005263, 121005265],
    'offer_id': ['J09-24', 'J09-25'],
    'requested_price': [1826, 1826],
    'auto_action_enabled': ['DISABLED', 'DISABLED'],
    'auto_add_to_ozon_actions_list_enabled': ['DISABLED', 'DISABLED'],
    'currency_code': ['RUB', 'RUB'],
    'min_price': [np.nan, 0],  # missing or empty
    'min_price_for_auto_actions_enabled': [np.nan, ''],
    'net_cost': [np.nan, ''],
    'old_price': [np.nan, ''],
    'price': ['1826', '1826'],  # as strings
    'price_strategy_enabled': ['DISABLED', 'DISABLED'],
    'status': ['Failed', 'Failed'],
    'response_message': [
        '400 Client Error: Bad Request for url: https://api-seller.ozon.ru/v1/product/import/prices',
        '400 Client Error: Bad Request for url: https://api-seller.ozon.ru/v1/product/import/prices'
    ]
}

# Create DataFrame
df = pd.DataFrame(data)

col_name_with_missing = ['new_ozon_price', 'price', 'net_cost', 'old_price', 'min_price']
for col in col_name_with_missing:
    if col not in df.columns:
        df[col] = 0
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)




# Print the resulting DataFrame
df.to_excel("dftest.xlsx")
print(df)

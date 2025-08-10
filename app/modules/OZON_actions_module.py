from app import app
import requests
import pandas as pd
import requests
import json
from app.modules import yandex_disk_handler


def api_get_actions(testing_mode=False, is_info_actions=True):
    BASE_URL = "https://api-seller.ozon.ru/v1/actions"  # замените, если нужно

    # Заголовки авторизации (замените на свои данные)
    headers = {
        'Client-Id': app.config['OZON_CLIENT_ID'],
        'Api-Key': app.config['OZON_API_TOKEN'],
        'Content-Type': 'application/json'
    }
    response = requests.get(f"{BASE_URL}", headers=headers)
    if response.status_code != 200:
        print(f"Ошибка при получении акций: {response.status_code} - {response.text}")
        return None

    data = response.json()
    actions_list = data.get("result", [])

    # Преобразуем список словарей в DataFrame
    df_actions = pd.DataFrame(actions_list)
    print(actions_list)
    return df_actions


def _get_for_action(id, last_id, headers='', limit=100, ):
    url = f"https://api-seller.ozon.ru/v1/actions/candidates"

    body = {
        "action_id": id,
        "limit": limit
    }
    if last_id:
        body["last_id"] = last_id

    response = requests.post(url, headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        print(f"Error fetching candidates for action {id}: {response.status_code}")
        return None

    data = response.json()
    return data.get('result', {})


def get_candidates_for_action(df_actions=None, headers=None, limit=100, testing_mode=False, is_upload_yadisk=True):
    if testing_mode:
        df_updated_prices, _ = yandex_disk_handler.download_from_YandexDisk(app.config['YANDEX_CANDIDATES_FOR_ACTION'])
        return df_updated_prices

    all_products_info = []

    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert list to comma-separated string
                v_str = ','.join(map(str, v))
                items.append((new_key, v_str))
            else:
                items.append((new_key, v))
        return dict(items)

    for action_id in df_actions['id']:
        print(f"Processing action ID: {action_id}")
        last_id = None

        while True:
            result = _get_for_action(action_id, last_id=last_id, headers=headers, limit=limit)
            if result is None:
                break

            products = result.get('products', [])
            total = result.get('total', 0)
            last_id = result.get('last_id')

            for prod in products:
                # Add the action_id to each product dictionary before flattening
                prod['action_id'] = str(action_id)
                flat_prod = flatten_dict(prod)
                all_products_info.append(flat_prod)

            print(f"Fetched {len(products)} products for action {action_id}, total so far: {len(all_products_info)}")

            if not last_id:
                break
            if len(all_products_info) >= total:
                break

        print(f"Total products in action {action_id}: {len(all_products_info)}")

    df_products = pd.DataFrame(all_products_info)

    if is_upload_yadisk:
        yandex_disk_handler.upload_to_YandexDisk(file=df_products, path=app.config['YANDEX_CANDIDATES_FOR_ACTION'],
                                                 file_name="candidates_for_action.xlsx")

    return df_products


def check_availability_actions(price, action_price, max_action_price, allowed_percent=10):
    """
    Checks whether the price should be adjusted based on the action price, max action price,
    and allowed percentage difference.
    Returns a tuple: (new_price, take_place_flag)
    """
    suggested_price = max(action_price, max_action_price)
    if price <= suggested_price:
        return suggested_price, 1
    else:
        # Calculate percentage difference
        difference = price - suggested_price
        percent_diff = (difference / price) * 100
        if percent_diff <= allowed_percent:
            return suggested_price, 1
        else:
            return price, 0


def analyze_availability_actions(df, allowed_percent=10):
    """
    Analyzes the DataFrame and updates 'allowed_price' and 'take_place' based on the logic.
    """
    # Ensure 'take_place' column exists
    if 'take_place' not in df.columns:
        df['take_place'] = 0

    if 'allowed_price' not in df.columns:
        df['allowed_price'] = df['price']

    # Compute results
    results = [
        check_availability_actions(price, action_price, max_action_price, allowed_percent)
        for price, action_price, max_action_price in
        zip(df['price'], df['action_price'], df['max_action_price'])
    ]

    # Unzip results into two lists
    allowed_prices, take_place_flags = zip(*results)

    # Assign to DataFrame columns
    df['allowed_price'] = allowed_prices
    df['take_place'] = take_place_flags

    return df


def go_out_action(df, headers, testing_mode=False, is_out_actions=True):
    """
    Deactivates products from Ozon actions where 'take_place' == 0,
    grouped by 'action_id', makes API calls, and updates the DataFrame
    with response info per product.

    Parameters:
        df (DataFrame): Original DataFrame containing product info.
        headers (dict): Headers with authentication info.

    Returns:
        df (DataFrame): Updated DataFrame with 'out_action_status' and 'out_action_response_info' columns.
    """
    if testing_mode:
        return df

    if not is_out_actions:
        return df

    url = "https://api-seller.ozon.ru/v1/actions/products/deactivate"

    # Add response info columns if they don't exist
    if 'out_action_status' not in df.columns:
        df['out_action_status'] = ''
    if 'out_action_response_info' not in df.columns:
        df['out_action_response_info'] = ''

    # Get unique action IDs
    action_ids = df['action_id'].unique()

    for action_id in action_ids:
        # Filter products for current action_id with take_place == 0
        mask = (df['action_id'] == action_id) & (df['take_place'] == 0)
        products_to_deactivate = df[mask]

        product_ids = products_to_deactivate['product_id'].tolist()

        if not product_ids:
            # Mark as no products to deactivate for this action_id
            df.loc[mask, 'out_action_status'] = 'No products to deactivate'
            df.loc[mask, 'out_action_response_info'] = ''
            continue

        # Prepare payload
        payload = {
            "action_id": action_id,
            "product_ids": product_ids
        }

        # Send API request
        response = requests.post(url=url, json=payload, headers=headers)

        # Process response for each product
        for _, row in products_to_deactivate.iterrows():
            # Find the index in the original DataFrame
            mask_row = (
                    (df['action_id'] == action_id) &
                    (df['product_id'] == row['product_id'])
            )
            indices = df[mask_row].index

            # Store response info
            response_text = response.text
            status_code = response.status_code

            # Update the DataFrame
            if len(indices) > 0:
                df.at[indices[0], 'out_action_status'] = 'Response received'
                df.at[indices[0], 'out_action_response_info'] = f"Status: {status_code}, Response: {response_text}"

            # Print detailed info
            print(f"Product ID: {row['product_id']} (Action ID: {action_id})")
            print(f"Response Status Code: {status_code}")
            print(f"Response Text: {response_text}")
            print("-" * 50)

    return df


def go_in_action(df, headers, testing_mode=False, is_in_actions=True):
    """
    Adds products to actions based on the DataFrame, makes API calls,
    and updates the original DataFrame with response info per product.

    Parameters:
        df (DataFrame): Original DataFrame containing product info.
        headers (dict): Headers with authentication info.

    Returns:
        df (DataFrame): Updated original DataFrame with added response info columns.
    """
    if testing_mode:
        return df
    if not is_in_actions:
        return df

    url = "https://api-seller.ozon.ru/v1/actions/products/activate"

    # Add new columns to the original DataFrame for response info
    df['add_to_action_status'] = ''
    df['response_info'] = ''

    # Filter only products where take_place == 1
    df_prepared = df[df['take_place'] == 1][['action_id', 'product_id', 'allowed_price', 'stock']]

    for action_id in df_prepared['action_id'].unique():
        # Select products for current action_id
        products_for_action = df_prepared[df_prepared['action_id'] == action_id]

        # Build products list with required fields
        products_list = [
            {
                "action_price": row['allowed_price'],
                "product_id": row['product_id'],
                "stock": row['stock']
            }
            for _, row in products_for_action.iterrows()
        ]

        payload = {
            "action_id": action_id,
            "products": products_list
        }

        # Make the API request
        response = requests.post(url=url, json=payload, headers=headers)

        # Process response for each product
        for _, row in products_for_action.iterrows():
            # Find the index in the original DataFrame
            mask = (
                    (df['action_id'] == action_id) &
                    (df['product_id'] == row['product_id'])
            )
            indices = df[mask].index

            # Store response info
            response_text = response.text
            status_code = response.status_code

            # Update the original DataFrame
            if len(indices) > 0:
                df.at[indices[0], 'in_action_status'] = 'Response received'
                df.at[indices[0], 'in_action_response_info'] = f"Status: {status_code}, Response: {response_text}"

            # Print detailed info
            print(f"Product ID: {row['product_id']} (Action ID: {action_id})")
            print(f"Response Status Code: {status_code}")
            print(f"Response Text: {response_text}")
            print("-" * 50)

    return df

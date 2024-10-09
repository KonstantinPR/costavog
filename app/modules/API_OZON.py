import logging
import math
import time

from flask import flash

import app.modules.request_handler
from app import app
import requests
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta
from app.modules import io_output
from app.modules import yandex_disk_handler, pandas_handler, request_handler
import datetime


def create_cards_report_ozon(client_id, api_key, language="DEFAULT", offer_ids=[], search="", skus=[],
                             visibility="ALL"):
    url = "https://api-seller.ozon.ru/v1/report/products/create"

    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    data = {
        "language": language,
        "offer_id": offer_ids,
        "search": search,
        "sku": skus,
        "visibility": visibility
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()['result']['code']
    else:
        print(f"Error creating report: {response.status_code}, {response.text}")
        return None


def process_cards_report(report_code, client_id, api_key, max_retries=20, retry_interval=20):
    """
    Handles the report generation, status checking, and downloading for Ozon cards.
    :param report_code: The report code received after creating the report.
    :param client_id: Ozon API client ID.
    :param api_key: Ozon API key.
    :param max_retries: Maximum number of attempts to check the report status.
    :param retry_interval: Time in seconds between retries.
    :return: The report content if successful, or None if it fails.
    """

    if not report_code:
        logging.error("Error creating or processing the report")
        return None

    for attempt in range(max_retries):
        print(f"attempt {attempt}")
        time.sleep(retry_interval)

        # Check report status
        report_info = check_report_info_ozon(report_code, client_id, api_key)
        if report_info:
            status = report_info['result']['status']
            print(f"status {status}")
            if status == 'success':
                # Report is ready, download the file
                report_file_url = report_info['result']['file']
                return download_report_file_ozon(report_file_url)
            elif status in ['processing', 'waiting']:
                continue  # Still processing, retry later
            else:
                logging.error(f"Report failed with status: {status}")
                return None

    logging.error(f"Report still processing after {max_retries} attempts")
    return None


def check_report_info_ozon(report_code, client_id, api_key):
    url = "https://api-seller.ozon.ru/v1/report/info"
    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    data = {
        "code": report_code
    }

    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()


def download_report_file_ozon(report_file_url):
    response = requests.get(report_file_url)

    if response.status_code == 200:
        return response.content  # CSV content
    else:
        print(f"Error downloading file: {response.status_code}, {response.text}")
        return None


def list_reports_ozon(client_id, api_key, report_type="ALL", page=1, page_size=100):
    url = "https://api-seller.ozon.ru/v1/report/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": api_key,
        "Content-Type": "application/json"
    }
    body = {
        "page": max(1, page),  # Ensure page is at least 1
        "page_size": min(page_size, 1000),  # Limit page_size to 1000
        "report_type": report_type
    }

    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 200:
        return response.json().get("result", {}).get("reports", [])
    else:
        print(f"Error listing reports: {response.status_code}, {response.text}")
        return None


def get_stock_ozon_api(client_id, api_key, limit=1000, offset=0, warehouse_type="ALL", is_to_yadisk=True):
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"

    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    all_rows = []

    while True:
        data = {
            "limit": limit,
            "offset": offset,
            "warehouse_type": warehouse_type
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            result = response.json().get('result', {})
            rows = result.get('rows', [])

            if not rows:
                break  # No more data to fetch, stop the loop

            all_rows.extend(rows)  # Add the current batch of rows to the list

            if len(rows) < limit:
                break  # If less than the limit, we've fetched everything

            offset += limit  # Update the offset for the next batch

        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None
    stock_report = {'result': {'rows': all_rows}}

    columns = [
        'free_to_sell_amount',
        'item_code',
        'item_name',
        'promised_amount',
        'reserved_amount',
        'sku',
        'warehouse_name',
        'idc'
    ]

    df = pandas_handler.convert_to_dataframe(stock_report['result']['rows'], columns)
    df = pandas_handler.to_str(df=df, columns="sku")

    if is_to_yadisk:
        file_name = f"stock_ozon.xlsx"
        yandex_disk_handler.upload_to_YandexDisk(df, file_name, path=app.config['YANDEX_STOCK_OZON'])

    return df


def get_price_ozon_api(limit=1000, last_id='', client_id='', api_key='', normalize=True):
    url = "https://api-seller.ozon.ru/v4/product/info/prices"
    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    prices = []

    while True:
        data = {
            "filter": {
                "offer_id": [],  # Populate based on your needs
                "product_id": [],  # Populate based on your needs
                "visibility": "ALL"
            },
            "last_id": last_id,  # Use the last_id for pagination
            "limit": int(limit)
        }

        response = requests.post(url, headers=headers, json=data)
        print(f"response {response}")
        response.raise_for_status()

        result = response.json()
        items = result.get('result', {}).get('items', [])

        if not items:  # If no items are returned, break the loop
            break

        prices.extend(items)

        # Update last_id for the next request
        last_id = result.get('result', {}).get('last_id', None)  # Get the last_id from the response
        print(f"last_id {last_id}")
        if last_id is None:  # If no more items are available, break the loop
            break

        # if len(prices) > 3000:
        #     break  # You can return prices or break here if 3000+ items are reached

    # Convert list of prices to DataFrame
    df = pd.DataFrame(prices)

    # Normalize any nested fields like 'price', 'commissions', 'price_index'
    if normalize:
        df = pd.json_normalize(
            prices,
            sep='_',
            record_prefix='',
            meta=['product_id', 'offer_id'],  # Keys to keep as top-level columns
            record_path=None,  # You can specify different paths if deeply nested
            errors='ignore'
        )

    return df


def get_realization_report_ozon_api(client_id='', api_key='', month='', year=''):
    url = "https://api-seller.ozon.ru/v2/finance/realization"
    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    data = {
        "month": month,  # The month (e.g., 9 for September)
        "year": year  # The year (e.g., 2024)
    }

    response = requests.post(url, headers=headers, json=data)
    print(f"response {response}")
    response.raise_for_status()  # Check for HTTP errors

    json_response = response.json().get('result', {})
    return json_response


def check_date_realization_report_ozon():
    today = datetime.datetime.today()
    day = today.day
    month = today.month - 1
    if day < 5:
        month = today.month - 2
    year = today.year
    if month == 0:
        year = today.year - 1
        month = 12
    return month, year


def get_transaction_list_ozon_api(client_id='', api_key='', date_from='', date_to='', page=1, page_size=1000):
    url = "https://api-seller.ozon.ru/v3/finance/transaction/list"
    headers = {
        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }

    data = {
        "filter": {
            "date": {
                "from": date_from,  # Ensure this is in ISO 8601 format
                "to": date_to  # Ensure this is in ISO 8601 format
            },
            "operation_type": [],
            "posting_number": "",
            "transaction_type": "all"
        },
        "page": page,
        "page_size": page_size
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Check for HTTP errors
        json_response = response.json().get('result', {})
        return json_response
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # Print the HTTP error
        print(f"Response content: {response.content.decode('utf-8')}")  # Print the response content
        return {}  # Return an empty dictionary or handle it as needed
    except Exception as err:
        print(f"Other error occurred: {err}")  # Print other errors
        return {}  # Return an empty dictionary or handle it as needed


def flatten_nested_columns(df, columns, isNormalize=True):
    """Flatten specified nested columns in a DataFrame."""
    for column in columns:
        if column in df.columns:
            # Explode the column if it contains lists
            df_exploded = df.explode(column)
            # Normalize the exploded column
            nested_df = pd.json_normalize(df_exploded[column])
            # Rename columns with the column name as a prefix
            nested_df.columns = [f'{column}_{col}' for col in nested_df.columns]
            # Join the normalized DataFrame back to the original
            df = df_exploded.join(nested_df)
            # Drop the original nested column
            df = df.drop(columns=[column], errors='ignore')

    # Drop duplicates based on 'operation_id'
    df = df.drop_duplicates(subset='operation_id')

    return df


def aggregate_by(col_name: str, df: pd.DataFrame, nonnumerical: list = [], is_group=True) -> pd.DataFrame:
    # Replace missing or empty values in the col_name column
    if not is_group:
        return df
    df[col_name].replace('', 'Empty Article', inplace=True)
    df[col_name].fillna('Empty Article', inplace=True)

    # Create an empty dictionary to hold aggregation logic
    agg_funcs = {}

    # Loop through each column in the DataFrame
    for column in df.columns:
        if column == col_name:
            continue  # Skip the grouping column itself

        # Check if the column is numeric, if so, we'll sum it
        if pd.api.types.is_numeric_dtype(df[column]) and column not in nonnumerical:
            agg_funcs[column] = 'sum'
        else:
            # Otherwise, we'll take the first value (suitable for strings or non-numeric data)
            agg_funcs[column] = 'first'

    # Group by the specified column and apply the aggregation functions
    df_grouped = df.groupby(col_name).agg(agg_funcs).reset_index()

    return df_grouped


def count_items_by(df, col="items_sku", in_col=None, negative=True, is_count=True):
    """
    Counts positive and negative values in 'in_col' for each unique 'col' (e.g., items_sku).
    Positive values are assigned to 'delivery_to' and negative values are assigned to 'delivery_from'.

    :param df: DataFrame with transaction data.
    :param col: The column to group by (e.g., 'items_sku').
    :param in_col: List of columns containing the values to count (e.g., ['accruals_for_sale', 'services_price']).
    :param negative: Whether to count negative values (True by default).
    :param is_count: Whether to perform the counting.
    :return: DataFrame with 'delivery_to' and 'delivery_from' columns added.
    """

    if not is_count or not in_col:
        return df

    # Initialize columns for delivery_to and delivery_from
    df['delivery_to'] = 0
    df['delivery_from'] = 0

    # Loop through each specified column
    for column in in_col:
        # Count positive values for delivery_to
        df['delivery_to'] += df[column].gt(0).astype(int)
        df['delivery_from'] += df[column].lt(0).astype(int)

    return df


def batch_update_prices(prices_data, client_id, api_key, batch_size=1000):
    total_products = len(prices_data)
    total_batches = math.ceil(total_products / batch_size)

    for i in range(total_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_products)
        batch = prices_data[start_index:end_index]

        print(f"Processing batch {i + 1}/{total_batches} with {len(batch)} products")

        # Ensure auto_action_enabled and min_price are properly set
        for product in batch:
            if product.get("auto_action_enabled") == "ENABLED" and product.get("min_price") == "0":
                product["min_price"] = None  # Set to None or a valid minimum price
                product["auto_action_enabled"] = "DISABLED"  # Disable automatic promotions if necessary

        # Call the API for this batch
        response = update_prices(batch, client_id=client_id, api_key=api_key)

        if response.get('result'):
            successful_updates = [r for r in response['result'] if r.get('updated')]
            failed_updates = [r for r in response['result'] if not r.get('updated')]

            if successful_updates:
                flash(f'Successfully updated prices for {len(successful_updates)} products in batch {i + 1}.',
                      'success')
            if failed_updates:
                flash(f'Failed to update prices for {len(failed_updates)} products in batch {i + 1}.', 'danger')
        else:
            flash(f'No valid response from API for batch {i + 1}.', 'danger')


def update_prices(prices_data, client_id='', api_key=''):
    url = 'https://api-seller.ozon.ru/v1/product/import/prices'  # Adjust the URL
    headers = {
        'Client-Id': client_id,  # Replace with actual Client ID
        'Api-Key': api_key,  # Replace with actual API Key
    }
    payload = {"prices": prices_data}

    response = requests.post(url, headers=headers, json=payload)
    print(response)
    return response.json()  # Return the response from the API


def item_code_without_sizes(df, art_col_name=''):
    """
    Function to remove unwanted characters from a column.

    Parameters:
    - df: DataFrame containing the data
    - art_col_name (optional): Name of the column to process

    Returns:
    - DataFrame with the modified column
    """
    if art_col_name not in df.columns:
        logging.warning(f"Column '{art_col_name}' not found in the DataFrame")
        return pd.DataFrame()  # Create an empty DataFrame instead of returning None

    mask = ((df[art_col_name].str.casefold().str.startswith('j')) |
            (df[art_col_name].str.casefold().str.startswith('ia')) |
            (df[art_col_name].str.casefold().str.startswith('ts')))

    df.loc[mask, 'clear_sku'] = df.loc[mask, art_col_name].str.split('-').str[0].values
    df.loc[mask, 'clear_sku'] = df.loc[mask, art_col_name].str[:-3].values

    print(f"df {df}")
    return df

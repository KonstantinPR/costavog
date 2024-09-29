import logging
import time

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


def get_stock_ozon_api(client_id, api_key, limit=1000, offset=0, warehouse_type="ALL"):
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

    return {'result': {'rows': all_rows}}


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
    return df

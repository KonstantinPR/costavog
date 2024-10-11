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


def get_average_storage_cost(testing_mode=False, is_shushary=None):
    df = get_storage_cost(testing_mode, is_shushary)

    df = df.groupby('vendorCode').agg(
        {'warehousePrice': 'mean', 'barcodesCount': 'mean', 'volume': 'mean', 'nmId': 'first'}).reset_index()
    df['storagePricePerBarcode'] = df['warehousePrice'] / df['barcodesCount']

    return df


def get_storage_cost(testing_mode=False, is_shushary=None, number_last_days=app.config['LAST_DAYS_DEFAULT'],
                     days_delay=0,
                     upload_to_yadisk=True):
    print(f"get_storage_cost...")

    if testing_mode:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
        return df

    storage_file_name = "storage_data"
    headers = {
        'accept': 'application/json',
        'Authorization': app.config['WB_API_TOKEN'],  # Uncomment and adjust this line if you're using Flask
    }

    # Set default date_from to a week ago if not provided
    date_from = (datetime.now() - timedelta(days=number_last_days)).strftime('%Y-%m-%d')
    date_to = (datetime.now() - timedelta(days=days_delay)).strftime('%Y-%m-%d')

    # Step 1: Create a report
    create_report_url = 'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage'
    params = {
        'dateFrom': date_from,
        'dateTo': date_to
    }
    response = requests.get(create_report_url, headers=headers, params=params)
    print(f"get_storage_cost status_code {response.status_code}")
    print(f"get_storage_cost status_code {response.text}")
    if response.status_code not in {200, 201}:  # Check for successful response (200 or 201)
        logging.warning("Failed to create report so file will be got from yadisk::", response.text)
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
        return df

    task_id = response.json()['data']['taskId']

    # Step 2: Check report status
    status_url = f'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status'
    while True:
        response = requests.get(status_url, headers=headers)
        if response.status_code not in {200, 201}:
            logging.warning("Failed to check report status so file will be got from yadisk:", response.text)
            df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
            return df

        status = response.json()['data']['status']
        if status == 'done':
            break  # Exit loop once report is ready
        elif status == 'error':
            logging.warning("Report generation failed.")
            return None

        # Wait and try again after some time
        time.sleep(10)  # Adjust sleep duration as needed

    # Step 3: Download report
    download_url = f'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download'
    response = requests.get(download_url, headers=headers)
    if response.status_code not in {200, 201}:
        logging.warning("Failed to download report so file will be got from yadisk::", response.text)
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
        return df

    report_data = response.json()
    # logging.warning(f"storage cost is receiving from API WB ...")
    # Step 4: Convert data to DataFrame
    df = pd.DataFrame(report_data)

    if upload_to_yadisk and not df is None:
        io_df = io_output.io_output(df)
        file_name = f'{storage_file_name}.xlsx'
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_STORAGE_COST'])

    if 'warehouse' in df.columns and is_shushary:
        print("Удаление сгоревших товаров Шушар...")
        df = df[df['warehouse'] != 'Санкт-Петербург Шушары']

    return df


def get_wb_price_api(request=None, testing_mode=None, is_from_yadisk=None):
    """
    Retrieve information price wb from the Wildberries API.
    """
    print("get_wb_price_api ...")

    if request: is_from_yadisk = request.form.get('is_from_yadisk')

    if testing_mode or is_from_yadisk:
        df, filename = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_PRICES')
        return df, filename

    # API endpoint and headers
    url = 'https://discounts-prices-api.wb.ru/api/v2/list/goods/filter'
    headers = {
        'accept': 'application/json',
        'Authorization': app.config['WB_API_TOKEN'],
    }

    # Parameters for pagination
    limit = 1000  # Maximum items per page
    offset = 0  # Initial offset

    # List to store data from all pages
    all_goods = []

    # Loop to retrieve data from all pages
    while True:
        # Make the request to the API with pagination parameters
        params = {'limit': limit, 'offset': offset}
        response = requests.get(url, headers=headers, params=params)
        print(f"Response status code: {response.status_code}")

        # Check if the request was successful
        if response.ok:
            data = response.json().get('data')
            if data:
                # Extract goods data from the response
                list_goods = data.get('listGoods', [])
                if list_goods:
                    all_goods.extend(list_goods)

                    # Check if there are more pages to retrieve
                    if len(list_goods) < limit:
                        break  # No more pages, exit the loop
                    else:
                        offset += limit  # Move to the next page
                else:
                    logging.warning("No goods data received from Wildberries API.")
                    break  # No more goods data available, exit the loop
            else:
                logging.warning("No data received from Wildberries API.")
                break  # No more data available, exit the loop
        else:
            logging.warning(f"Failed to fetch data from Wildberries API: {response.text}")
            break  # API request failed, exit the loop

    # Convert all goods data to DataFrame
    # df = pd.DataFrame(all_goods)
    df = pd.json_normalize(all_goods, 'sizes', ["vendorCode", 'nmID'], errors='ignore')
    # df['discount'] = (1 - (df['discountedPrice'] / df['price'])) * 100
    df['discount'] = ((df['price'] - df['discountedPrice']) / df['price']) * 100

    # Upload data to Yandex Disk
    file_name = "wb_price_data.xlsx"
    io_df = io_output.io_output(df)
    yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_PRICES'])

    return df, file_name


def get_wb_stock_api(request=None, testing_mode=False, is_shushary=True, is_upload_yandex=True,
                     date_from: str = '2019-01-01'):
    """get wb stock via api put in df"""

    print("get_wb_stock_api ...")

    if testing_mode:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STOCK_WB')
        if 'warehouseName' in df.columns and is_shushary:
            print("Удаление сгоревших товаров Шушар из остатков ...")
            df = df[df['warehouseName'] != 'Санкт-Петербург Шушары']
        return df

    print("stock from API WB ...")
    api_key = app.config['WB_API_TOKEN']
    url = f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}"
    headers = {'Authorization': api_key}

    response = requests.get(url, headers=headers)
    df = response.json()
    df = pd.json_normalize(df)

    if 'warehouseName' in df.columns and is_shushary:
        print("Удаление сгоревших товаров Шушар из остатков ...")
        df = df[df['warehouseName'] != 'Санкт-Петербург Шушары']

    df = df.reset_index().rename_axis(None, axis=1)
    df.replace(np.NaN, 0, inplace=True)

    if not request:
        return df

    if hasattr(request, 'form'):
        # If it's a Flask request object, get the values from the form
        no_city = request.form.get('no_city')
        no_sizes = request.form.get('no_sizes')
    elif isinstance(request, dict):
        # If it's a dictionary, directly access the values
        no_city = request.get('no_city')
        no_sizes = request.get('no_sizes')
    else:
        raise ValueError("Invalid request type. It should be either a 'requests' object or a dictionary.")

    print("stock from API WB is gotten")

    if no_city == 'no_city' and no_sizes == 'no_sizes':
        df = df.pivot_table(index=['nmId'],
                            values=['quantityFull',
                                    'inWayFromClient',
                                    'inWayToClient',
                                    'supplierArticle',
                                    'category',
                                    'subject',
                                    'brand',
                                    ],
                            aggfunc={'quantityFull': sum,
                                     'inWayFromClient': sum,
                                     'inWayToClient': sum,
                                     'supplierArticle': 'first',
                                     'category': 'first',
                                     'subject': 'first',
                                     'brand': 'first',
                                     },
                            margins=False)

        # df['quantityFull'] = df['quantityFull'] - df['inWayFromClient'] - df['inWayToClient']
        df['quantityFull'] = df['quantityFullAll']
        df = df.reset_index().rename_axis(None, axis=1)

        if is_upload_yandex and df is not None:
            io_df = io_output.io_output(df)
            file_name = f'stock_wb.xlsx'
            yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_STOCK_WB'])

        return df

    if no_city == 'no_city':
        df = df.pivot_table(index=['nmId', 'techSize'],
                            values=['quantityFull',
                                    'inWayFromClient',
                                    'inWayToClient',
                                    'supplierArticle',
                                    'category',
                                    'subject',
                                    'brand',
                                    ],
                            aggfunc={'quantityFull': sum,
                                     'inWayFromClient': sum,
                                     'inWayToClient': sum,
                                     'supplierArticle': 'first',
                                     'category': 'first',
                                     'subject': 'first',
                                     'brand': 'first',
                                     },
                            margins=False)

        df['quantityFull'] = df['quantityFullAll']
        df = df.reset_index().rename_axis(None, axis=1)

        return df

    if no_sizes == 'no_sizes':
        df = df.pivot_table(index=['nmId', 'warehouseName'],
                            values=['quantityFull',
                                    'inWayFromClient',
                                    'inWayToClient',
                                    'supplierArticle',
                                    'category',
                                    'subject',
                                    'brand',
                                    ],
                            aggfunc={'quantityFull': sum,
                                     'inWayFromClient': sum,
                                     'inWayToClient': sum,
                                     'supplierArticle': 'first',
                                     'category': 'first',
                                     'subject': 'first',
                                     'brand': 'first',
                                     },
                            margins=False)

        df['quantityFull'] = df['quantityFullAll']
        df = df.reset_index().rename_axis(None, axis=1)

        return df

    return df


def get_all_cards_api_wb(testing_mode=False, is_from_yadisk=False, is_to_yadisk=False, textSearch: str = None,
                         is_unique=False, limit_cards=None):
    """get_all_cards_api_wb"""
    print("get_all_cards_api_wb ...")

    if testing_mode or is_from_yadisk:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_ALL_CARDS_WB')
        return df

    limit = 100
    total = 100
    updatedAt = None
    nmId = None
    dfs = []
    count = 0
    while total >= limit:
        print(f"{get_all_cards_api_wb.__name__} ... total {count}")
        headers = {
            'accept': 'application/json',
            'Authorization': app.config['WB_API_TOKEN2'],
        }

        data = {
            "settings": {
                "cursor": {
                    "limit": limit,
                    "updatedAt": updatedAt,
                    "nmID": nmId,
                },
                "filter": {
                    "textSearch": textSearch,
                    "withPhoto": -1
                }
            }
        }

        response = requests.post('https://suppliers-api.wildberries.ru/content/v2/get/cards/list',
                                 data=json.dumps(data), headers=headers)

        if response.status_code != 200:
            logging.warning(f"Error in API request: {response.status_code}")
            print(f"response.statuse_code {response.status_code}")
            print(f"response.text {response.text}")
            break

        df_json = response.json()
        if 'error' in df_json:
            logging.warning(f"API Error: {df_json['errorText']}")
            break

        total = df_json['cursor']['total']
        updatedAt = df_json['cursor']['updatedAt']
        nmId = df_json['cursor']['nmID']
        dfs += df_json['cards']
        count = count + total

        if limit_cards and count > int(limit_cards):
            break

    df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID', "dimensions", "characteristics"],
                           errors='ignore')

    if is_to_yadisk:
        io_df = io_output.io_output(df)
        file_name = f'all_cards_wb.xlsx'
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_ALL_CARDS_WB'])

    return df


def get_wb_sales_realization_api_v2(date_from: str, date_to: str, days_step: int = 7):
    """Get sales data from the Wildberries API v2"""

    api_key = app.config['WB_API_TOKEN2']  # Assuming you have the API token configured in your app
    headers = {'Authorization': api_key}
    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

    url_params = {
        'dateFrom': date_from,
        'dateTo': date_to
    }

    response = requests.get(url, headers=headers, params=url_params)
    print(f'response {response}')
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        logging.warning(f"Failed to fetch data for {date_from} to {date_to}")
        return None


def get_wb_sales_funnel_api(request,
                            testing_mode=False,
                            is_funnel=True,
                            is_re_double=True,
                            is_to_yadisk=True) -> (pd.DataFrame, str):
    """get_wb_sales_funnel_api"""
    print("get_wb_sales_funnel_api...")
    if not is_funnel:
        return pd.DataFrame, None

    date_from = request_handler.request_date_from(request)
    date_end = request_handler.request_date_end(request)

    if testing_mode:
        print(f"df downloading in {get_wb_sales_funnel_api.__doc__} from YandexDisk")
        df, filename = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_SALES_FUNNEL_WB')
        return df, filename

    is_from_yadisk = request.form.get('is_from_yadisk')
    testing_mode = request.form.get('testing_mode')
    is_erase_points = request.form.get('is_erase_points')
    is_exclude_nmIDs = request.form.get('is_exclude_nmIDs')

    # Retrieve nmIDs from API and exclude cards from Yandex Disk
    df_nmIDs = get_all_cards_api_wb(testing_mode=testing_mode, is_from_yadisk=is_from_yadisk)
    if not 'nmID' in df_nmIDs.columns: logging.warning(f'column nmID in df_nmIDs is not found')

    nmIDs = df_nmIDs['nmID'].unique()
    if is_exclude_nmIDs:
        nmIDs_exclude = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_EXCLUDE_CARDS')[0]['nmID']
        nmIDs = pandas_handler.nmIDs_exclude(nmIDs, nmIDs_exclude)

    api_key = app.config['WB_API_TOKEN2']
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    # Convert the input date strings to datetime objects
    date_from = datetime.strptime(date_from, "%Y-%m-%d")
    date_end = datetime.strptime(date_end, "%Y-%m-%d")

    # Convert the datetime objects to the desired format strings
    date_from = date_from.strftime("%Y-%m-%d %H:%M:%S")
    date_end = date_end.strftime("%Y-%m-%d %H:%M:%S")

    print(f"gettin sales funnel by date_from {date_from}, date_end {date_end}")

    df = _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers)

    # Rename columns (stay only last part before points, for example statistic.order.date to only date)
    if is_erase_points:
        df = df.rename(columns=lambda x: x.split('.')[-1])

    if is_re_double:
        df = _rename_double_columns(df, "_re")

    if is_to_yadisk and df is not None and not df.empty:
        io_df = io_output.io_output(df)
        file_name = f'wb_sales_funnel.xlsx'
        print(f'df uploading in {get_wb_sales_funnel_api.__doc__} to YandexDisk by name {file_name}')
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_SALES_FUNNEL_WB'])

    # Log a message indicating successful retrieval of sales funnel data
    print("Sales funnel data retrieved successfully")
    file_name = f'wb_sales_funnel_{str(date_from)[:10]}_{str(date_end[:10])}.xlsx'

    return df, file_name


def _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers, chunk_size=1000):
    df = pd.DataFrame()
    chunks = [nmIDs[i:i + chunk_size] for i in range(0, len(nmIDs), chunk_size)]
    cards_count = chunk_size
    print(f"getting sales_funnel via API {cards_count} qt ...")
    for chunk in chunks:

        payload = {
            "brandNames": [],
            "objectIDs": [],
            "tagIDs": [],
            "nmIDs": chunk,
            "timezone": "Europe/Moscow",
            "period": {
                "begin": date_from,
                "end": date_end
            },
            "orderBy": {
                "field": "ordersSumRub",
                "mode": "asc"
            },
            "page": 1
        }

        response = requests.post(url, json=payload, headers=headers)
        print(f'response.status_code {get_wb_sales_funnel_api.__doc__}: {response.status_code}')
        if response.status_code != 200:
            logging.warning(f'Error in {get_wb_sales_funnel_api.__doc__}: {response.text}')
            return None
        else:
            try:
                # Convert the response data from JSON to a Python dictionary
                response_dict = json.loads(response.text)

                # Extract the 'cards' data from the response dictionary
                cards_data = response_dict['data']['cards']

                # Flatten the nested dictionaries into separate columns using pd.json_normalize
                df_chunk = pd.json_normalize(cards_data, errors='ignore', record_prefix='')


            except Exception as e:
                logging.warning(f'Error parsing response: {e}')
                return None

        df = pd.concat([df, df_chunk], ignore_index=True)
        cards_count += len(chunk)

    return df


def _rename_double_columns(df, suffix):
    # Get the list of column names
    columns = df.columns

    # Dictionary to store the count of each column name
    column_counts = {}

    # List to store the new column names
    new_columns = []

    # Iterate through the columns
    for column in columns:
        # If the column name is repeated, append "_prev" with count to it
        if column in column_counts:
            count = column_counts[column]
            new_column_name = f"{column}_{suffix}_{count}"
            column_counts[column] += 1
            new_columns.append(new_column_name)
        else:
            column_counts[column] = 1
            new_columns.append(column)

    # Update the DataFrame with the new column names
    df.columns = new_columns
    return df


def get_wb_sales_realization_api_v3(request, api_key='', date_from='', date_end='', rrdid=0, limit=100000):
    # Example usage
    if not api_key:
        api_key = app.config['WB_API_TOKEN2']  # Replace with your actual API key

    if not date_from or not date_end:
        date_from = request_handler.request_date_from(request)
        date_end = request_handler.request_date_end(request)

    # Convert the input date strings to datetime objects
    date_from = str(datetime.strptime(date_from, "%Y-%m-%d"))[0:10]
    date_end = str(datetime.strptime(date_end, "%Y-%m-%d"))[0:10]
    print(date_from, date_end)

    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

    # Set up the headers with your API key
    headers = {
        "Authorization": f"Bearer {api_key}"  # or however the API expects the token
    }

    # Set up the query parameters
    params = {
        "dateFrom": date_from,
        "dateTo": date_end,
        "rrdid": rrdid,
        "limit": limit
    }

    try:
        # Make the GET request
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise an error for bad responses

        # Parse the JSON response
        data = response.json()
        df = pd.DataFrame(data)

        if 'is_archive':
            yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                            path_or_config=app.config['REPORT_SALES_REALIZATION'])

        if 'is_to_yadisk' in request.form:
            file_name = "report_sales_realization.xlsx"
            yandex_disk_handler.upload_to_YandexDisk(df, file_name, path=app.config['REPORT_SALES_REALIZATION'])

        return df

    except requests.exceptions.HTTPError as e:
        if response.status_code == 401:
            error_info = response.json()
            print("Unauthorized Error:", error_info)
            return pd.DataFrame()  # Return an empty DataFrame on error
        else:
            print(f"An error occurred: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on other errors

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on request exceptions

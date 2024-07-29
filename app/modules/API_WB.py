import logging

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


def get_average_storage_cost(testing_mode=False, is_delete_shushary=None):
    df = get_storage_cost(testing_mode, is_delete_shushary)

    df = df.groupby('vendorCode').agg(
        {'warehousePrice': 'mean', 'barcodesCount': 'mean', 'volume': 'mean', 'nmId': 'first'}).reset_index()
    df['storagePricePerBarcode'] = df['warehousePrice'] / df['barcodesCount']

    return df


def get_storage_cost(testing_mode=False, is_delete_shushary=None, number_last_days=app.config['LAST_DAYS_DEFAULT'],
                     days_delay=0,
                     upload_to_yadisk=True):
    print(f"get_storage_cost...")

    if testing_mode:
        logging.warning(f"storage cost is receiving from Yandex Disk")
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
    logging.warning(f"status_code {response.status_code}")
    logging.warning(f"status_code {response.text}")
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
    # logging.warning(f"df {df}")

    if upload_to_yadisk and not df is None:
        io_df = io_output.io_output(df)
        file_name = f'{storage_file_name}.xlsx'
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_STORAGE_COST'])

    if 'warehouse' in df.columns and is_delete_shushary:
        logging.warning("Удаление сгоревших товаров Шушар...")
        df = df[df['warehouse'] != 'Санкт-Петербург Шушары']

    # logging.warning(f"df {df}")
    return df


# def get_wb_price_api():
#     headers = {
#         'accept': 'application/json',
#         'Authorization': app.config['WB_API_TOKEN'],
#     }
#     response = requests.get('https://suppliers-api.wildberries.ru/public/api/v1/info', headers=headers)
#     data = response.json()
#     df = pd.DataFrame(data)
#     df = df.rename(columns={'nmId': 'nm_id'})
#     return df


def get_wb_price_api(request=None, testing_mode=None, is_from_yadisk=None):
    """
    Retrieve information price wb from the Wildberries API.
    """
    logging.warning("get_wb_price_api ...")

    if request: is_from_yadisk = request.form.get('is_from_yadisk')

    if testing_mode or is_from_yadisk:
        logging.warning("testing mode, ...")
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
        logging.warning(f"Response status code: {response.status_code}")

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


# def get_wb_price_api(testing_mode=None):
#     """get_wb_price_api"""
#     headers = {
#         'accept': 'application/json',
#         'Authorization': app.config['WB_API_TOKEN'],
#     }
#     response = requests.get('https://discounts-prices-api.wb.ru/api/v2/list/goods/filter', headers=headers)
#     logging.warning(f"response {get_wb_price_api.__doc__}: {response.status_code}")
#
#     if response.status_code in {200, 201} and not testing_mode:
#         print(f"Failed to fetch data from Wildberries API: {response.text}")
#         data = response.json()
#         if data:  # Check if the response contains data
#             df = pd.DataFrame(data)
#             # df = df.rename(columns={'nmId': 'nm_id'})
#             # Upload data to Yandex Disk
#             file_name = "wb_price_data.xlsx"
#             io_df = io_output.io_output(df)
#             file_name = f'{file_name}.xlsx'
#             yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_PRICES'])
#             return df
#         else:
#             logging.warning("No data received from Wildberries API. Retrieving from Yandex Disk...")
#             df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_PRICES')
#             return df
#     else:
#         logging.warning(f"response text {get_wb_price_api.__doc__}: {response.text}")
#         df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_PRICES')
#         return df


def get_wb_stock_api(request=None, testing_mode=False, is_delete_shushary=True,
                     is_upload_yandex=True, date_from: str = '2019-01-01'):
    """
    get wb stock via api put in df
    :return: df
    """

    logging.warning("get_wb_stock_api ...")

    if testing_mode:
        logging.warning("testing mode, stock from yandex disk ...")
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STOCK_WB')
        if 'warehouseName' in df.columns and is_delete_shushary:
            logging.warning("Удаление сгоревших товаров Шушар из остатков ...")
            df = df[df['warehouseName'] != 'Санкт-Петербург Шушары']
        return df

    logging.warning("stock from API WB ...")
    api_key = app.config['WB_API_TOKEN']
    url = f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}"
    headers = {'Authorization': api_key}

    response = requests.get(url, headers=headers)
    # logging.warning(response)
    df = response.json()
    df = pd.json_normalize(df)
    # df.to_excel("wb_stock.xlsx")

    if 'warehouseName' in df.columns and is_delete_shushary:
        logging.warning("Удаление сгоревших товаров Шушар из остатков ...")
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

    df.to_excel("df_stock.xlsx")

    if no_city == 'no_city' and no_sizes == 'no_sizes':
        df = df.pivot_table(index=['nmId'],
                            values=['quantityFull',
                                    'supplierArticle',
                                    'category',
                                    'subject',
                                    'brand',
                                    ],
                            aggfunc={'quantityFull': sum,
                                     'supplierArticle': 'first',
                                     'category': 'first',
                                     'subject': 'first',
                                     'brand': 'first',
                                     },
                            margins=False)

        df = df.reset_index().rename_axis(None, axis=1)

        if is_upload_yandex and df is not None:
            io_df = io_output.io_output(df)
            file_name = f'stock_wb.xlsx'
            yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_STOCK_WB'])

        return df

    if no_city == 'no_city':
        df = df.pivot_table(index=['nmId', 'techSize'],
                            values=['quantityFull',
                                    'supplierArticle',
                                    'category',
                                    'subject',
                                    'brand',
                                    ],
                            aggfunc={'quantityFull': sum,
                                     'supplierArticle': 'first',
                                     'category': 'first',
                                     'subject': 'first',
                                     'brand': 'first',
                                     },
                            margins=False)

        df = df.reset_index().rename_axis(None, axis=1)
        return df

    if no_sizes == 'no_sizes':
        df = df.pivot_table(index=['nmId', 'warehouseName'],
                            values=['quantityFull',
                                    'supplierArticle',
                                    'category',
                                    'subject',
                                    'brand',
                                    ],
                            aggfunc={'quantityFull': sum,
                                     'supplierArticle': 'first',
                                     'category': 'first',
                                     'subject': 'first',
                                     'brand': 'first',
                                     },
                            margins=False)

        df = df.reset_index().rename_axis(None, axis=1)
        return df

    return df


#
# def get_all_cards_api_wb(textSearch: str = None):
#     logging.warning("get_all_cards_api_wb ...")
#     limit = 1000
#     total = 1000
#     updatedAt = None
#     nmId = None
#     dfs = []
#
#     while total >= limit:
#         headers = {
#             'accept': 'application/json',
#             'Authorization': app.config['WB_API_TOKEN2'],
#         }
#
#         data = {
#             "sort": {
#                 "cursor": {
#                     "limit": limit,
#                     "updatedAt": updatedAt,
#                     "nmID": nmId,
#                 },
#                 "filter": {
#                     "textSearch": textSearch,
#                     "withPhoto": -1
#                 }
#             }
#         }
#
#         # response = requests.post('https://suppliers-api.wildberries.ru/content/v3/cards/cursor/list',
#         #                          data=json.dumps(data), headers=headers)
#
#         response = requests.post('https://suppliers-api.wildberries.ru/content/v2/get/cards/list',
#                                  data=json.dumps(data), headers=headers)
#
#         # logging.warning(f"{response}")
#
#         if response.status_code != 200:
#             logging.warning(f"Error in API request: {response.status_code}")
#             break
#         df_json = response.json()
#         logging.warning(df_json)
#         total = df_json['data']['cursor']['total']
#         updatedAt = df_json['data']['cursor']['updatedAt']
#         nmId = df_json['data']['cursor']['nmID']
#         dfs += df_json['data']['cards']
#
#     df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID'])
#
#     return df


def get_all_cards_api_wb(testing_mode=False, is_from_yadisk=False, is_to_yadisk=False, textSearch: str = None,
                         is_unique=False, limit_cards=None):
    """get_all_cards_api_wb"""

    # print(f"testing_mode or is_from_yadisk {testing_mode} {is_from_yadisk}, {testing_mode or is_from_yadisk} ")

    if testing_mode or is_from_yadisk:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_ALL_CARDS_WB')
        logging.warning("all cards is from yandex disk ...")
        return df

    logging.warning("get_all_cards_api_wb ...")
    limit = 100
    total = 100
    updatedAt = None
    nmId = None
    dfs = []
    count = 0
    while total >= limit:
        logging.warning(f"{get_all_cards_api_wb.__name__} ... total {count}")
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

    logging.warning(f"{get_all_cards_api_wb.__name__} forming df ...")
    df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID', "dimensions", "characteristics"],
                           errors='ignore')

    if is_to_yadisk:
        io_df = io_output.io_output(df)
        file_name = f'all_cards_wb.xlsx'
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_ALL_CARDS_WB'])

    return df


# def get_wb_sales_realization_api(date_from: str, date_end: str, days_step: int):
#     """get sales as api wb sales realization describe"""
#     t = time.process_time()
#     api_key = app.config['WB_API_TOKEN']
#     headers = {'Authorization': api_key}
#     # url = "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod?"
#     url = "https://statistics-api.wildberries.ru/api/v2/supplier/reportDetailByPeriod?"
#
#     url_all = f"{url}dateFrom={date_from}&rrdid=0&dateto={date_end}"
#     response = requests.get(url_all, headers=headers)
#     logging.warning(f"response {response}")
#     df = response.json()
#     df = pd.json_normalize(df)
#
#     return df


def get_wb_sales_realization_api(date_from: str, date_end: str, days_step: int):
    """get sales as api wb sales realization describe"""

    api_key = app.config['WB_API_TOKEN2']
    headers = {'Authorization': api_key}
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod?"
    # url = "https://statistics-api.wildberries.ru/api/v3/supplier/reportDetailByPeriod?"

    url_all = f"{url}dateFrom={date_from}&rrdid=0&dateto={date_end}"
    response = requests.get(url_all, headers=headers)
    logging.warning(f"response {response}")
    logging.warning(f"response.json() {response.json()}")
    df = response.json()
    # logging.warning(df)
    df = pd.json_normalize(df)

    return df


def get_wb_sales_realization_api_v2(date_from: str, date_to: str, days_step: int = 7):
    """Get sales data from the Wildberries API v2"""

    api_key = app.config['WB_API_TOKEN2']  # Assuming you have the API token configured in your app
    headers = {'Authorization': api_key}
    # url = "https://statistics-api.wildberries.ru/api/v3/supplier/reportDetailByPeriod"
    url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

    url_params = {
        'dateFrom': date_from,
        'dateTo': date_to
    }
    print(f"dateFrom {date_from}")

    response = requests.get(url, headers=headers, params=url_params)
    logging.warning(f'response {response}')
    if response.status_code == 200:
        data = response.json()
        logging.warning(f"data {data}")
        df = pd.json_normalize(data)
        return df
    else:
        logging.warning(f"Failed to fetch data for {date_from} to {date_to}")
        return None


def get_wb_sales_funnel_api(request, testing_mode=False, is_erase_points=True, is_re_double=True, is_to_yadisk=True):
    """get_wb_sales_funnel_api"""

    date_from = request_handler.request_date_from(request)
    date_end = request_handler.request_date_end(request)

    if testing_mode:
        logging.warning(f"df downloading in {get_wb_sales_funnel_api.__doc__} from YandexDisk")
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

    logging.warning(f" date_from {date_from}, date_end {date_end}")

    df = _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers)

    # Rename columns (stay only last part before points, for example statistic.order.date to only date)
    if is_erase_points:
        df = df.rename(columns=lambda x: x.split('.')[-1])

    if is_re_double:
        df = _rename_double_columns(df, "_re")

    if is_to_yadisk and df is not None and not df.empty:
        io_df = io_output.io_output(df)
        file_name = f'wb_sales_funnel.xlsx'
        logging.warning(f'df uploading in {get_wb_sales_funnel_api.__doc__} to YandexDisk by name {file_name}')
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_SALES_FUNNEL_WB'])

    # Log a message indicating successful retrieval of sales funnel data
    logging.warning("Sales funnel data retrieved successfully")
    file_name = f'wb_sales_funnel_{str(date_from)[:10]}_{str(date_end[:10])}.xlsx'

    return df, file_name


def _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers, chunk_size=1000):
    df = pd.DataFrame()
    chunks = [nmIDs[i:i + chunk_size] for i in range(0, len(nmIDs), chunk_size)]
    cards_count = chunk_size
    for chunk in chunks:
        logging.warning(f"getting via get_wb_sales_funnel_api {cards_count} ...")

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
        logging.warning(f'response.status_code {get_wb_sales_funnel_api.__doc__}: {response.status_code}')
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

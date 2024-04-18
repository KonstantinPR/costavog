from app import app
import requests
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta
from app.modules import io_output
from app.modules import yandex_disk_handler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO


def get_average_storage_cost(testing_mode=False, is_delete_shushary=None):
    df = get_storage_cost(testing_mode, is_delete_shushary)
    # Step 1: Get the DataFrame
    if df is None:
        return None

    df = df.groupby('vendorCode').agg(
        {'warehousePrice': 'mean', 'barcodesCount': 'mean', 'volume': 'mean', 'nmId': 'first'}).reset_index()
    df['storagePricePerBarcode'] = df['warehousePrice'] / df['barcodesCount']

    # Calculate total_warehouse_price as Decimal
    total_warehouse_price = df['storagePricePerBarcode'].sum()

    # Calculate shareCost using Decimal arithmetic
    # df['shareCost'] = (df['storagePricePerBarcode'] / total_warehouse_price)
    # print(f"df['shareCost'] {df['shareCost'].sum()}")

    return df


# def get_average_storage_cost(testing_mode=False, is_delete_shushary=None):
#     df = get_storage_cost(testing_mode, is_delete_shushary)
#     # Step 1: Get the DataFrame
#     if df is None:
#         return None
#
#     df = df.groupby('vendorCode').agg(
#         {'warehousePrice': 'mean', 'barcodesCount': 'mean', 'volume': 'mean', 'nmId': 'first'}).reset_index()
#     df['storagePricePerBarcode'] = df['warehousePrice'] / df['barcodesCount']
#     total_warehouse_price = Decimal(str(df['storagePricePerBarcode'].sum()))
#     df['shareCost'] = df['storagePricePerBarcode'].apply(lambda x: Decimal(x) / total_warehouse_price)
#
#     return df


# def get_average_storage_data_mean():
#     df = get_storage_data()
#     # Step 1: Get the DataFrame
#     if df is None:
#         return None
#
#     # Step 2: Calculate storage price per barcode count
#     df['storagePricePerBarcode'] = df['warehousePrice'] / df['barcodesCount']
#
#     # Step 3: Group by vendorCode and calculate average storage price per barcode count
#     df_grouped = df.groupby('vendorCode')['storagePricePerBarcode'].mean().reset_index()
#     df = df_grouped.merge(df, how='left', left_on='vendorCode', right_on='vendorCode')
#     df = df[["vendorCode", "volume", "storagePricePerBarcode"]]
#
#     return df


def get_storage_cost(testing_mode, is_delete_shushary=None, number_last_days=app.config['LAST_DAYS_DEFAULT'],
                     days_delay=0,
                     upload_to_yadisk=True):
    if testing_mode:
        print(f"storage cost is receiving from Yandex Disk")
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
    print(f"status_code {response.status_code}")
    if response.status_code not in {200, 201}:  # Check for successful response (200 or 201)
        print("Failed to create report so file will be got from yadisk::", response.text)
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
        return df

    task_id = response.json()['data']['taskId']

    # Step 2: Check report status
    status_url = f'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status'
    while True:
        response = requests.get(status_url, headers=headers)
        if response.status_code not in {200, 201}:
            print("Failed to check report status so file will be got from yadisk:", response.text)
            df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
            return df

        status = response.json()['data']['status']
        if status == 'done':
            break  # Exit loop once report is ready
        elif status == 'error':
            print("Report generation failed.")
            return None

        # Wait and try again after some time
        time.sleep(10)  # Adjust sleep duration as needed

    # Step 3: Download report
    download_url = f'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download'
    response = requests.get(download_url, headers=headers)
    if response.status_code not in {200, 201}:
        print("Failed to download report so file will be got from yadisk::", response.text)
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
        return df

    report_data = response.json()
    print(f"storage cost is receiving from API WB {response.text}")
    # Step 4: Convert data to DataFrame
    df = pd.DataFrame(report_data)
    # print(f"df {df}")

    if upload_to_yadisk and not df is None:
        io_df = io_output.io_output(df)
        file_name = f'{storage_file_name}.xlsx'
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_STORAGE_COST'])

    if is_delete_shushary == 'is_delete_shushary':
        print("Удаление сгоревших товаров Шушар...")
        df = df[df['warehouse'] != 'Санкт-Петербург Шушары']

    # print(f"df {df}")
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

def get_wb_price_api(testing_mode=None):
    headers = {
        'accept': 'application/json',
        'Authorization': app.config['WB_API_TOKEN'],
    }
    response = requests.get('https://suppliers-api.wildberries.ru/public/api/v1/info', headers=headers)
    if response.status_code in {200, 201} and not testing_mode:
        data = response.json()
        if data:  # Check if the response contains data
            df = pd.DataFrame(data)
            # df = df.rename(columns={'nmId': 'nm_id'})
            # Upload data to Yandex Disk
            file_name = "wb_price_data.xlsx"
            io_df = io_output.io_output(df)
            file_name = f'{file_name}.xlsx'
            yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_KEY_PRICES'])
            return df
        else:
            print("No data received from Wildberries API. Retrieving from Yandex Disk...")
            df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_PRICES')
            return df
    else:
        print(f"Failed to fetch data from Wildberries API: {response.text}")
        print("Retrieving from Yandex Disk...")
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_PRICES')
        return df


def get_wb_stock_api(testing_mode=False, request=None, is_delete_shushary=None,
                     is_upload_yandex=True,
                     date_from: str = '2019-01-01'):
    """
    get wb stock via api put in df
    :return: df
    """

    if testing_mode:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STOCK_WB')
        print("testing mode, stock from yandex disk ...")
        return df

    print("stock from API WB ...")
    api_key = app.config['WB_API_TOKEN']
    url = f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}"
    headers = {'Authorization': api_key}

    response = requests.get(url, headers=headers)
    # print(response)
    df = response.json()
    df = pd.json_normalize(df)
    # df.to_excel("wb_stock.xlsx")

    if is_delete_shushary == 'is_delete_shushary':
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
#     print("get_all_cards_api_wb ...")
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
#         # print(f"{response}")
#
#         if response.status_code != 200:
#             print(f"Error in API request: {response.status_code}")
#             break
#         df_json = response.json()
#         print(df_json)
#         total = df_json['data']['cursor']['total']
#         updatedAt = df_json['data']['cursor']['updatedAt']
#         nmId = df_json['data']['cursor']['nmID']
#         dfs += df_json['data']['cards']
#
#     df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID'])
#
#     return df


def get_all_cards_api_wb(testing_mode=False, is_from_yadisk=False, is_to_yadisk=True, textSearch: str = None):
    """get_all_cards_api_wb"""

    if testing_mode or is_from_yadisk:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_ALL_CARDS_WB')
        print("all cards is from yandex disk ...")
        return df

    print("get_all_cards_api_wb ...")
    limit = 1000
    total = 1000
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
            print(f"Error in API request: {response.status_code}")
            break

        df_json = response.json()
        if 'error' in df_json:
            print(f"API Error: {df_json['errorText']}")
            break

        total = df_json['cursor']['total']
        updatedAt = df_json['cursor']['updatedAt']
        nmId = df_json['cursor']['nmID']
        dfs += df_json['cards']
        count = count + total

    print(f"{get_all_cards_api_wb.__name__} forming df ...")
    df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID'], errors='ignore')

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
#     print(f"response {response}")
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
    print(f"response {response}")
    print(f"response.json() {response.json()}")
    df = response.json()
    print(df)
    df = pd.json_normalize(df)

    return df


def get_wb_sales_realization_api_v2(date_from: str, date_to: str, days_step: int = 7):
    """Get sales data from the Wildberries API v2"""

    api_key = app.config['WB_API_TOKEN']  # Assuming you have the API token configured in your app
    headers = {'Authorization': api_key}
    url = "https://statistics-api.wildberries.ru/api/v3/supplier/reportDetailByPeriod"

    url_params = {
        'dateFrom': date_from,
        'dateTo': date_to
    }

    response = requests.get(url, headers=headers, params=url_params)
    logging.info(f'response {response}')
    if response.status_code == 200:
        data = response.json()
        logging.info(f"data {data}")
        df = pd.json_normalize(data)
        return df
    else:
        logging.info(f"Failed to fetch data for {date_from} to {date_to}")
        return None


def get_wb_sales_funnel_api(nmIDs, date_from, date_end, testing_mode=False, is_to_yadisk=True):
    """get_wb_sales_funnel_api"""

    if testing_mode:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_SALES_FUNNEL_WB')
        logging.info(f"df downloading in {get_wb_sales_funnel_api.__doc__} from YandexDisk")
        return df

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

    logging.info(f" {date_from, date_end}")

    df = _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers)

    if is_to_yadisk and df is not None and not df.empty:
        io_df = io_output.io_output(df)
        file_name = f'all_cards_wb.xlsx'
        logging.info(f'df uploading in {get_wb_sales_funnel_api.__doc__} to YandexDisk by name {file_name}')
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['YANDEX_SALES_FUNNEL_WB'])

    return df


def _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers, chunk_size=1000):
    df = pd.DataFrame()
    chunks = [nmIDs[i:i + chunk_size] for i in range(0, len(nmIDs), chunk_size)]
    cards_count = chunk_size
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
        logging.info(f'response {get_wb_sales_funnel_api.__doc__}: {response.status_code}')
        if response.status_code != 200:
            logging.info(f'Error in {get_wb_sales_funnel_api.__doc__}: {response.text}')
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
                logging.info(f'Error parsing response: {e}')
                return None

        logging.info(f"got via get_wb_sales_funnel_api {cards_count}")
        df = pd.concat([df, df_chunk], ignore_index=True)
        cards_count += chunk_size
    return df

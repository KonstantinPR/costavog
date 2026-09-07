import app.modules.request_handler
from app import app
import numpy as np
import json
from app.modules import yandex_disk_handler, pandas_handler, request_handler, sales_report_module
import time
import requests
import logging
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)


def get_storage_cost(testing_mode=False, is_shushary=None, number_last_days=app.config['LAST_DAYS_DEFAULT'],
                     days_delay=0, upload_to_yadisk=True, is_mean=True, is_archive=True):
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
    print(response)
    task_id = response.json()['data']['taskId']
    print(f" task_id {task_id}")

    # Step 2: Check report status
    status_url = f'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status'
    while True:
        response = requests.get(status_url, headers=headers)
        print(f"response.status_code {response.status_code}")
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

    # Step 3: Download report with simple fallback
    download_url = f'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download'

    try:
        response = requests.get(download_url, headers=headers, timeout=30)
        if response.status_code not in {200, 201}:
            raise Exception(f"Download failed with status {response.status_code}")

        report_data = response.json()
    except Exception as e:
        print(f"Download failed with error: {str(e)}")
        logging.warning("Failed to download report, using yadisk backup")
        df, _ = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_STORAGE_COST')
        return df

    report_data = response.json()
    # logging.warning(f"storage cost is receiving from API WB ...")
    # Step 4: Convert data to DataFrame
    df = pd.DataFrame(report_data)

    if 'warehouse' in df.columns and is_shushary:
        print("Удаление сгоревших товаров Шушар...")
        df = df[df['warehouse'] != 'Санкт-Петербург Шушары']

    if is_mean:
        df = df.groupby('vendorCode').agg(
            {'warehousePrice': 'mean', 'barcodesCount': 'mean', 'volume': 'mean', 'nmId': 'first'}).reset_index()
        df['storagePricePerBarcode'] = df['warehousePrice'] / df['barcodesCount']

    if upload_to_yadisk and not df is None:
        file_name = f'{storage_file_name}.xlsx'
        yandex_disk_handler.copy_file_to_archive_folder(path_or_config=app.config['YANDEX_KEY_STORAGE_COST'],
                                                        is_archive=is_archive)
        yandex_disk_handler.upload_to_YandexDisk(file=df, file_name=file_name,
                                                 path=app.config['YANDEX_KEY_STORAGE_COST'])
    print(f"get_storage_cost completed ...")
    return df


def get_wb_price_api(request=None, testing_mode=None, is_from_yadisk=None):
    """
    Retrieve information price wb from the Wildberries API.
    """
    print("get_wb_price_api ...")

    if request:
        is_from_yadisk = request.form.get('is_from_yadisk')

    if testing_mode or is_from_yadisk:
        df, filename = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_KEY_PRICES')
        return df, filename

    url = 'https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter'
    headers = {
        'accept': 'application/json',
        'Authorization': app.config['WB_API_TOKEN'],
    }

    limit = 1000
    offset = 0
    all_goods = []

    # Создаем сессию без агрессивного retry
    session = requests.Session()

    while True:
        params = {'limit': limit, 'offset': offset}

        # ВАЖНО: задержка ДО каждого запроса (кроме первого)
        if offset > 0:
            wait_time = 0.7  # Увеличил с 0.6 до 0.7 секунды
            print(f"Waiting {wait_time}s before next request...")
            time.sleep(wait_time)

        try:
            response = session.get(url, headers=headers, params=params, timeout=30)
            print(f"Offset: {offset}, Status: {response.status_code}")

            # Обработка 429 - ошибка слишком частых запросов
            if response.status_code == 429:
                wait_time = 15  # Ждем 15 секунд при блокировке
                print(f"⚠️ Rate limit exceeded! Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                # Не увеличиваем offset, пробуем тот же запрос снова
                continue

            if response.status_code == 401:
                logging.error("Authorization failed! Check your API token")
                break

            if response.status_code == 403:
                logging.error("Access denied! Check permissions")
                break

            if response.ok:
                data = response.json().get('data')
                if data:
                    list_goods = data.get('listGoods', [])
                    if list_goods:
                        all_goods.extend(list_goods)
                        print(f"✅ Fetched {len(list_goods)} goods, total: {len(all_goods)}")

                        if len(list_goods) < limit:
                            print("📦 Last page reached")
                            break
                        else:
                            offset += limit
                    else:
                        print("ℹ️ No more goods data")
                        break
                else:
                    print("ℹ️ No data in response")
                    break
            else:
                logging.error(f"API error: {response.status_code} - {response.text[:200]}")
                break

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            print(f"❌ Connection error, waiting 10 seconds...")
            time.sleep(10)
            # Не увеличиваем offset, пробуем снова

    print(f"🏁 Finished fetching {len(all_goods)} goods")

    if not all_goods:
        logging.error("No goods fetched from API")
        return pd.DataFrame(), None

    # Нормализация данных
    try:
        df = pd.json_normalize(all_goods, 'sizes', ["vendorCode", 'nmID'], errors='ignore')

        # Конвертируем цены в числа
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['discountedPrice'] = pd.to_numeric(df['discountedPrice'], errors='coerce').fillna(0)

        # Расчет скидки с защитой от деления на ноль
        mask = df['price'] > 0
        df['discount'] = 0.0
        df.loc[mask, 'discount'] = ((df.loc[mask, 'price'] - df.loc[mask, 'discountedPrice']) / df.loc[
            mask, 'price']) * 100
        df['d_disc'] = round(df['discount']).astype(int)

        print(f"📊 Processed {len(df)} rows")

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return pd.DataFrame(), None

    # Сохраняем на Яндекс.Диск
    file_name = "wb_price_data.xlsx"
    try:
        yandex_disk_handler.upload_to_YandexDisk(file=df, file_name=file_name, path=app.config['YANDEX_KEY_PRICES'])
        print(f"💾 Uploaded to Yandex Disk: {file_name}")
    except Exception as e:
        logging.error(f"Failed to upload to Yandex Disk: {e}")

    return df, file_name


def get_wb_stock_api(request=None, testing_mode=False, is_shushary=True, is_upload_yandex=True,
                     date_from: str = '2019-01-01', is_archive=True):
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

    # New endpoint URL
    url = "https://seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses"

    # New headers
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    # Request body
    payload = {
        "limit": 250000,  # Maximum limit
        "offset": 0
    }

    # Make POST request with JSON payload
    response = requests.post(url, headers=headers, json=payload)

    # Check if request was successful
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")
        return None

    # Parse response
    data = response.json()
    # print(data)

    # CRITICAL FIX: Extract items from data['data']['items']
    items_list = data['data']['items']

    print(f"Number of items: {len(items_list)}")
    print(f"First item: {items_list[0] if items_list else 'empty'}")

    # Create DataFrame from items list - THIS WILL WORK NOW
    df = pd.DataFrame(items_list)

    # Debug: Check what we got
    print(f"DataFrame columns: {df.columns.tolist()}")
    print(f"DataFrame shape: {df.shape}")
    print(f"First 3 rows:\n{df.head(3)}")

    # Rename columns to match expected format
    if 'quantityFull' not in df.columns:
        df['quantityFull'] = 0
    if 'quantity' not in df.columns:
        df['quantity'] = 0

    # Make sure inWayToClient and inWayFromClient exist
    if 'inWayToClient' not in df.columns:
        df['inWayToClient'] = 0
    if 'inWayFromClient' not in df.columns:
        df['inWayFromClient'] = 0

    # Filter out Shushary warehouse if needed
    if 'warehouseName' in df.columns and is_shushary:
        print("Удаление сгоревших товаров Шушар из остатков ...")
        df = df[df['warehouseName'] != 'Санкт-Петербург Шушары']
        df = df[df['warehouseName'] != 'Склад СПБ Шушары Московское']  # Also filter this one

    df = df.reset_index(drop=True).rename_axis(None, axis=1)
    df.replace(np.NaN, 0, inplace=True)

    print(f"DataFrame columns after cleanup: {df.columns.tolist()}")
    print(f"DataFrame shape after cleanup: {df.shape}")

    if not request:
        return df

    # Process request parameters
    if hasattr(request, 'form'):
        no_city = request.form.get('no_city')
        no_sizes = request.form.get('no_sizes')
    elif isinstance(request, dict):
        no_city = request.get('no_city')
        no_sizes = request.get('no_sizes')
    else:
        raise ValueError("Invalid request type. It should be either a 'requests' object or a dictionary.")

    print("stock from API WB is gotten")

    # Add missing columns that might be needed for pivot_table
    if 'supplierArticle' not in df.columns:
        df['supplierArticle'] = ''
    if 'category' not in df.columns:
        df['category'] = ''
    if 'subject' not in df.columns:
        df['subject'] = ''
    if 'brand' not in df.columns:
        df['brand'] = ''
    if 'techSize' not in df.columns:
        df['techSize'] = ''

    # Save before pivot to check
    # df.to_excel("df_before_pivot.xlsx")

    # Handle different pivot table scenarios
    if no_city == 'no_city' and no_sizes == 'no_sizes':
        print(no_city)
        print(no_sizes)

        # Now df should have proper columns: nmId, quantityFull, inWayFromClient, inWayToClient, etc.
        df_pivot = df.pivot_table(index=['nmId'],
                                  values=['quantityFull',
                                          'quantity',
                                          'inWayFromClient',
                                          'inWayToClient',
                                          'supplierArticle',
                                          'category',
                                          'subject',
                                          'brand',
                                          ],
                                  aggfunc={'quantityFull': sum,
                                           'quantity': sum,
                                           'inWayFromClient': sum,
                                           'inWayToClient': sum,
                                           'supplierArticle': 'first',
                                           'category': 'first',
                                           'subject': 'first',
                                           'brand': 'first',
                                           },
                                  margins=False)

        df_pivot['quantityFull'] = df_pivot['quantity'] + df_pivot['inWayFromClient'] + df_pivot[
            'inWayToClient']
        df_pivot['quantityWarehouse'] = df_pivot['quantityFull'] - df_pivot['inWayFromClient'] - df_pivot[
            'inWayToClient']
        df_pivot = df_pivot.reset_index().rename_axis(None, axis=1)

        # df_pivot.to_excel("df_after_pivot.xlsx")

        return df_pivot

    if no_city == 'no_city':
        df_pivot = df.pivot_table(index=['nmId', 'techSize'],
                                  values=['quantityFull',
                                          'quantity',
                                          'inWayFromClient',
                                          'inWayToClient',
                                          'supplierArticle',
                                          'category',
                                          'subject',
                                          'brand',
                                          ],
                                  aggfunc={'quantityFull': sum,
                                           'quantity': sum,
                                           'inWayFromClient': sum,
                                           'inWayToClient': sum,
                                           'supplierArticle': 'first',
                                           'category': 'first',
                                           'subject': 'first',
                                           'brand': 'first',
                                           },
                                  margins=False)
        df_pivot['quantityFull'] = df_pivot['quantity'] + df_pivot['inWayFromClient'] + df_pivot[
            'inWayToClient']
        df_pivot['quantityWarehouse'] = df_pivot['quantityFull'] - df_pivot['inWayFromClient'] - df_pivot[
            'inWayToClient']
        df_pivot = df_pivot.reset_index().rename_axis(None, axis=1)

        if is_archive:
            yandex_disk_handler.copy_file_to_archive_folder(path_or_config=app.config['YANDEX_KEY_STOCK_WB'],
                                                            is_archive=is_archive)

        if is_upload_yandex and df_pivot is not None:
            file_name = f'stock_wb.xlsx'
            yandex_disk_handler.upload_to_YandexDisk(file=df_pivot, file_name=file_name,
                                                     path=app.config['YANDEX_KEY_STOCK_WB'])

        return df_pivot

    if no_sizes == 'no_sizes':
        df_pivot = df.pivot_table(index=['nmId', 'warehouseName'],
                                  values=['quantityFull',
                                          'quantity',
                                          'inWayFromClient',
                                          'inWayToClient',
                                          'supplierArticle',
                                          'category',
                                          'subject',
                                          'brand',
                                          ],
                                  aggfunc={'quantityFull': sum,
                                           'quantity': sum,
                                           'inWayFromClient': sum,
                                           'inWayToClient': sum,
                                           'supplierArticle': 'first',
                                           'category': 'first',
                                           'subject': 'first',
                                           'brand': 'first',
                                           },
                                  margins=False)
        df_pivot['quantityFull'] = df_pivot['quantity'] + df_pivot['inWayFromClient'] + df_pivot[
            'inWayToClient']
        df_pivot['quantityWarehouse'] = df_pivot['quantityFull'] - df_pivot['inWayFromClient'] - df_pivot[
            'inWayToClient']
        df_pivot = df_pivot.reset_index().rename_axis(None, axis=1)

        return df_pivot

    return df


def get_all_cards_api_wb(testing_mode=False, is_from_yadisk=False, is_to_yadisk=False, textSearch: str = None,
                         is_unique=False, limit_cards=None, is_unpack=True):
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
            'Authorization': app.config['WB_API_TOKEN'],
        }

        data = {
            "settings": {
                "sort": {
                    "ascending": True
                },
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

        # response = requests.post('https://suppliers-api.wildberries.ru/content/v2/get/cards/list',data=json.dumps(data), headers=headers)
        response = requests.post('https://content-api.wildberries.ru/content/v2/get/cards/list', data=json.dumps(data),
                                 headers=headers)

        if response.status_code != 200:
            logging.warning(f"Error in API request: {response.status_code}")
            print(f"response.statuse_code {response.status_code}")
            print(f"response.text {response.text}")
            break

        df_json = response.json()
        if 'error' in df_json:
            logging.warning(f"API Error: {df_json['errorText']}")
            break

        total = df_json['cursor'].get('total', 0)
        if total == 0:
            break

        updatedAt = df_json['cursor'].get('updatedAt', updatedAt)
        nmId = df_json['cursor'].get('nmID', nmId)
        dfs += df_json['cards']
        count = count + total

        if limit_cards and count > int(limit_cards):
            break

    df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID', "dimensions", "characteristics"],
                           errors='ignore')

    if is_unpack:
        # Unpack dimensions into individual columns
        if 'dimensions' in df.columns:
            dimensions_df = pd.json_normalize(df['dimensions'])
            df = df.drop(columns=['dimensions']).join(dimensions_df)

        # Handle characteristics by creating separate columns for each 'name'
        characteristics_df = df['characteristics'].explode().apply(pd.Series)
        characteristics_df['value'] = characteristics_df['value'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else x)
        characteristics_pivot = characteristics_df.pivot(columns='name', values='value')

        # Join the pivoted characteristics back to the main dataframe
        df = df.drop(columns=['characteristics'])  # drop the original characteristics column
        df = df.join(characteristics_pivot)

        # Handle 'skus' to remove brackets, turning it into a string
        df['skus'] = df['skus'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    if is_to_yadisk:
        file_name = f'all_cards_wb.xlsx'
        yandex_disk_handler.upload_to_YandexDisk(file=df, file_name=file_name, path=app.config['YANDEX_ALL_CARDS_WB'])

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

    nmIDs = [int(nmID) for nmID in df_nmIDs['nmID'].unique() if pd.notnull(nmID)]
    print(f"len of len(nmIDs) is {len(nmIDs)}")
    if is_exclude_nmIDs:
        nmIDs_exclude = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_EXCLUDE_CARDS')[0]['nmID']
        nmIDs = pandas_handler.nmIDs_exclude(nmIDs, nmIDs_exclude)

    with app.app_context():
        api_key = app.config['WB_API_TOKEN']
    url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    # Convert the input date strings to datetime objects
    date_from = datetime.strptime(date_from, "%Y-%m-%d").strftime("%Y-%m-%d")
    date_end = datetime.strptime(date_end, "%Y-%m-%d").strftime("%Y-%m-%d")

    print(f"getting sales funnel by date_from {date_from}, date_end {date_end}")

    df = _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers)

    if df is None:
        print("Error: _sales_funnel_loop_request returned None.  Unable to proceed.")
        return pd.DataFrame(), None  # Return an empty DataFrame and None

    # Rename columns (stay only last part before points, for example statistic.order.date to only date)
    if is_erase_points:
        df = df.rename(columns=lambda x: x.split('.')[-1])

    if is_re_double:
        df = _rename_double_columns(df, "_re")

    if is_to_yadisk and df is not None and not df.empty:
        file_name = f'wb_sales_funnel.xlsx'
        print(f'df uploading in {get_wb_sales_funnel_api.__doc__} to YandexDisk by name {file_name}')
        yandex_disk_handler.upload_to_YandexDisk(file=df, file_name=file_name,
                                                 path=app.config['YANDEX_SALES_FUNNEL_WB'])

    # Log a message indicating successful retrieval of sales funnel data
    print("Sales funnel data retrieved successfully")
    file_name = f'wb_sales_funnel_{str(date_from)[:10]}_{str(date_end[:10])}.xlsx'

    return df, file_name


# def get_wb_sales_funnel_api(request,
#                             testing_mode=False,
#                             is_funnel=True,
#                             is_re_double=True,
#                             is_to_yadisk=True) -> (pd.DataFrame, str):
#     """get_wb_sales_funnel_api"""
#     print("get_wb_sales_funnel_api...")
#     if not is_funnel:
#         return pd.DataFrame, None
#
#     date_from = request_handler.request_date_from(request)
#     date_end = request_handler.request_date_end(request)
#
#     if testing_mode:
#         print(f"df downloading in {get_wb_sales_funnel_api.__doc__} from YandexDisk")
#         df, filename = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_SALES_FUNNEL_WB')
#         return df, filename
#
#     is_from_yadisk = request.form.get('is_from_yadisk')
#     testing_mode = request.form.get('testing_mode')
#     is_erase_points = request.form.get('is_erase_points')
#     is_exclude_nmIDs = request.form.get('is_exclude_nmIDs')
#
#     # Retrieve nmIDs from API and exclude cards from Yandex Disk
#     df_nmIDs = get_all_cards_api_wb(testing_mode=testing_mode, is_from_yadisk=is_from_yadisk)
#     if not 'nmID' in df_nmIDs.columns: logging.warning(f'column nmID in df_nmIDs is not found')
#
#     nmIDs = [int(nmID) for nmID in df_nmIDs['nmID'].unique() if pd.notnull(nmID)]
#     if is_exclude_nmIDs:
#         nmIDs_exclude = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_EXCLUDE_CARDS')[0]['nmID']
#         nmIDs = pandas_handler.nmIDs_exclude(nmIDs, nmIDs_exclude)
#
#     api_key = app.config['WB_API_TOKEN2']
#     # url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"
#     url = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
#     headers = {
#         "Authorization": api_key,
#         "Content-Type": "application/json"
#     }
#
#     # Convert the input date strings to datetime objects
#     date_from = datetime.strptime(date_from, "%Y-%m-%d")
#     date_end = datetime.strptime(date_end, "%Y-%m-%d")
#
#     # Convert the datetime objects to the desired format strings
#     date_from = date_from.strftime("%Y-%m-%d %H:%M:%S")
#     date_end = date_end.strftime("%Y-%m-%d %H:%M:%S")
#
#     print(f"getting sales funnel by date_from {date_from}, date_end {date_end}")
#
#     df = _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers)
#     # df.to_excel("funnel.xlsx")
#
#     # Rename columns (stay only last part before points, for example statistic.order.date to only date)
#     if is_erase_points:
#         df = df.rename(columns=lambda x: x.split('.')[-1])
#
#     if is_re_double:
#         df = _rename_double_columns(df, "_re")
#
#     if is_to_yadisk and df is not None and not df.empty:
#         file_name = f'wb_sales_funnel.xlsx'
#         print(f'df uploading in {get_wb_sales_funnel_api.__doc__} to YandexDisk by name {file_name}')
#         yandex_disk_handler.upload_to_YandexDisk(file=df, file_name=file_name,
#                                                  path=app.config['YANDEX_SALES_FUNNEL_WB'])
#
#     # Log a message indicating successful retrieval of sales funnel data
#     print("Sales funnel data retrieved successfully")
#     file_name = f'wb_sales_funnel_{str(date_from)[:10]}_{str(date_end[:10])}.xlsx'
#
#     return df, file_name


def _sales_funnel_loop_request(nmIDs, date_from, date_end, url, headers, chunk_size=1000):
    df = pd.DataFrame()
    total_cards = len(nmIDs)
    page = 0  # Offset counter
    all_data_retrieved = False

    print(f"Getting sales funnel data for {total_cards} nmIDs in chunks of {chunk_size}...")

    while not all_data_retrieved:
        payload = {
            "selectedPeriod": {
                "start": date_from,
                "end": date_end
            },
            "nmIds": [],  # Get all products (empty list means all)
            "brandNames": [],
            "subjectIds": [],
            "tagIds": [],
            "skipDeletedNm": False,
            "orderBy": {
                "field": "orderCount",
                "mode": "asc"
            },
            "limit": chunk_size,
            "offset": page * chunk_size
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            response_dict = response.json()

            if 'data' in response_dict and 'products' in response_dict['data']:
                cards_data = response_dict['data']['products']
                if not cards_data:  # If no data, we're done
                    all_data_retrieved = True
                    print("No more data to retrieve.")
                    break

                df_chunk = pd.json_normalize(cards_data, errors='ignore')
                df = pd.concat([df, df_chunk], ignore_index=True)
                num_cards = len(cards_data)
                print(f"Processed page {page + 1}, cards: {num_cards} (Total {len(df)} cards)")

                if num_cards < chunk_size:  # THIS MUST BE HERE IF NOT THE LOOP WILL BE INFINITE
                    all_data_retrieved = True
                    print("The end of page")

            else:
                logging.warning(f"No 'data' or 'products' found in response: {response_dict}")
                print("No data found in this chunk.")
                all_data_retrieved = True

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            print(f"Request failed: {e}")
            if hasattr(response, 'json'):
                print(f"Json: {response.json()}")
            else:
                print("No JSON response available")
            return None

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode response: {e}, Response text: {response.text}")
            print(f"Failed to decode JSON response: {e}")
            return None

        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            print(f"An unexpected error occurred: {e}")
            return None

        page += 1
        time.sleep(20)

    print("Sales funnel data retrieval complete.")
    # df.to_excel("funnel_all.xlsx", index=False)  # Write to excel output AFTER the loop
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

    date_from, date_end = request_handler.date_handler(request=request, date_from=date_from, date_end=date_end,
                                                       format_date="%Y-%m-%d")

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

        yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                        path_or_config=app.config['REPORT_SALES_REALIZATION'])

        yandex_disk_handler.upload_to_YandexDisk(request=request, file=df, file_name="report_sales_realization.xlsx",
                                                 path=app.config['REPORT_SALES_REALIZATION'])

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


def get_wb_sales_report(request, do_mapping=True) -> tuple[pd.DataFrame, str]:
    """
    Process the request and return DataFrame and filename.
    If dates are empty, automatically fetches last two weeks of data (14 DAYS).
    """
    try:
        # Get form data
        api_key = request.form.get('api_key')
        if not api_key:
            api_key = app.config.get('WB_API_TOKEN', '')
            if not api_key:
                raise ValueError("API ключ не найден ни в форме, ни в конфигурации")

        date_from = request.form.get('date_from')
        date_to = request.form.get('date_to')

        # Auto-set dates to last two weeks if empty
        if not date_from or not date_to:
            today = datetime.now()
            if not date_to:
                date_to = today.strftime('%Y-%m-%d')
                logger.info(f"Date_to not provided, using today: {date_to}")

            if not date_from:
                date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
                date_from_obj = date_to_obj - timedelta(days=14)
                date_from = date_from_obj.strftime('%Y-%m-%d')
                logger.info(f"Date_from not provided, using two weeks before date_to: {date_from}")

        period = request.form.get('period', 'weekly')
        fields_str = request.form.get('fields', '')
        get_all_pages = request.form.get('get_all_pages', 'false') == 'true'

        # Check for testing mode
        testing_mode = request.form.get('testing_mode') == 'testing_mode'

        if testing_mode:
            logger.info("Testing mode enabled - would use Yandex.Disk here")
            df = pd.DataFrame({'Message': ['Тестовый режим - здесь будут данные с Яндекс.Диска']})
            file_name = f"wb_sales_report_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            return df, file_name

        # Parse fields
        fields = None
        if fields_str and fields_str.strip():
            fields = [f.strip() for f in fields_str.split(',') if f.strip()]

        logger.info(f"Getting WB sales report: {date_from} to {date_to}, period={period}")

        # Fetch data
        if get_all_pages:
            data = sales_report_module.get_all_report_pages(
                api_key=api_key,
                date_from=date_from,
                date_to=date_to,
                period=period,
                fields=fields
            )
        else:
            data = sales_report_module.get_wb_sales_report_detailed(
                api_key=api_key,
                date_from=date_from,
                date_to=date_to,
                period=period,
                fields=fields
            )

        # Convert to DataFrame
        if data:
            df = pd.DataFrame(data)

            # Rename columns
            df = sales_report_module.rename_columns_to_russian(df, do_mapping=do_mapping)
            df = sales_report_module.ensure_expected_headers(df)  # Apply fixes

            # OPTIONAL DEBUGGING - check mapping status
            if do_mapping:
                mapping_summary = sales_report_module.get_mapping_summary(df, do_mapping)
                logger.info(f"Mapping summary: {mapping_summary}")

            # Reorder columns to match old format priority
            priority_columns = [
                'Номер отчёта', 'Начало периода', 'Конец периода', 'Артикул WB',
                'Бренд', 'Название товара', 'Кол-во', 'Вайлдберриз реализовал Товар (Пр)',
                'К перечислению Продавцу за реализованный Товар', 'Дата продажи', 'Дата заказа покупателем'
            ]

            existing_priority = [col for col in priority_columns if col in df.columns]
            other_columns = [col for col in df.columns if col not in existing_priority]

            if existing_priority:
                df = df[existing_priority + other_columns]

            # Convert date columns and remove timezone
            date_columns = ['Начало периода', 'Конец периода', 'Дата формирования', 'Дата операции']
            datetime_columns = ['Дата заказа покупателем', 'Дата продажи']

            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)

            for col in datetime_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)

            # Generate filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_name = f"wb_sales_report_{date_from}_to_{date_to}_{timestamp}.xlsx"

            logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        else:
            df = pd.DataFrame({'Message': ['Нет данных за указанный период']})
            file_name = f"wb_sales_report_no_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return df, file_name

    except Exception as e:
        logger.error(f"Error in get_wb_sales_report: {str(e)}")
        error_df = pd.DataFrame({'Error': [str(e)]})
        file_name = f"wb_sales_report_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return error_df, file_name

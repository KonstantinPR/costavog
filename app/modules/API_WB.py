from app import app
import requests
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta


def get_storage_data(date_from=None, date_to=None):
    headers = {
        'accept': 'application/json',
        'Authorization': app.config['WB_API_TOKEN'],  # Uncomment and adjust this line if you're using Flask
    }

    # Set default date_from to a week ago if not provided
    if date_from is None:
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    # Set default date_to to today if not provided
    if date_to is None:
        date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Step 1: Create a report
    create_report_url = 'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage'
    params = {
        'dateFrom': date_from,
        'dateTo': date_to
    }
    response = requests.get(create_report_url, headers=headers, params=params)
    print(f"status_code {response.status_code}")
    if response.status_code not in {200, 201}:  # Check for successful response (200 or 201)
        print("Failed to create report:", response.text)
        return None

    task_id = response.json()['data']['taskId']

    # Step 2: Check report status
    status_url = f'https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status'
    while True:
        response = requests.get(status_url, headers=headers)
        if response.status_code not in {200, 201}:
            print("Failed to check report status:", response.text)
            return None

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
        print("Failed to download report:", response.text)
        return None

    report_data = response.json()

    # Step 4: Convert data to DataFrame
    df = pd.DataFrame(report_data)

    return df


def get_wb_price_api(g=None):
    headers = {
        'accept': 'application/json',
        # 'Authorization': app.config['WB_API_TOKEN2'],
        'Authorization': app.config['WB_API_TOKEN'],
    }

    response = requests.get('https://suppliers-api.wildberries.ru/public/api/v1/info', headers=headers)
    data = response.json()
    df = pd.DataFrame(data)
    df = df.rename(columns={'nmId': 'nm_id'})
    return df


def get_wb_stock_api_extanded():
    """to modify wb stock"""

    df = df_wb_stock_api()

    # df.to_excel("df_wb_stock_api.xlsx")

    df = df.pivot_table(index=['nmId'],
                        values=['quantity',
                                # 'daysOnSite',
                                'supplierArticle',
                                ],
                        aggfunc={'quantity': sum,
                                 # 'daysOnSite': max,
                                 'supplierArticle': max,
                                 },
                        margins=False)

    df = df.reset_index().rename_axis(None, axis=1)
    df = df.rename(columns={'nmId': 'nm_id'})
    df.replace(np.NaN, 0, inplace=True)

    return df


def df_wb_stock_api(date_from: str = '2019-01-01'):
    """
    get wb stock via api put in df
    :return: df
    """

    api_key = app.config['WB_API_TOKEN']
    url = f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}"
    headers = {'Authorization': api_key}

    response = requests.get(url, headers=headers)
    # print(response)
    df = response.json()
    df = pd.json_normalize(df)
    # df.to_excel("wb_stock.xlsx")

    return df


def get_all_cards_api_wb(textSearch: str = None):
    print("get_all_cards_api_wb ...")
    limit = 1000
    total = 1000
    updatedAt = None
    nmId = None
    dfs = []

    while total >= limit:
        headers = {
            'accept': 'application/json',
            'Authorization': app.config['WB_API_TOKEN2'],
        }

        data = {
            "sort": {
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

        response = requests.post('https://suppliers-api.wildberries.ru/content/v1/cards/cursor/list',
                                 data=json.dumps(data), headers=headers)

        if response.status_code != 200:
            print(f"Error in API request: {response.status_code}")
            break
        df_json = response.json()
        total = df_json['data']['cursor']['total']
        updatedAt = df_json['data']['cursor']['updatedAt']
        nmId = df_json['data']['cursor']['nmID']
        dfs += df_json['data']['cards']

    df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID'])

    return df


def get_wb_sales_realization_api(date_from: str, date_end: str, days_step: int):
    """get sales as api wb sales realization describe"""
    t = time.process_time()
    api_key = app.config['WB_API_TOKEN']
    headers = {'Authorization': api_key}
    # url = "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod?"
    url = "https://statistics-api.wildberries.ru/api/v2/supplier/reportDetailByPeriod?"

    url_all = f"{url}dateFrom={date_from}&rrdid=0&dateto={date_end}"
    response = requests.get(url_all, headers=headers)
    print(f"response {response}")
    df = response.json()
    df = pd.json_normalize(df)

    return df

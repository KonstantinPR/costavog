from app import app
import requests
import pandas as pd
import numpy as np
import time
import json


def get_wb_stock_api_extanded():
    """to modify wb stock"""

    df = df_wb_stock_api()

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
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod?"
    url_all = f"{url}dateFrom={date_from}&rrdid=0&dateto={date_end}"
    response = requests.get(url_all, headers=headers)
    df = response.json()
    df = pd.json_normalize(df)

    return df

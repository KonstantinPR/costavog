from app import app
from functools import reduce
import math
import requests
from app.models import Product, db
import datetime
import pandas as pd
import numpy as np
from app.modules import yandex_disk_handler, detailing_reports
from app.modules import io_output
import time
import json


def get_wb_stock_api():
    """to modify wb stock"""

    df = df_wb_stock_api()

    df = df.pivot_table(index=['nmId'],
                        values=['quantity',
                                'daysOnSite',
                                'supplierArticle',
                                ],
                        aggfunc={'quantity': sum,
                                 'daysOnSite': max,
                                 'supplierArticle': max,
                                 },
                        margins=False)

    df = df.reset_index().rename_axis(None, axis=1)
    df = df.rename(columns={'nmId': 'nm_id'})
    df.replace(np.NaN, 0, inplace=True)

    return df


def df_wb_stock_api(date_from: str = '2018-06-24T21:00:00.000Z'):
    """
    get wb stock via api put in df
    :return: df
    """
    t = time.process_time()
    date_from = date_from

    api_key = app.config['WB_API_TOKEN']
    path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?"
    # print(time.process_time() - t)
    path_all = f"{path_start}dateFrom=2018-06-24T21:00:00.000Z&key={api_key}"
    response = requests.get(path_all)
    data = response.json()
    df = pd.DataFrame(data)
    return df


def get_all_cards_api_wb(textSearch: str = None):
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
                    "limit": total,
                    "updatedAt": updatedAt,
                    "nmID": nmId,
                },
                "filter": {
                    "textSearch": textSearch,
                    "withPhoto": -1
                }
            }
        }

        data = json.dumps(data)
        url = 'https://suppliers-api.wildberries.ru/content/v1/cards/cursor/list'

        response = requests.post(url, data=data, headers=headers)

        print(type(response))
        # print(response)
        df_json = response.json()
        # print(type(df_json))
        # print(df_json)
        # print(df_json['data']['cursor']['total'])
        # print(df_json['data']['cursor']['updatedAt'])
        # print(df_json['data']['cursor']['nmID'])

        total = df_json['data']['cursor']['total']
        updatedAt = df_json['data']['cursor']['updatedAt']
        nmId = df_json['data']['cursor']['nmID']
        # df = pd.DataFrame(df_json['data']['cards'])
        dfs = dfs + df_json['data']['cards']

    # df = pd.concat(dfs)
    # dfs = dfs.explode('sizes')
    # df = dfs.join(pd.json_normalize(dfs.pop('sizes')))
    # df_n = pd.json_normalize(dfs, 'sizes')
    # df = dfs.join(df_n)
    df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID'])

    # df['sizes'] = [list_dict_to_str(x) for x in df['sizes']]

    # df = df.rename(columns={'nmId': 'nm_id'})
    return df


def get_wb_sales_realization_api(date_from: str, date_end: str, days_step: int):
    """get sales as api wb sales realization describe"""
    t = time.process_time()
    path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?"
    date_from = date_from
    api_key = app.config['WB_API_TOKEN']
    # print(time.process_time() - t)
    limit = 100000
    path_all = f"{path_start}dateFrom={date_from}&key={api_key}&limit={limit}&rrdid=0&dateto={date_end}"
    # path_all_test = f"https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom=2022-06-01&key={api_key}&limit=1000&rrdid=0&dateto=2022-06-25"
    # print(time.process_time() - t)
    response = requests.get(path_all)
    # print(response)
    # print(time.process_time() - t)
    data = response.json()
    # print(time.process_time() - t)
    df = pd.DataFrame(data)
    # print(time.process_time() - t)

    return df

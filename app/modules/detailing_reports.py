import os
from app import app
import zipfile
import pandas as pd
import numpy as np
import os
import io
from app.modules import io_output
from os import listdir
import datetime
from datetime import datetime
import requests
import time
from functools import reduce


def get_wb_sales_realization_api(date_from: str, date_end: str, days_step: int):
    """get sales as api wb sales realization describe"""
    t = time.process_time()
    path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?"
    date_from = date_from
    api_key = app.config['WB_API_TOKEN']
    print(time.process_time() - t)
    limit = 100000
    path_all = f"{path_start}dateFrom={date_from}&key={api_key}&limit={limit}&rrdid=0&dateto={date_end}"
    # path_all_test = f"https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom=2022-06-01&key={api_key}&limit=1000&rrdid=0&dateto=2022-06-25"
    print(time.process_time() - t)
    response = requests.get(path_all)
    print(time.process_time() - t)
    data = response.json()
    print(time.process_time() - t)
    df = pd.DataFrame(data)
    print(time.process_time() - t)

    return df


# def df_merge(df_list, ):
#     df_merged = reduce(lambda left, right: pd.merge(left, right, on=['DATE'],
#                                                     how='outer'), df_list).fillna('void')
#     return df_merged

def get_important_columns(df):
    pass


def get_wb_sales_realization_pivot(df):
    df1 = df.pivot_table(index=['nm_id'],
                         columns='supplier_oper_name',
                         values=['ppvz_for_pay',
                                 'delivery_rub',
                                 'penalty',
                                 'quantity',
                                 'retail_price_withdisc_rub'
                                 ],
                         aggfunc={'ppvz_for_pay': sum,
                                  'delivery_rub': sum,
                                  'penalty': sum,
                                  'quantity': sum,
                                  'retail_price_withdisc_rub': sum,
                                  },
                         margins=False)
    print(df1)

    df2 = df.pivot_table(index=['nm_id'],
                         values=['sa_name',
                                 'brand_name', ],
                         aggfunc={'sa_name': max,
                                  'brand_name': max, },
                         margins=False)

    df = df1.merge(df2, how='left', on='nm_id')

    return df


def get_wb_stock_api(date_from: str, date_end: str, days_step: int):
    """get sales as api wb sales realization describe"""
    t = time.process_time()
    path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?"
    date_from = date_from
    api_key = app.config['WB_API_TOKEN']
    print(time.process_time() - t)
    path_all = f"https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?dateFrom=2018-06-24T21:00:00.000Z&key={api_key}"
    # path_all_test = f"https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom=2022-06-01&key={api_key}&limit=1000&rrdid=0&dateto=2022-06-25"
    print(time.process_time() - t)
    response = requests.get(path_all)
    print(time.process_time() - t)
    data = response.json()
    print(time.process_time() - t)
    df = pd.DataFrame(data)
    print(time.process_time() - t)
    df = df.pivot_table(index=['supplierArticle'],
                        values=['quantityFull',
                                'daysOnSite',
                                ],
                        aggfunc={'quantityFull': sum,
                                 'daysOnSite': max, },
                        margins=False)

    return df


def get_wb_sales_realization_pivot2(df):
    df_pivot_sells_sum = df[df['supplier_oper_name'] == 'Продажа'].pivot_table(index=['nm_id'],
                                                                               values=['ppvz_for_pay'],
                                                                               aggfunc={'ppvz_for_pay': sum},
                                                                               margins=False)

    df_pivot_correct_sells_sum = df[df['supplier_oper_name'] == 'Продажа'].pivot_table(index=['nm_id'],
                                                                                       values=['ppvz_for_pay'],
                                                                                       aggfunc={'ppvz_for_pay': sum},
                                                                                       margins=False)

    df_pivot_returns_sells_sum = df[df['supplier_oper_name'] == 'Возврат'].pivot_table(
        index=['nm_id'],
        values=['ppvz_for_pay'],
        aggfunc={'ppvz_for_pay': sum},
        margins=False)

    df_pivot_correct_return_returns_sum = df[df['supplier_oper_name'] == 'Корректный возврат'].pivot_table(
        index=['nm_id'],
        values=['ppvz_for_pay'],
        aggfunc={'ppvz_for_pay': sum},
        margins=False)

    df_pivot_penalty_sum = df[df['supplier_oper_name'] == 'Штрафы'].pivot_table(
        index=['nm_id'],
        values=['penalty'],
        aggfunc={'penalty': sum},
        margins=False)

    df_pivot_logistic_sum = df[df['supplier_oper_name'] == 'Логистика'].pivot_table(index=['nm_id'],
                                                                                    values=['delivery_rub'],
                                                                                    aggfunc={'delivery_rub': sum},
                                                                                    margins=False)

    df_pivot_reversal_sales_sum = df[df['supplier_oper_name'] == 'Продажа'].pivot_table(index=['nm_id'],
                                                                                        values=['ppvz_for_pay'],
                                                                                        aggfunc={'ppvz_for_pay': sum},
                                                                                        margins=False)

    dfs = [df_pivot_sells_sum,
           df_pivot_correct_sells_sum,
           df_pivot_returns_sells_sum,
           df_pivot_correct_return_returns_sum,
           df_pivot_penalty_sum,
           df_pivot_logistic_sum,
           df_pivot_reversal_sales_sum, ]

    df = reduce(lambda left, right: pd.merge(left, right, on=['nm_id'],
                                             how='outer'), dfs)

    # df_pivot = df.pivot_table(index=['nm_id'],
    #                           values=['ppvz_for_pay',
    #                                   'delivery_rub', ],
    #                           aggfunc={'ppvz_for_pay': sum,
    #                                    'delivery_rub': sum, },
    #                           margins=False)

    return df

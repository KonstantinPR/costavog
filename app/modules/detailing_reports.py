import os
from app import app
import zipfile
import pandas as pd
import numpy as np
import os
import io
from app.modules import io_output
from os import listdir
from datetime import datetime, timedelta
from app.models import Company, UserModel, Transaction, Task, Product, db
import requests
import time
from functools import reduce
from copy import copy


def dataframe_divide(df, period_dates_list, date_from, date_format="%Y-%m-%d"):
    df['rr_dt'] = [x[0:10] + " 00:00:00" for x in df['rr_dt']]
    df['rr_dt'] = pd.to_datetime(df['rr_dt'])
    # df = df.set_index(df['rr_dt'])
    # df = df.sort_index()
    print(df)

    if isinstance(date_from, str):
        date_from = datetime.strptime(date_from, date_format)

    df_list = []

    for date_end in period_dates_list:
        print(f"from df date {date_from}")
        print(f"end df date {date_end}")

        # df = df[date_from:date_end]

        d = df[(df['rr_dt'] > date_from) & (df['rr_dt'] <= date_end)]
        print(f"d {d}")
        date_from = date_end
        df_list.append(d)

    return df_list


def get_period_dates_list(date_from, date_end, days_bunch, date_parts=1, days_step=7, date_format="%Y-%m-%d"):
    period_dates_list = []
    date_from = datetime.strptime(date_from, date_format)
    date_end = datetime.strptime(date_end, date_format)
    date_end_local = date_from + timedelta(days_bunch)

    print(type(date_end_local))
    print(type(date_end))

    print(f"date_end_local {date_end_local}\n")
    while date_end_local <= date_end:
        period_dates_list.append(date_end_local)
        print(f"type per list {period_dates_list}")
        print(f"date_parts {date_parts}")
        print(f"date_end_local {date_end_local}")
        print(f"days bunch {days_bunch}")
        date_end_local = date_end_local + timedelta(days_bunch)
        date_end_local = datetime(date_end_local.year, date_end_local.month, date_end_local.day)
        print(f"date_local_end {date_end_local}\n")
        print(type(date_end_local))
        print(f"date_end {date_end}\n")

    return period_dates_list


def get_days_bunch_from_delta_date(date_from, date_end, date_parts, date_format="%Y-%m-%d"):
    print(date_from)
    print(date_end)
    date_format = "%Y-%m-%d"
    if not date_parts:
        date_parts = 1
    delta = datetime.strptime(date_end, date_format) - datetime.strptime(date_from, date_format)
    delta = delta.days

    days_bunch = int(int(delta) / int(date_parts))
    return days_bunch


def combine_date_to_revenue(date_from, date_end, days_step=7):
    df = get_wb_sales_realization_api(date_from, date_end, days_step)
    df_sales = get_wb_sales_realization_pivot(df)
    df_stock = get_wb_stock_api(date_from, date_end, days_step)
    df_net_cost = pd.read_sql(
        db.session.query(Product).filter_by(company_id=app.config['CURRENT_COMPANY_ID']).statement, db.session.bind)
    df = df_sales.merge(df_stock, how='outer', on='nm_id')
    df = df.merge(df_net_cost, how='outer', left_on='supplierArticle', right_on='article')
    df = get_revenue_column(df)
    return df


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

def revenue_correcting(x, y, z, w):
    if z > 0:
        return x - y
    else:
        return x - w


def get_important_columns(df):
    df = df[[
        'brand_name',
        'subject_name',
        'nm_id',
        'supplierArticle',
        'Прибыль',
        ('ppvz_for_pay', 'Продажа'),
        ('retail_price_withdisc_rub', 'Продажа'),
        ('ppvz_for_pay', 'Возврат'),
        ('ppvz_for_pay', 'Логистика'),
        ('quantity', 'Продажа'),
        ('quantity', 'Возврат'),
        ('quantity', 'Логистика'),
        'net_cost',
        ('delivery_rub', 'Возврат'),
        ('delivery_rub', 'Логистика'),
        ('delivery_rub', 'Продажа'),
        ('penalty', 'Возврат'),
        ('penalty', 'Логистика'),
        ('penalty', 'Продажа'),
        ('retail_price_withdisc_rub', 'Возврат'),
        ('retail_price_withdisc_rub', 'Логистика'),
        ('delivery_amount', 'Логистика'),
        ('return_amount', 'Логистика'),
        'daysOnSite',
        'quantityFull',
        'article',
        'company_id',
        'sa_name',
    ]]
    print(df)
    return df


def get_revenue_column_by_part(df, period_dates_list=None):
    df.replace(np.NaN, 0, inplace=True)

    for date in period_dates_list:
        # if period_dates_list.index(date) == 0:
        #     date = ''
        # else:
        date = f"_{str(date)[:10]}"

        df[f'Прибыль{date}'] = df[f"('ppvz_for_pay', 'Продажа'){date}"] - \
                               df[f"('ppvz_for_pay', 'Возврат'){date}"] - \
                               df[f"('delivery_rub', 'Логистика'){date}"]

        df[f'Прибыль{date}'] = [revenue_correcting(x, y, z, w)
                                for x, y, z, w
                                in zip(
                df[f'Прибыль{date}'],
                df['net_cost'],
                df[f"('ppvz_for_pay', 'Продажа'){date}"],
                df[f"('delivery_rub', 'Логистика'){date}"],
            )]

        # df['supplierArticle'] = [x for x in df['sa_name'] if x != 0]

    return df


def get_revenue_column(df):
    df.replace(np.NaN, 0, inplace=True)

    df['Прибыль'] = df[('ppvz_for_pay', 'Продажа')] - \
                    df[('ppvz_for_pay', 'Возврат')] - \
                    df[('delivery_rub', 'Логистика')]

    df['Прибыль'] = [revenue_correcting(x, y, z, w) for x, y, z, w in zip(df['Прибыль'],
                                                                          df['net_cost'],
                                                                          df[('ppvz_for_pay', 'Продажа')],
                                                                          df[('delivery_rub', 'Логистика')],
                                                                          )]

    # df['supplierArticle'] = [x for x in df['sa_name'] if x != 0]

    return df


def get_wb_sales_realization_pivot(df):
    df1 = df.pivot_table(index=['nm_id'],
                         columns='supplier_oper_name',
                         values=['ppvz_for_pay',
                                 'delivery_rub',
                                 'penalty',
                                 'quantity',
                                 'delivery_amount',
                                 'return_amount',
                                 'retail_price_withdisc_rub'
                                 ],
                         aggfunc={'ppvz_for_pay': sum,
                                  'delivery_rub': sum,
                                  'penalty': sum,
                                  'quantity': sum,
                                  'delivery_amount': sum,
                                  'return_amount': sum,
                                  'retail_price_withdisc_rub': sum,
                                  },
                         margins=False)
    print(df1)

    df2 = df.pivot_table(index=['nm_id'],
                         values=['sa_name',
                                 'brand_name',
                                 'subject_name'],
                         aggfunc={'sa_name': max,
                                  'brand_name': max,
                                  'subject_name': max,
                                  },
                         margins=False)

    df = df1.merge(df2, how='left', on='nm_id')

    return df


def get_wb_stock_api(date_from: str = '2018-06-24T21:00:00.000Z'):
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
    print(df)
    print(time.process_time() - t)
    df = df.pivot_table(index=['nmId'],
                        values=['quantityFull',
                                'daysOnSite',
                                'supplierArticle',
                                ],
                        aggfunc={'quantityFull': sum,
                                 'daysOnSite': max,
                                 'supplierArticle': max,
                                 },
                        margins=False)
    df = df.reset_index().rename_axis(None, axis=1)
    df = df.rename(columns={'nmId': 'nm_id'})

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

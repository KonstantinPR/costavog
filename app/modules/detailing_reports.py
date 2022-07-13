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
import re
import math

IMPORTANT_COL_DESC = [
    'brand_name',
    'subject_name',
    'nm_id',
    'supplierArticle',
]

IMPORTANT_COL_REPORT = [
    'Согласованная скидка, %',
    'discount',
    'Перечисление руб',
    'Логистика руб',
    'Логистика шт',
    'price_disc',
    'net_cost',
    'quantity_Возврат_sum',
    'quantity_Продажа_sum',
    'quantityFull',
    'k_discount',
    'k_is_sell',
    'k_revenue',
    'k_logistic',
    'k_net_cost',
]

NEW_COL_ON_REVENUE = [

]

DEFAULT_NET_COST = 1000


# /// --- K REVENUE FORMING ---
def k_is_sell(sell_sum, qt_full):
    # нет продаж и товара много
    k = 1
    if not sell_sum:
        k = 1.02
    if sell_sum <= 5:
        k = 1
    if sell_sum > 5:
        k = 0.99
    if sell_sum > 10:
        k = 0.98
    if qt_full <= 10:
        return 1.01 * k
    if 10 < qt_full <= 50:
        return 1.02 * k
    if 50 < qt_full <= 100:
        return 1.05 * k
    if 100 < qt_full <= 1000:
        return 1.1 * k
    return 1


def k_revenue(sum, mean, last):
    if sum > 0 and mean > 0 and last > 0:
        return 0.98
    if sum < 0 and mean < 0 and last < 0:
        return 0.9
    if sum > 0 and mean > 0 and last < 0:
        return 0.95
    return 1


def k_logistic(log_rub, to_rub):
    # каково отношение денег к перечислению и денег, потраченных на логистику:
    if to_rub == 0:
        return 1
    k_log = log_rub / to_rub
    # в зависимости от цены товара (чем дороже - тем больше можно возить без вреда на прибыльности)
    if k_log > 1 or k_log < 0:
        # если логистика = всему что к перечислению, то уменьшаем скидку пополам
        return 0.50
    if k_log > 0.5:
        # если логистика = половине от перечисляемого, то уменьшаем скидку в 2 раза
        return 0.80
    if k_log > 0.25:
        # если логистика = четверти от перечисляемого, то уменьшаем скидку на четверть
        return 0.98
    # в остальных случаях оставляем скидку без изменения
    return 1


def k_net_cost(net_cost, price_disc):
    if net_cost == 0:
        net_cost = DEFAULT_NET_COST
    if price_disc <= net_cost:
        return 0.80
    if price_disc >= net_cost * 2 and net_cost < 1000:
        return 1.01
    if price_disc >= net_cost * 3 and net_cost < 1000:
        return 1.02
    if price_disc >= net_cost * 2 and net_cost >= 1000:
        return 1.03
    return 1


def get_k_discount(df, df_revenue_col_name_list):
    # если не было продаж увеличиваем скидку
    df['k_is_sell'] = [k_is_sell(x, y) for x, y in zip(df['quantity_Продажа_sum'], df['quantityFull'])]
    # постоянно растет или падает прибыль, отрицательная или положительная
    df['k_revenue'] = [k_revenue(x, y, z) for x, y, z in zip(df['Прибыль_sum'], df['Прибыль_mean'], df['Прибыль_last'])]
    # Защита от покатушек - поднимаем цену
    df['k_logistic'] = [k_logistic(x, y) for x, y in zip(df['Логистика руб'], df['Перечисление руб'])]
    # Защита от цены ниже себестоимости - тогда повышаем
    df['k_net_cost'] = [k_net_cost(x, y) for x, y in zip(df['net_cost'], df['price_disc'])]
    df['k_discount'] = df['k_is_sell'] * df['k_revenue'] * df['k_logistic'] * df['k_net_cost']

    return df


# --- K REVENUE FORMING /// ---

# /// --- NEW COLUMN ON REVENUE ANILIZE ---

def df_revenue_growth(df, df_revenue_col_name_list):
    growth1 = df[df_revenue_col_name_list[0]] - df[df_revenue_col_name_list[1]]
    growth2 = df[df_revenue_col_name_list[1]] - df[df_revenue_col_name_list[2]]
    growth = (growth2 - growth1) / growth2
    return growth


def df_revenue_col_name_list(df):
    df_revenue_col_name_list = [col for col in df.columns if f'Прибыль_' in col]
    return df_revenue_col_name_list


# --- NEW COLUMN ON REVENUE ANILIZE /// ---

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


def get_period_dates_list(date_from, date_end, days_bunch, date_parts=1, date_format="%Y-%m-%d"):
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
    df_stock = get_wb_stock_api(date_from)
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
        return x


def get_important_columns(df):
    df = df[[
        'brand_name',
        'subject_name',
        'nm_id',
        'supplierArticle',
        'Прибыль',
        'ppvz_for_pay_Продажа',
        'retail_price_withdisc_rub_Продажа',
        'ppvz_for_pay_Возврат',
        'ppvz_for_pay_Логистика',
        'quantity_Продажа',
        'quantity_Возврат',
        'quantity_Логистика',
        'net_cost',
        'delivery_rub_Возврат',
        'delivery_rub_Логистика',
        'delivery_rub_Продажа',
        'penalty_Возврат',
        'penalty_Логистика',
        'penalty_Продажа',
        'retail_price_withdisc_rub_Возврат',
        'retail_price_withdisc_rub_Логистика',
        'delivery_amount_Логистика',
        'return_amount_Логистика',
        'daysOnSite',
        'quantityFull',
        'article',
        'company_id',
        'sa_name',
    ]]
    print(df)
    return df


def df_reorder_important_col_desc_first(df):
    important_col_list = IMPORTANT_COL_DESC
    n = 0
    col_list = df.columns.tolist()
    for col in col_list:
        if col in important_col_list:
            idx = col_list.index(col)
            col_list[idx], col_list[n] = col_list[n], col_list[idx]
            n += 1
    df = df.reindex(columns=col_list)
    return df


def df_reorder_important_col_report_first(df):
    important_col_list = IMPORTANT_COL_REPORT
    n = len(IMPORTANT_COL_DESC) - 1
    col_list = df.columns.tolist()
    for col in col_list:
        if col in important_col_list:
            idx = col_list.index(col)
            col_list[idx], col_list[n] = col_list[n], col_list[idx]
            n += 1
    df = df.reindex(columns=col_list)
    return df


def df_reorder_revenue_col_first(df):
    n = len(IMPORTANT_COL_DESC) + len(IMPORTANT_COL_REPORT) - 1
    col_list = df.columns.tolist()
    for col in col_list:
        if "Прибыль" in col:
            idx = col_list.index(col)
            col_list[idx], col_list[n] = col_list[n], col_list[idx]
            n += 1
    df = df.reindex(columns=col_list)
    return df


def df_stay_not_null(df):
    df = df.loc[:, df.any()]
    return df


def get_revenue_by_part(df, period_dates_list=None):
    df.replace(np.NaN, 0, inplace=True)

    for date in period_dates_list:
        if period_dates_list.index(date) == 0:
            date = ''
        else:
            date = f"_{str(date)[:10]}"

        df[f'Прибыль{date}'] = df[f'ppvz_for_pay_Продажа{date}'] - \
                               df[f'ppvz_for_pay_Возврат{date}'] - \
                               df[f'delivery_rub_Логистика{date}'] - \
                               df[f'quantity_Продажа{date}'] * df['net_cost'] + \
                               df[f'quantity_Возврат{date}'] * df['net_cost']

    return df


def get_revenue_column(df):
    df.replace(np.NaN, 0, inplace=True)

    df['Прибыль'] = df['ppvz_for_pay_Продажа'] - \
                    df['ppvz_for_pay_Возврат'] - \
                    df['delivery_rub_Логистика'] - \
                    df['quantity_Продажа'] * df['net_cost'] + \
                    df['quantity_Возврат'] * df['net_cost']

    return df


def df_column_set_to_str(df):
    for col in df.columns:
        if isinstance(col, tuple):
            df.rename(columns={col: '_'.join(col)}, inplace=True)
    return df


def _change_old_column_name(df):
    # соединяем старые названия возврата - корректный вовзрат и продажа - корректная продажа
    if 'ppvz_for_pay_Корректная продажа' in df:
        df['ppvz_for_pay_Продажа'] = df['ppvz_for_pay_Корректная продажа'] + df['ppvz_for_pay_Продажа']
    if 'ppvz_for_pay_Корректный возврат' in df:
        df['ppvz_for_pay_Возврат'] = df['ppvz_for_pay_Корректный возврат'] + df['ppvz_for_pay_Возврат']
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
    df = df_column_set_to_str(df)
    df = _change_old_column_name(df)

    return df


def get_wb_price_api():
    headers = {
        'accept': 'application/json',
        'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjI3YzViYzIzLThlNDktNDNjMy04YTA2LWQ0MDI0ZjRmZDM2ZiJ9._bCg_tfpB1D1TRggc7pOeCWeFKCPy2IQr4FTz8HTk34',
    }

    response = requests.get('https://suppliers-api.wildberries.ru/public/api/v1/info', headers=headers)
    data = response.json()
    df = pd.DataFrame(data)
    df = df.rename(columns={'nmId': 'nm_id'})
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

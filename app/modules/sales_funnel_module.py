import logging
from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
import datetime
import time
from app.modules import API_WB, detailing_api_module
from app.modules import io_output, yandex_disk_handler, request_handler, pandas_handler
import math
import numpy as np
import pandas as pd

DISCOUNT_COLUMNS = {
    'buyoutsCount': 'buyoutsCount',
    'ordersCount': 'ordersCount',
    'quantityFull': 'stocksWb',
    'price': 'price',
    'func_discount': 'func_discount',
    'discount': 'discount',
}


def calculate_discount(df, p_buy=1.3, p_order=1.1, p_qt=0.9, d_sum=100, n_net=8, smooth_days=1,
                       discount_columns: dict = None):
    """
    make k_discount to be around 1 by sigmoid
    n_net: how hard price/netcost affect the discount changing
    """

    default_price = 500

    if not discount_columns:
        discount_columns = DISCOUNT_COLUMNS

    buyoutsCount = discount_columns['buyoutsCount']
    ordersCount = discount_columns['ordersCount']
    quantityFull = discount_columns['quantityFull']
    price = discount_columns['price']
    func_discount = discount_columns['func_discount']
    discount = discount_columns['discount']

    FALSE_LIST = pandas_handler.FALSE_LIST_2
    # FALSE_LIST = pandas_handler.FALSE_LIST
    if 'price_disc' not in df.columns:
        df['price_disc'] = df[price] * (1 - df[discount] / 100)
    df[func_discount] = ''

    df = pandas_handler.replace_false_values(df, [buyoutsCount, quantityFull], FALSE_LIST)

    # df[[buyoutsCount, quantityFull]] = df[[buyoutsCount, quantityFull]].fillna(0)

    # print(f"df[buyoutsCount] {df[buyoutsCount]}")
    # print(f"buyoutsCount {buyoutsCount}")

    df.loc[df[quantityFull].isin(FALSE_LIST), quantityFull] = 0.5

    df.loc[df[buyoutsCount].isin(FALSE_LIST), buyoutsCount] = 0

    # df.to_excel('buyoutsCount.xlsx')

    # def is_numeric(value):
    #     return not np.isnan(pd.to_numeric(value, errors='coerce'))
    #
    # # Check the types of values in the quantityFull column
    # all_types_quantityFull = df[quantityFull].apply(lambda x: 'numeric' if is_numeric(x) else type(x)).unique()
    # print(f'Types in quantityFull: {all_types_quantityFull}')
    #
    # # Check the types of values in the 'buyoutsCount' column
    # all_types_buyoutsCount = df[buyoutsCount].apply(lambda x: 'numeric' if is_numeric(x) else type(x)).unique()
    # print(f'Types in buyoutsCount: {all_types_buyoutsCount}')

    df = df.reset_index(drop=True)
    df[price] = df[price].apply(lambda x: default_price if x in pandas_handler.FALSE_LIST_2 else x)

    # function
    # df['k1'] = df[buyoutsCount] ** p_buy / df[quantityFull] ** p_qt
    # df['k2'] = (df[buyoutsCount] + df[quantityFull])
    k_buy_order = df[buyoutsCount].sum() / df[ordersCount].sum()
    print(f"k_buy_order {k_buy_order}")
    df['k1'] = df[buyoutsCount] ** p_buy / df[quantityFull] ** p_qt
    df['k2'] = (df[ordersCount] * k_buy_order) ** p_buy / df[quantityFull] ** p_qt
    df['k3'] = (df[buyoutsCount] + df[ordersCount] * k_buy_order + df[quantityFull])

    # function
    k = 5
    df['k_sell/stock'] = (((df['k1'] + df['k2']) * df['k3']) / (5 + df[quantityFull])) / k
    # Calculate new discount
    df['func_delta'] = df[discount] - (1 - ((df['price_disc'] * (1 + df['k_sell/stock'])) / df[price])) * 100
    # df.to_excel("df.xlsx")
    df['func_delta'] = df['func_delta'].apply(pandas_handler.false_to_null)
    if 'smooth_days' not in df.columns: df['smooth_days'] = 1
    df['func_delta'] = round(df['func_delta'] / df['smooth_days'])
    df[func_discount] = df[discount] - df['func_delta']
    df[func_discount] = df[func_discount].apply(lambda x: 1 if x < 0 else x)

    # function
    df['k_net/price'] = (4 * df['net_cost'] ** 0.99) / (df['price_disc'] ** 1.01)
    df['1-k_net/price'] = 1 - ((4 * df['net_cost'] ** 0.99) / (df['price_disc'] ** 1.01))

    df.loc[df[func_discount] > 0, func_discount] = df[func_discount] + (
            df['func_delta'] * df['1-k_net/price'] / n_net)
    df.loc[df['func_delta'].isin(FALSE_LIST), func_discount] = df[discount] + df['1-k_net/price']
    df[func_discount] = df[func_discount].apply(lambda x: 1 if x < 0 else x)

    df['price_recommended'] = round((df['k_net/price']) * df['price_disc'])
    df['disc_recommended'] = round((1 - (((df['k_net/price']) * df['price_disc']) / df[price])) * 100)
    # df[func_discount] = round(df[func_discount].apply(lambda x: 0 if x <= 0 else x))
    df[func_discount] = round(df[func_discount])

    df.loc[df[quantityFull] == 0.5, quantityFull] = 0

    return df

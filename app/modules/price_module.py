import requests
from bs4 import BeautifulSoup
import pandas as pd
import math
import numpy as np
import random
from typing import Union
from app.modules import detailing_upload_module

DEFAULT_NET_COST = 500


# /// --- K REVENUE FORMING ---
def k_is_sell(pure_sells_qt, net_cost):
    '''v 1.0'''
    if not net_cost: net_cost = DEFAULT_NET_COST
    k_net_cost = math.sqrt(DEFAULT_NET_COST / net_cost)

    if pure_sells_qt > 100 * k_net_cost:
        return 0.90
    if pure_sells_qt > 50 * k_net_cost:
        return 0.92
    if pure_sells_qt > 20 * k_net_cost:
        return 0.94
    if pure_sells_qt > 10 * k_net_cost:
        return 0.96
    if pure_sells_qt > 5 * k_net_cost:
        return 0.97
    if pure_sells_qt > 3 * k_net_cost:
        return 0.98
    if pure_sells_qt > 1 * k_net_cost:
        return 0.99

    return 1.01


def k_qt_full(qt):
    k = 1
    if qt <= 3:
        return 0.98
    if qt <= 5:
        return 0.99
    if 10 < qt <= 50:
        return 1.01
    if 50 < qt <= 100:
        return 1.03
    if qt > 100:
        return 1.04
    return k


def k_revenue(selqt, sum, mean, last):
    # если одна или менее продаж (совсем мало)
    if selqt <= 1:
        return 1

    # если прибыль растет - можно чуть увеличить цену
    if sum > 0 and mean > 0 and last > 0:
        return 0.99
    # если прибыль отрицательная и падает - минимизируем покатушки - сильно поднимаем цены
    if sum < 0 and mean < 0 and last < 0:
        return 0.96
    # если последний период отрицательный - чуть поднимаем цену для минимизации эффекта покатушек
    if sum > 0 and mean > 0 and last < 0:
        return 0.98
    return 1


def k_logistic(log_rub, to_rub, from_rub, net_cost):
    if not net_cost: net_cost = DEFAULT_NET_COST
    k_net_cost = math.sqrt(DEFAULT_NET_COST / net_cost)

    if to_rub > 0:
        if log_rub > 0.50 * to_rub:
            return 0.96
        if log_rub > 0.25 * to_rub:
            return 0.98

    if log_rub > k_net_cost * net_cost and to_rub == 0:
        return 0.99

    return 1


def k_net_cost(net_cost, price_disc):
    if net_cost == 0:
        net_cost = DEFAULT_NET_COST
    k_net_cost = math.sqrt(DEFAULT_NET_COST / net_cost) * 2
    if k_net_cost < 1:  k_net_cost = 1
    if price_disc <= net_cost:
        return 0.94
    if price_disc <= net_cost * k_net_cost:
        return 0.95
    if price_disc <= net_cost * 1.1 * k_net_cost:
        return 0.96
    if price_disc <= net_cost * 1.3 * k_net_cost:
        return 0.97
    if price_disc <= net_cost * 1.4 * k_net_cost:
        return 0.98
    if price_disc >= net_cost * 4 * k_net_cost:
        return 1.05
    if price_disc >= net_cost * 3 * k_net_cost:
        return 1.04
    if price_disc >= net_cost * 2 * k_net_cost:
        return 1.02
    if price_disc >= net_cost * 1 * k_net_cost:
        return 1.01
    if price_disc == 0:
        return 1

    return 0.99


def k_rating(rating):
    if rating == 5:
        return 0.98
    if rating == 4:
        return 0.99
    if rating == 3:
        return 1.01
    if rating == 2:
        return 1.02
    if rating == 1:
        return 1.03
    return 1


def n_discount(price_disc, k_discount, price):
    if price == 0 or np.isnan(price_disc) or np.isnan(k_discount):
        return np.nan  # Return NaN if any of the values are NaN or if price is zero
    else:
        n_discount = (1 - (price_disc / (price * (k_discount ** 10)))) * 100
        if n_discount < 0:
            return 0
        return int(round(n_discount))


def discount(df):
    col_map = detailing_upload_module.INITIAL_COLUMNS_DICT

    df = detailing_upload_module.rename_mapping(df, col_map=col_map, to='key')

    # если не было продаж увеличиваем скидку
    df['k_is_sell'] = [k_is_sell(x, y) for x, y in zip(df['pure_sells_qt'], df['net_cost'])]
    # постоянно растет или падает прибыль, отрицательная или положительная
    # df['k_revenue'] = [k_revenue(w, x, y, z) for w, x, y, z in
    #                    zip(df['quantity_Продажа_sum'], df['Прибыль_sum'], df['Прибыль_mean'], df['Прибыль_last'])]
    df['k_revenue'] = 1
    # Защита от покатушек - поднимаем цену
    df['k_logistic'] = [k_logistic(w, x, y, z) for w, x, y, z in
                        zip(df['logistics'], df['sell'], df['back'], df['net_cost'])]
    # Защита от цены ниже себестоимости - тогда повышаем
    df['k_net_cost'] = [k_net_cost(x, y) for x, y in zip(df['net_cost'], df['price_disc'])]
    df['k_qt_full'] = [k_qt_full(x) for x in df['stock']]
    df['k_rating'] = [k_rating(x) for x in df['raiting']]
    # df['k_discount'] = (df['k_is_sell'] + df['k_revenue'] + df['k_logistic'] + df['k_net_cost'] + df[
    #     'k_qt_full']) / 5
    df['k_discount'] = 1

    # IMPORTANT !!! days on site was delete some 12/07/2023
    # df.loc[(df['daysOnSite'] > MIN_DAYS_ON_SITE_TO_ANALIZE) & (df['quantity'] > 0), 'k_discount'] = \
    #     (df['k_is_sell'] + df['k_revenue'] + df['k_logistic'] + df['k_net_cost'] + df['k_qt_full'] + df['k_rating']) / 6

    weight_dict = {}
    weight_dict['k_is_sell'] = 1.3
    weight_dict['k_revenue'] = 1
    weight_dict['k_logistic'] = 1
    weight_dict['k_net_cost'] = 1.2
    weight_dict['k_qt_full'] = 1
    weight_dict['k_rating'] = 1

    weighted_sum = (
            df['k_is_sell'] * weight_dict['k_is_sell'] +
            df['k_revenue'] * weight_dict['k_revenue'] +
            df['k_logistic'] * weight_dict['k_logistic'] +
            df['k_net_cost'] * weight_dict['k_net_cost'] +
            df['k_qt_full'] * weight_dict['k_qt_full'] +
            df['k_rating'] * weight_dict['k_rating']
        # Add other coefficients here with their respective weights
    )

    # Calculate the total weight as the sum of individual weights
    total_weight = sum(weight_dict.values())

    # Update the 'k_discount' column based on the weighted sum and total weight
    df.loc[(df['stock'] > 0) | (df['outcome-net-storage'] != 0), 'k_discount'] = weighted_sum / total_weight

    df['n_discount'] = [n_discount(price_disc, k_discount, price) for price_disc, k_discount, price in
                        zip(df['price_disc'], df['k_discount'], df['price'])]

    df['delta_discount'] = df['discount'] - df['n_discount']

    df = detailing_upload_module.rename_mapping(df, col_map=col_map, to='value')

    return df

# def discount_old(df):
#     col_map = detailing_upload_module.INITIAL_COLUMNS_DICT
#
#     df = detailing_upload_module.rename_mapping(df, col_map=col_map, to='key')
#     sells_on_outcome_storage = df['sell'].sum() / (df['outcome'].sum() + df['storage'].sum())
#     df['k_net_cost'] = [k_net_cost(x, y, sells_on_outcome_storage) for x, y in zip(df['net_cost'], df['price_disc'])]
#     mean_stock = df.loc[df['stock'] != 0, 'stock'].mean()
#     df['k_stock'] = [k_stock(x, mean_stock) for x in df['stock']]
#     mean_sell = df.loc[df['quantity_full_sells_qt'] != 0, 'quantity_full_sells_qt'].mean()
#     df['k_sell'] = [k_sell(x, mean_sell) for x in df['sell']]
#
#     df = detailing_upload_module.rename_mapping(df, col_map=col_map, to='value')
#
#     return df

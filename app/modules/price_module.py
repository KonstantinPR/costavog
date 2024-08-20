import requests
# from bs4 import BeautifulSoup
import pandas as pd
import math
import numpy as np
import random

from app.modules import detailing_upload_module

DEFAULT_NET_COST = 250
DEFAULT_PURE_VALUE = DEFAULT_NET_COST * 1.2


# /// --- K REVENUE FORMING ---

def mix_discounts(df, is_mix_discounts=False, k_func_disc=1, k_n_disc=3):
    """

    :param df: DataFrame Pandas
    :param is_mix_discounts: new_discount is influenced by func_discount if True
    :param k_func_discount: influence coefficient of func_discount
    :param k_n_discount: influence coefficient of n_discount
    :return: df: DataFrame Pandas
    """

    if 'new_discount' not in df.columns:
        df['new_discount'] = df['n_discount']

    if not is_mix_discounts:
        df['d_disc'] = round(df['discount'])
        return df

    sum_k_discount = k_func_disc + k_n_disc
    df['new_discount'] = round((df['func_discount'] * k_func_disc + df['n_discount'] * k_n_disc) / sum_k_discount)

    df['d_disc'] = round(df['discount'] - df['new_discount'])

    return df


def count_norma_revenue(df):
    # k_norma must count dynamicly every time as clear_sell/(clear_sell - (storage + logistic...)) + norma_revenue
    # (bank_deposit * 2 = ~40%)
    # for example 200 / (30 + 70) + 0.4 = 2.4 it means that we give wb 50% of sell price, and in that case
    # price (with disc) of goods must be (net_cost * 2.4)

    bank_percentage = 0.18
    k_bank_leverage = 1.2
    # what k_revenue must be we want to sell on wb?
    k = 1 + bank_percentage * k_bank_leverage
    expenses = df['Хранение'].sum() + df['Логистика'].sum() + df['Удержания_minus'].sum() + df["WB_комиссия руб"].sum()
    clear_sells = df['Ч. WB_реализовал'].sum() + df['Удержания_plus'].sum()
    revenue = clear_sells - expenses

    # k_norma_revenue = 1 + (expenses / revenue) + k
    k_norma_revenue = (clear_sells / revenue) * k

    if revenue <= 0:
        k_norma_revenue = 10

    df['k_norma_revenue'] = k_norma_revenue

    return k_norma_revenue


def discount(df, k_delta=1, k_norma_revenue=2.5):
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
    df['k_net_cost'] = [k_net_cost(x, y, k_norma_revenue) for x, y in zip(df['net_cost'], df['price_disc'])]
    df['k_pure_value'] = [k_net_cost(x, y, k_norma_revenue) for x, y in zip(df['pure_value'], df['price_disc'])]
    df['k_qt_full'] = [k_qt_full(qt, volume) for qt, volume in zip(df['stock'], df['volume'])]
    df['k_rating'] = [k_rating(x) for x in df['Rating']]
    # df['k_discount'] = (df['k_is_sell'] + df['k_revenue'] + df['k_logistic'] + df['k_net_cost'] + df[
    #     'k_qt_full']) / 5
    df['k_discount'] = 1

    # IMPORTANT !!! days on site was delete some 12/07/2023
    # df.loc[(df['daysOnSite'] > MIN_DAYS_ON_SITE_TO_ANALIZE) & (df['quantity'] > 0), 'k_discount'] = \
    #     (df['k_is_sell'] + df['k_revenue'] + df['k_logistic'] + df['k_net_cost'] + df['k_qt_full'] + df['k_rating']) / 6

    weight_dict = {}
    weight_dict['k_is_sell'] = 2
    # weight_dict['k_revenue'] = 1
    weight_dict['k_logistic'] = 1
    weight_dict['k_net_cost'] = 4
    weight_dict['k_pure_value'] = 1
    weight_dict['k_qt_full'] = 1
    weight_dict['k_rating'] = 1

    weighted_sum = (
            df['k_is_sell'] * weight_dict['k_is_sell'] +
            # df['k_revenue'] * weight_dict['k_revenue'] +
            df['k_logistic'] * weight_dict['k_logistic'] +
            df['k_net_cost'] * weight_dict['k_net_cost'] +
            df['k_pure_value'] * weight_dict['k_pure_value'] +
            df['k_qt_full'] * weight_dict['k_qt_full'] +
            df['k_rating'] * weight_dict['k_rating']
        # Add other coefficients here with their respective weights
    )

    # Calculate the total weight as the sum of individual weights
    total_weight = sum(weight_dict.values())

    # Update the 'k_discount' column based on the weighted sum and total weight
    # df.loc[(df['stock'] > 0) | (df['outcome-net'] != 0), 'k_discount'] = weighted_sum / total_weight
    df['k_discount'] = weighted_sum / total_weight

    df['n_discount'] = [n_discount(price_disc, k_discount, price, k_delta=k_delta) for price_disc, k_discount, price in
                        zip(df['price_disc'], df['k_discount'], df['price'])]
    df.loc[(df['n_discount'] == 0), 'n_discount'] = df['discount']
    # df['n_delta'] = round((df['discount'] - df['n_discount']) / df['smooth_days']) * k_delta
    df['n_delta'] = round((df['discount'] - df['n_discount']) / df['smooth_days'])
    # df['n_discount'] = df['discount']
    df['n_discount'] = round(df['discount'] - df['n_delta'])
    df.loc[(df['n_delta'] < 0) & (df['stock'] == 0), 'n_discount'] = df['discount']
    df.loc[df['n_discount'] < 0, 'n_discount'] = 0

    df = detailing_upload_module.rename_mapping(df, col_map=col_map, to='value')

    return df


def k_dynamic(df, days_by=1):
    """Calculate k that is influenced by the dynamic of sales in different periods, with higher k for greater sales growth."""
    if days_by <= 1:
        return df

    # Calculate the Simple Moving Average (SMA) for each row
    sales_columns = [col for col in df.columns if 'Продажа_' in col]
    window_size = min(len(sales_columns), days_by)  # Choose the smaller of the two for window size
    k_dynamic_sales_name = 'k_dynamic'

    # Convert selected columns to numeric type
    df[sales_columns] = df[sales_columns].apply(pd.to_numeric, errors='coerce')

    # Replace non-numeric values with 0
    df[sales_columns] = df[sales_columns].fillna(0)

    # Calculate rolling mean
    df[k_dynamic_sales_name] = df[sales_columns].rolling(window=window_size, axis=1).mean().iloc[:, -1]

    # Normalize the result around 1 based on the mean value and rate of change
    mean_sma = df[k_dynamic_sales_name].mean()
    df[k_dynamic_sales_name] = 1 + (mean_sma - df[k_dynamic_sales_name]) / mean_sma

    # Apply the normalization function to the k_dynamic_sales_name column
    df[k_dynamic_sales_name] = df[k_dynamic_sales_name].apply(normalize_around_one)

    return df


# Define the function to calculate exponential moving average
def calculate_ema(row, alpha, sales_columns):
    series = row[sales_columns]
    ema = series.ewm(alpha=alpha, adjust=False).mean().iloc[-1]  # Calculate EMA for the last element only
    return ema


# Define the normalization function
def normalize_around_one(value):
    return 1 + (1 - value) / 50


def nice_price(plan_delta_discount: float, current_price: float) -> float:
    """формирование цены в виде 995"""
    new_price = int(round(current_price * (1 - plan_delta_discount / 100), 0))
    nice_new_price = round(new_price, -2) - random.randint(1, 2) * 5
    return nice_new_price


def k_is_sell(pure_sells_qt, net_cost):
    '''v 1.0'''
    if not net_cost: net_cost = DEFAULT_NET_COST
    k_net_cost = (DEFAULT_NET_COST / net_cost) ** 0.5

    if pure_sells_qt > 50 * k_net_cost:
        return 0.50
    if pure_sells_qt > 20 * k_net_cost:
        return 0.60
    if pure_sells_qt > 10 * k_net_cost:
        return 0.70
    if pure_sells_qt > 5 * k_net_cost:
        return 0.80
    if pure_sells_qt > 3 * k_net_cost:
        return 0.90
    if pure_sells_qt > 2 * k_net_cost:
        return 0.95
    if pure_sells_qt > 1 * k_net_cost:
        return 0.98
    if pure_sells_qt >= 1:
        return 0.99

    return 1.01


def k_qt_full(qt, volume):
    k = 1
    k_volume = 2
    volume_all = qt * volume
    if volume_all < 1:
        return 0.96
    if volume_all < 1 * k_volume:
        return 0.95
    if volume_all <= 1 * k_volume:
        return 0.96
    if volume_all <= 2 * k_volume:
        return 0.97
    if volume_all <= 3 * k_volume:
        return 0.98
    if volume_all <= 5 * k_volume:
        return 0.99
    if volume_all <= 10 * k_volume:
        return 1
    if 10 * k_volume < volume_all <= 20 * k_volume:
        return 1.01
    if 20 * k_volume < volume_all <= 50 * k_volume:
        return 1.03
    if 50 * k_volume < volume_all <= 70 * k_volume:
        return 1.06
    if 70 * k_volume < volume_all <= 100 * k_volume:
        return 1.09
    if volume_all > 100 * k_volume:
        return 1.12
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
    k_net_cost = (DEFAULT_NET_COST / net_cost) ** 0.5

    if to_rub == 0:
        if log_rub >= net_cost * 2:
            return 0.90
        if log_rub >= net_cost:
            return 0.95
        if log_rub >= net_cost / 2:
            return 0.97
        if log_rub >= net_cost / 4:
            return 0.98

    if to_rub > 0:
        if log_rub > 0.50 * to_rub:
            return 0.90
        if log_rub > 0.25 * to_rub:
            return 0.95

    if log_rub > k_net_cost * net_cost and to_rub == 0:
        return 0.99

    return 1

#
# def k_pure_value(pure_value, price_disc, k_norma_revenue):
#     """cost to buy and deliver good to wb"""
#     if pure_value == 0:
#         pure_value = DEFAULT_NET_COST
#
#     k_pure_value = ((DEFAULT_NET_COST / pure_value) * 2) ** 0.5
#     if k_pure_value < 1:
#         k_pure_value = 1
#
#     if price_disc <= pure_value / 4:
#         return 0.50
#     if price_disc <= pure_value / 2:
#         return 0.60
#     if price_disc <= pure_value:
#         return 0.70
#     if price_disc <= pure_value * k_pure_value:
#         return 0.80
#     if price_disc <= pure_value * ((0.50 * k_norma_revenue) * k_pure_value):
#         return 0.85
#     if price_disc <= pure_value * ((0.60 * k_norma_revenue) * k_pure_value):
#         return 0.89
#     if price_disc <= pure_value * ((0.70 * k_norma_revenue) * k_pure_value):
#         return 0.92
#     if price_disc <= pure_value * ((0.80 * k_norma_revenue) * k_pure_value):
#         return 0.93
#     if price_disc <= pure_value * ((0.90 * k_norma_revenue) * k_pure_value):
#         return 0.94
#     if price_disc <= pure_value * ((0.92 * k_norma_revenue) * k_pure_value):
#         return 0.95
#     if price_disc <= pure_value * ((0.96 * k_norma_revenue) * k_pure_value):
#         return 0.96
#     if price_disc <= pure_value * ((0.97 * k_norma_revenue) * k_pure_value):
#         return 0.97
#     if price_disc <= pure_value * ((0.98 * k_norma_revenue) * k_pure_value):
#         return 0.98
#     if price_disc <= pure_value * ((0.99 * k_norma_revenue) * k_pure_value):
#         return 0.99
#     if price_disc >= pure_value * ((5 * k_norma_revenue) * k_pure_value):
#         return 1.20
#     if price_disc >= pure_value * ((2.5 * k_norma_revenue) * k_pure_value):
#         return 1.10
#     if price_disc >= pure_value * ((2 * k_norma_revenue) * k_pure_value):
#         return 1.06
#     if price_disc >= pure_value * ((1.5 * k_norma_revenue) * k_pure_value):
#         return 1.05
#     if price_disc >= pure_value * ((1.25 * k_norma_revenue) * k_pure_value):
#         return 1.04
#     if price_disc >= pure_value * ((1.15 * k_norma_revenue) * k_pure_value):
#         return 1.03
#     if price_disc >= pure_value * ((1.1 * k_norma_revenue) * k_pure_value):
#         return 1.02
#     if price_disc > pure_value * ((1.05 * k_norma_revenue) * k_pure_value):
#         return 1.01
#     if price_disc > pure_value * ((k_norma_revenue) * k_pure_value):
#         return 1
#     if price_disc == 0:
#         return 1
#
#     return 1


def k_net_cost(net_cost, price_disc, k_norma_revenue):
    """cost to buy and deliver good to wb"""
    if net_cost == 0:
        net_cost = DEFAULT_NET_COST

    k_net_cost = (DEFAULT_NET_COST / net_cost) ** 0.5
    if k_net_cost < 1:
        k_net_cost = 1

    if price_disc <= net_cost / 4:
        return 0.50
    if price_disc <= net_cost / 2:
        return 0.60
    if price_disc <= net_cost:
        return 0.70
    if price_disc <= net_cost * k_net_cost:
        return 0.80
    if price_disc <= net_cost * ((0.50 * k_norma_revenue) * k_net_cost):
        return 0.85
    if price_disc <= net_cost * ((0.60 * k_norma_revenue) * k_net_cost):
        return 0.89
    if price_disc <= net_cost * ((0.70 * k_norma_revenue) * k_net_cost):
        return 0.91
    if price_disc <= net_cost * ((0.80 * k_norma_revenue) * k_net_cost):
        return 0.93
    if price_disc <= net_cost * ((0.90 * k_norma_revenue) * k_net_cost):
        return 0.95
    if price_disc <= net_cost * ((0.92 * k_norma_revenue) * k_net_cost):
        return 0.97
    if price_disc <= net_cost * ((0.96 * k_norma_revenue) * k_net_cost):
        return 0.98
    if price_disc <= net_cost * ((0.97 * k_norma_revenue) * k_net_cost):
        return 0.985
    if price_disc <= net_cost * ((0.98 * k_norma_revenue) * k_net_cost):
        return 0.99
    if price_disc <= net_cost * ((0.99 * k_norma_revenue) * k_net_cost):
        return 0.995
    if price_disc >= net_cost * ((5 * k_norma_revenue) * k_net_cost):
        return 1.20
    if price_disc >= net_cost * ((2.5 * k_norma_revenue) * k_net_cost):
        return 1.10
    if price_disc >= net_cost * ((2 * k_norma_revenue) * k_net_cost):
        return 1.06
    if price_disc >= net_cost * ((1.5 * k_norma_revenue) * k_net_cost):
        return 1.05
    if price_disc >= net_cost * ((1.25 * k_norma_revenue) * k_net_cost):
        return 1.04
    if price_disc >= net_cost * ((1.15 * k_norma_revenue) * k_net_cost):
        return 1.03
    if price_disc >= net_cost * ((1.1 * k_norma_revenue) * k_net_cost):
        return 1.02
    if price_disc > net_cost * ((1.05 * k_norma_revenue) * k_net_cost):
        return 1.01
    if price_disc > net_cost * ((k_norma_revenue) * k_net_cost):
        return 1
    if price_disc == 0:
        return 1

    return 1


def k_rating(rating):
    if rating == 5:
        return 0.98
    if rating == 4:
        return 0.99
    if rating == 3:
        return 1
    if rating == 2:
        return 1.01
    if rating == 1:
        return 1.02
    return 1


def n_discount(price_disc, k_discount, price, k_delta=5):
    if price == 0 or np.isnan(price_disc) or np.isnan(k_discount):
        return np.nan  # Return NaN if any of the values are NaN or if price is zero
    else:
        n_discount = (1 - (price_disc / (price * (k_discount ** k_delta)))) * 100
        if n_discount < 0:
            return 0
        return int(round(n_discount))

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

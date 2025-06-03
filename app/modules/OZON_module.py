import logging
import math
import time

from flask import flash

import app.modules.request_handler
from app import app
import requests
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta
from app.modules import io_output
from app.modules import yandex_disk_handler, pandas_handler, request_handler, API_OZON, OZON_module_update_ozon_prices
import datetime

preferred_columns = [
    'clear_sku',
    'Артикул',
    'Ozon Product ID',
    'SKU',
    'Barcode',
    'new_ozon_price',
    'Текущая цена с учетом скидки, ₽',
    'price_disc',
    'price',
    'delivery_charge',
    'return_delivery_charge',
    'accruals_for_sale',
    'sale_commission',
    'amount',
    'free_to_sell_amount',
    'services_price',
    'delivery_to',
    'delivery_from',
    'net_cost',
    'pure_value',
    'income',
    'sells',
    'margin',
    'margin_per_one'
]

nonnumerical = [
    'items_sku',
    'FBS OZON SKU ID',
    'FBO OZON SKU ID',
    'Barcode',
    'idc',
    'article',
    'nm_id',
    'net_cost',
    'pure_value',
    'qt',
    'LAST_MOD_DATE',
    'PREV_NETCOST',
    'company_id',
]


def merge_cards(df, left_on="SKU", right_on="items_sku", how='outer', testing=False,
                ya_path=app.config['YANDEX_CARDS_OZON'],
                is_merge_cards=True):
    if is_merge_cards:
        card_df, _ = yandex_disk_handler.download_from_YandexDisk(path=ya_path, testing_mode=testing)
        df_by_art_size = pandas_handler.df_merge_drop(left_df=card_df, right_df=df,
                                                      left_on=left_on,
                                                      right_on=right_on,
                                                      how=how)
        return df_by_art_size

    return df


def merge_stock(df, left_on="items_sku", right_on="sku", how='outer', testing=False,
                ya_path=app.config['YANDEX_STOCK_OZON'],
                is_merge_cards=True):
    if is_merge_cards:
        stock_df, _ = yandex_disk_handler.download_from_YandexDisk(path=ya_path, testing_mode=testing)
        stock_df = API_OZON.aggregate_by('sku', df=stock_df)
        df_by_art_size = pandas_handler.df_merge_drop(left_df=df, right_df=stock_df,
                                                      left_on=left_on,
                                                      right_on=right_on,
                                                      how=how)
        return df_by_art_size

    return df


def merge_net_cost(df, left_on="clear_sku", right_on="article", how='outer', testing=False,
                   ya_path=app.config['NET_COST_PRODUCTS'],
                   is_merge_net_cost=True):
    if is_merge_net_cost:
        net_cost, _ = yandex_disk_handler.download_from_YandexDisk(path=ya_path, testing_mode=testing)
        df_by_art_size = pandas_handler.df_merge_drop(left_df=df, right_df=net_cost,
                                                      left_on=left_on,
                                                      right_on=right_on,
                                                      how=how)
        return df_by_art_size
    return df


def add_sales_and_margin(df, delivery_from_col='delivery_from', delivery_to_col='delivery_to', income_col='income',
                         net_cost_col='net_cost'):
    """
    Adds 'sells', 'margin', and 'margin_per_one' columns to the DataFrame based on quantity columns.
    """
    # Calculate 'sells' as difference between delivery_to and delivery_from
    df['sells'] = df[delivery_to_col] - df[delivery_from_col]

    # Handle negative or zero sells to avoid division errors or nonsensical margins
    df['sells'] = df['sells'].apply(lambda x: x if x > 0 else 0)

    # Calculate 'margin' as 'income' - 'net_cost' * 'sells'
    df['margin'] = df[income_col] - df[net_cost_col] * df['sells']

    # Calculate 'margin_per_one' as 'margin' divided by 'sells', avoid division by zero
    df['margin_per_one'] = df.apply(lambda row: row['margin'] / row['sells'] if row['sells'] > 0 else 0, axis=1)

    return df


def rearrange_columns(df, preferred_order):
    """
    Rearranges columns in the DataFrame so that the preferred columns come first (if they exist),
    followed by all other columns.
    """
    # Get existing columns in DataFrame
    existing_columns = df.columns.tolist()

    # Filter preferred columns to only those that exist in the DataFrame
    preferred_existing = [col for col in preferred_order if col in existing_columns]

    # Get the remaining columns
    remaining_cols = [col for col in existing_columns if col not in preferred_existing]

    # Concatenate preferred columns first, then the rest
    new_order = preferred_existing + remaining_cols

    # Reindex the DataFrame
    df = df[new_order]

    return df


def get_wb_info(df, is_merge_wb_report=True):
    if is_merge_wb_report:
        wb_df = yandex_disk_handler.get_excel_file_from_ydisk(app.config['REPORT_DETAILING_UPLOAD'])
        if wb_df is None:
            # Log or handle error
            return df

        wb_columns_to_show = ["Артикул поставщика", "new_price", "price_disc", "price"]
        wb_df = wb_df[[col for col in wb_columns_to_show if col in wb_df.columns]]

        # Merge with main df
        df = pandas_handler.df_merge_drop(df, wb_df, "clear_sku", "Артикул поставщика")
        return df

    return df


def count_new_price(margin, current_price, new_price, price, qt):
    if qt <= 0:
        return max(current_price, price)
    if margin <= 0:
        if current_price > new_price:
            new_ozon_price = current_price - (current_price - new_price) / 4
        else:  # current_price <= new_price
            new_ozon_price = new_price - new_price * 0.01
    else:  # margin > 0
        if current_price <= new_price:
            new_ozon_price = current_price + (new_price - current_price) / 4
        else:
            # Optionally, define behavior for other cases if needed
            new_ozon_price = current_price  # or handle differently
    return round(new_ozon_price, -1) - 11


def analiz_wb_price(df, is_merge_wb_report=True):
    if is_merge_wb_report:
        df['new_ozon_price'] = [count_new_price(margin, current_price, new_price, price, qt)
                                for margin, current_price, new_price, price, qt in
                                zip(df['margin'], df['Текущая цена с учетом скидки, ₽'], df['new_price'], df['price'],
                                    df['Доступно к продаже по схеме FBO, шт.'])]
        return df  # assuming df_by_art is defined elsewhere
    return df


def ruled_prices(df, is_update_prices):
    if not is_update_prices:
        return df

    df['Ozon Product ID'] = df['Ozon Product ID'].apply(pandas_handler.false_to_null)

    df = df.copy()

    col_name_with_missing = ['new_ozon_price', 'price', 'net_cost', 'old_price', 'min_price']
    for col in col_name_with_missing:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    def correct_price(new_ozon_price, price):
        if new_ozon_price > price:
            price = new_ozon_price
        if new_ozon_price < 400:
            if (price - new_ozon_price) < 20:
                price = new_ozon_price + 20
        elif 400 <= new_ozon_price <= 10000:
            # Ensure discount is more than 5%, i.e., price <= 95% of original price
            if new_ozon_price > price * 0.95:
                price = new_ozon_price / 0.95 + 1
        elif new_ozon_price > 10000:
            if (price - new_ozon_price) < 500:
                price = new_ozon_price + 500
        return round(price, 0)

    # Update 'old_price'

    df['old_price'] = [correct_price(new_ozon_price, price) for new_ozon_price, price in
                       zip(df['new_ozon_price'], df['price'])]

    # Calculate 'min_price' based on 'price'
    df['min_price'] = df['price'].apply(lambda x: round(x ** 0.85, 0))

    # Ensure 'min_price' is at least 50% of 'new_ozon_price'
    df['min_price'] = df.apply(
        lambda row: max(row['min_price'], 0.5 * row['new_ozon_price']) + 10,
        axis=1
    )

    # Optionally, round 'min_price' to the nearest integer
    df['min_price'] = df['min_price'].round()

    # df.to_excel("df.xlsx", index=False)

    return df


def prepare_update(df_by_art, df_by_art_size, is_update_prices=True, is_update_available=True):
    if not is_update_prices:
        return df_by_art_size
    # Set 'new_ozon_price' for rows where 'free_to_sell_amount' <= 0
    # df_by_art.loc[df_by_art['Доступно к продаже по схеме FBO, шт.'] <= 0, 'new_ozon_price'] = df_by_art['price']

    # Select relevant columns from df_by_art_size
    df_by_art_size = df_by_art_size[['clear_sku', 'SKU', 'Артикул', 'Ozon Product ID']]

    # Merge the price info into df_by_art_size based on 'clear_sku'
    df_for_update_prices = df_by_art_size.merge(
        df_by_art,
        on='clear_sku',
        how='left',
        suffixes=('', '_right')
    )

    # Keep only relevant columns, ensuring columns exist in the DataFrame
    columns_to_keep = [col for col in
                       ['SKU', 'clear_sku', 'Ozon Product ID', 'Артикул', 'new_ozon_price', 'price', 'net_cost',
                        'Доступно к продаже по схеме FBO, шт.', 'income']
                       if col in df_for_update_prices.columns]

    df_for_update_prices = df_for_update_prices[columns_to_keep]

    # Filter rows where 'free_to_sell_amount' > 0
    if is_update_available:
        df_for_update_prices = df_for_update_prices[
            (df_for_update_prices['Доступно к продаже по схеме FBO, шт.'] > 0) | (df_for_update_prices['income'] != 0)]

    # Remove rows where 'product_id' is NaN or empty string
    df_for_update_prices = df_for_update_prices[
        df_for_update_prices['Ozon Product ID'].notnull() & (df_for_update_prices['Ozon Product ID'] != '')]
    # df_for_update_prices.to_excel("df_for_update_prices.xlsx")

    return df_for_update_prices


def update_ozon_prices(df, headers='', is_update_prices=True, testing_mode=True):
    if testing_mode:
        print("Testing mode: no API call made.")
        return df

    if not is_update_prices:
        return df

    df_sample = df.copy()  # work on a copy to avoid modifying original

    # Prepare the list of all prices and results
    prices_list_all = []
    results_list = []

    for _, row in df_sample.iterrows():
        price_data = {
            "auto_action_enabled": "DISABLED",
            "auto_add_to_ozon_actions_list_enabled": "DISABLED",
            "currency_code": "RUB",
            "min_price": str(row['min_price']),
            "min_price_for_auto_actions_enabled": True,
            "net_price": str(row['net_cost']),
            "old_price": str(row['old_price']),
            "price": str(row['new_ozon_price']),
            "price_strategy_enabled": "DISABLED",
            "product_id": int(row['Ozon Product ID']),
            "offer_id": str(row['Артикул']),
        }
        prices_list_all.append(price_data)

        results_list.append({
            "SKU": row['SKU'],
            "clear_sku": row['clear_sku'],
            "product_id": row['Ozon Product ID'],
            "offer_id": row['Артикул'],
            "auto_action_enabled": "DISABLED",
            "auto_add_to_ozon_actions_list_enabled": "DISABLED",
            "currency_code": "RUB",
            "min_price": row['min_price'],
            "net_cost": row['net_cost'],
            "old_price": row['old_price'],
            "price": row['new_ozon_price'],
            "min_price_for_auto_actions_enabled": True,
            "price_strategy_enabled": "DISABLED",
            "status": "Pending",
            "response_message": ""
        })

    all_results_df = OZON_module_update_ozon_prices.update_api_with_chank(prices_list_all, results_list,
                                                                          headers=headers)

    return all_results_df

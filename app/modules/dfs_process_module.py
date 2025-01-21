from pandas import DataFrame
from app import app
import zipfile
import pandas as pd
import numpy as np
import io
from datetime import datetime
from app.modules import pandas_handler, API_WB, yandex_disk_handler, price_module, sales_funnel_module, delivery_module
from flask import request
from types import SimpleNamespace
from typing import List, Type
from varname import nameof
from app.modules.decorators import timing_decorator
from app.modules.detailing_upload_dict_module import PREFIXES_ART_DICT
from app.modules.dfs_forming_module import get_dynamic_sales, get_storage_cost, zip_detail_V2, merge_stock, \
    merge_storage, merge_net_cost, merge_price, profit_count, get_period_sales, replace_incorrect_date, add_k, \
    pattern_splitting


def dfs_from_outside(r):
    d = SimpleNamespace()
    d.df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
    d.df_delivery = delivery_module.process_delivering(app.config['DELIVERY_PRODUCTS'], period=365)['df_delivery_pivot']
    d.df_price, _ = API_WB.get_wb_price_api(testing_mode=r.testing_mode)
    d.df_rating = yandex_disk_handler.get_excel_file_from_ydisk(app.config['RATING'])
    d.request_dict = {'no_sizes': 'no_sizes', 'no_city': 'no_city'}
    d.df_stock = API_WB.get_wb_stock_api(testing_mode=r.testing_mode, request=d.request_dict, is_shushary=r.is_shushary)
    d.df_funnel, d.funnel_name = API_WB.get_wb_sales_funnel_api(request, testing_mode=r.testing_mode,
                                                                is_funnel=r.is_funnel)
    d.df_storage = API_WB.get_storage_cost(testing_mode=r.testing_mode, is_shushary=r.is_shushary)
    return d


def choose_df_in(df_list, is_first_df):
    if is_first_df:
        return df_list[0]
    return pd.concat(df_list)


def dfs_forming(df, d, r, include_columns) -> pd.DataFrame:
    # df.to_excel("df_delivery2.xlsx")
    df = replace_incorrect_date(df)
    date_min = df["Дата продажи"].min()
    date_max = df["Дата продажи"].max()

    dfs_sales, incl_columns = get_dynamic_sales(df, r.days_by, include_columns)

    storage_cost = get_storage_cost(df)

    df = zip_detail_V2(df, drop_duplicates_in="Артикул поставщика")

    df = merge_stock(df, d.df_stock, is_get_stock=r.is_get_stock)

    if not 'quantityFull' in df.columns: df['quantityFull'] = 0
    df['quantityFull'].replace(np.NaN, 0, inplace=True)
    df['quantityFull + Продажа, шт.'] = df['quantityFull'] + df['Продажа, шт.']

    df = merge_storage(df, storage_cost, r.testing_mode, is_get_storage=r.is_get_storage,
                       is_shushary=r.is_shushary, df_storage=d.df_storage, files_period_days=r.files_period_days)
    df = merge_net_cost(df, d.df_net_cost, r.is_net_cost)
    df = merge_price(df, d.df_price, r.is_get_price).drop_duplicates(subset='nmId')
    df = profit_count(df)
    df = pandas_handler.df_merge_drop(df, d.df_rating, 'nmId', 'Артикул ВБ', how="outer")
    df['Rating'] = [x.split(" ")[0] if isinstance(x, str) else x for x in df['Рейтинг отзывов']]

    df = pandas_handler.fill_empty_val_by(['article', 'vendorCode', 'supplierArticle'], df, 'Артикул поставщика')
    df = pandas_handler.fill_empty_val_by(['brand'], df, 'Бренд')
    df = df.rename(columns={'Предмет_x': 'Предмет'})
    df = pandas_handler.fill_empty_val_by(['category'], df, 'Предмет')

    if dfs_sales:
        print(f"merging dfs_sales ...")
        for df_sale in dfs_sales:
            df = pandas_handler.df_merge_drop(df, df_sale, 'nmId', 'Код номенклатуры', how="outer")

    # --- DICOUNT ---

    df = get_period_sales(df, date_min, date_max)
    k_norma_revenue = price_module.count_norma_revenue(df)
    df = price_module.discount(df, k_delta=r.k_delta, k_norma_revenue=k_norma_revenue, reset_if_null=r.reset_if_null)
    discount_columns = sales_funnel_module.DISCOUNT_COLUMNS

    # Reorder the columns

    #  --- PATTERN SPLITTING ---
    df = pattern_splitting(df, prefixes_dict=PREFIXES_ART_DICT)

    if r.is_funnel and not d.df_funnel.empty:
        df = pandas_handler.df_merge_drop(df, d.df_funnel, "nmId", "nmID")
        df = sales_funnel_module.calculate_discount(df, discount_columns=discount_columns)
        df = price_module.mix_discounts(df, r.is_mix_discounts)

    df = add_k(df)

    # print(INCLUDE_COLUMNS)
    include_column = [col for col in incl_columns if col in df.columns]
    df = df[include_column + [col for col in df.columns if col not in incl_columns]]
    df = pandas_handler.round_df_if(df, half=10)
    if 'new_discount' not in df.columns: df['new_discount'] = df['n_discount']

    df = pandas_handler.drop_duplicates(df=df, columns="nmId")
    return df


def choose_dynamic_df_list_in(df_list, is_dynamic=False):
    if is_dynamic:
        return df_list
    return []

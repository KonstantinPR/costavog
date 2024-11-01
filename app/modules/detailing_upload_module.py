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
from typing import List, Type, Tuple
from varname import nameof
import multiprocessing

from app.modules.decorators import timing_decorator

'''Analize detaling WB reports, take all zip files from detailing WB and make one file EXCEL'''

DEFAULT_AMOUNT_DAYS = 7

MATERIAL_DICT = {
    'N30': 'N30',
    'N511': 'N511',
    'N1278': 'N1278',
    'MUTON': 'MUTON',
}

PREFIXES_ART_DICT = {
    'SH': 'SH',
    'SK': 'SK',
    'SN': 'SK',
    'SF': 'SF',
    'J': 'J',
    'MIT': 'MIT',
    'AN': 'MIT',
    'MK': 'MIT',
    'TIE': 'TIE',
    'FATA': 'F',
    'GR': 'GR',
    'LQ3': 'LQ',
    'SOHOCOAT': 'SOHO',
    'TG': 'TG'
}

STRFORMAT_DEFAULT = '%Y-%m-%d'

INITIAL_COLUMNS_DICT = {
    'brand': 'Бренд',
    'subject': 'Предмет',
    'prefix': 'prefix',
    'pattern': 'pattern',
    'material': 'material',
    'article_supplier': 'Артикул поставщика',
    'nmId': 'nmId',
    'new_discount': 'new_discount',
    'd_disc': 'd_disc',
    'n_discount': 'n_discount',
    'n_delta': 'n_delta',
    'func_discount': 'func_discount',
    'func_delta': 'func_delta',
    'discount': 'discount',
    'outcome-net': 'Маржа-себест.',
    'Маржа-себест./ шт.': 'Маржа-себест./ шт.',
    'outcome': 'Маржа',
    'income-logistic': 'Выручка-Логистика',
    'income': 'Выручка',
    'hold_up': 'Удержания_minus',
    'hold_down': 'Удержания_plus',
    'Ч. Продажа': 'Ч. Продажа',
    'sell': 'Продажа',
    'Ч. Продажа шт./Логистика шт.': 'Ч. Продажа шт./Логистика шт.',
    'pure_sells_qt': 'Ч. Продажа шт.',
    'commission': 'commission',
    'back_qt': 'Возврат, шт.',
    'stock': 'quantityFull',
    'logistics': 'Логистика',
    'logistics_single': 'Логистика. ед',
    'logistics_qt': 'Логистика шт.',
    'storage': 'Хранение',
    'storage_single': 'Хранение.ед',
    'price': 'price',
    'price_disc': 'price_disc',
    'Ср. Ц. Продажа/ед.': 'Ср. Ц. Продажа/ед.',
    'net_cost': 'net_cost',
    'volume': 'volume',
    'quantity_full_sells_qt': 'quantityFull + Продажа, шт.',
    'back': 'Возврат',
    'sells_days': 'Дней в продаже',
    'storagePricePerBarcode': 'storagePricePerBarcode',
    'shareCost': 'shareCost',
    'outcome_net_storage_single': 'Маржа-себест./ шт.',
    'supplier': 'Поставщик',
    'k_is_sell': 'k_is_sell',
    'k_logistic': 'k_logistic',
    'k_net_cost': 'k_net_cost',
    'k_pure_value': 'k_pure_value',
    'k_qt_full': 'k_qt_full',
    'k_rating': 'k_rating',
    'k_dynamic': 'k_dynamic',
    'k_discount': 'k_discount',
    'order_date': 'Дата заказа покупателем',
    'sell_date': 'Дата продажи',
    'raiting': 'Рейтинг',
    'k_norma_revenue': 'k_norma_revenue',
}

DINAMIC_COLUMNS = [
    "Total_Margin",
    "ABC_Category",
    "CV",
    "XYZ_Category",
]

CHOSEN_COLUMNS = [
    "Бренд",
    "Предмет",
    "prefix",
    "pattern",
    "material",
    "Артикул поставщика",
    "nmId",
    "new_discount",
    "d_disc",
    "n_discount",
    "n_delta",
    "func_discount",
    "func_delta",
    "discount",
    "Маржа-себест.",
    "Маржа-себест./ шт.",
    "Маржа",
    "Выручка-Логистика",
    "Выручка",
    "Удержания_minus",
    "Удержания_plus",
    "Ч. Продажа",
    "Продажа",
    "Ч. Продажа шт./Логистика шт.",
    "Ч. Продажа шт.",
    "commission",
    "Возврат, шт.",
    "quantityFull",
    "Логистика",
    "Логистика. ед",
    "Логистика шт.",
    "Хранение",
    "Хранение.ед",
    "price",
    "price_disc",
    "Ср. Ц. Продажа/ед.",
    "net_cost",
    "volume",
    "quantityFull + Продажа, шт.",
    "Возврат",
    "Дней в продаже",
    "storagePricePerBarcode",
    "shareCost",
    "Маржа-себест./ шт.",
    "Поставщик",
    "k_is_sell",
    "k_logistic",
    "k_net_cost",
    "k_pure_value",
    "k_qt_full",
    "k_rating",
    "k_discount",
    "openCardCount",
    "addToCartCount",
    "ordersCount",
    "ordersSumRub",
    "buyoutsCount",
    "buyoutsSumRub",
    "cancelCount",
    "cancelSumRub",
    "avgOrdersCountPerDay",
    "avgPriceRub",
    "addToCartPercent",
    "cartToOrderPercent",
    "buyoutsPercent",
    "Total_Margin",
    "ABC_Category",
    "CV",
    "XYZ_Category",
    "ABC_discount",
    "CV_discount",
    "ABC_CV_discount",

]


def get_period_sales(df, date_min, date_max, default_amount_days=DEFAULT_AMOUNT_DAYS, days_fading=0.25):
    days_period = (date_max - date_min).days

    df['days_period'] = days_period
    df['smooth_days'] = (df['days_period'] / default_amount_days) ** days_fading
    print(f"smooth_days {df['smooth_days'].mean()}")
    return df


def rename_mapping(df, col_map, to='key'):
    # Rename columns to keys from INITIAL_COLUMNS_DICT
    rename_mapping = {}

    for k, v in col_map.items():
        if k != v:
            if to == 'key':
                if k in df.columns:
                    print(f"Column '{v}' is already present in the DataFrame.")
                else:
                    rename_mapping[v] = k
            elif to == 'value':
                # Rename columns back to values from INITIAL_COLUMNS_DICT
                if v in df.columns:
                    print(f"Column '{k}' is already present in the DataFrame.")
                else:
                    rename_mapping[k] = v

    df = df.rename(columns=rename_mapping)
    return df


def _adding_missing_columns(df):
    if 'К перечислению за товар' not in df:
        df['К перечислению за товар'] = 0

    if 'К перечислению Продавцу за реализованный Товар' not in df:
        df['К перечислению Продавцу за реализованный Товар'] = 0

    df['К перечислению Продавцу за реализованный Товар'].replace(np.NaN, 0, inplace=True)

    df['К перечислению за товар'].replace(np.NaN, 0, inplace=True)

    df['К перечислению за товар ИТОГО'] = df['К перечислению за товар'] + df[
        'К перечислению Продавцу за реализованный Товар']

    if 'Возмещение издержек по эквайрингу' not in df:
        df['Возмещение издержек по эквайрингу'] = 0

    if 'Эквайринг/Комиссии за организацию платежей' not in df:
        df['Эквайринг/Комиссии за организацию платежей'] = 0

    df['Возмещение издержек по эквайрингу'] = df['Возмещение издержек по эквайрингу'] + df[
        'Эквайринг/Комиссии за организацию платежей']
    return df


def add_col(df, col_name):
    if col_name not in df:
        df['Эквайринг/Комиссии за организацию платежей'] = 0
    return col_name


def zip_detail_V2(concatenated_dfs, drop_duplicates_in=None):
    article_column_name = 'Артикул поставщика'
    df = concatenated_dfs
    # result.dropna(subset=["Артикул поставщика"], inplace=True)
    # result.to_excel('result.xlsx')

    df = pandas_handler.first_letter_up(df, 'Обоснование для оплаты')
    df = _adding_missing_columns(df)
    df = df.sort_values(by='Дата заказа покупателем', ascending=True)

    sales_name = 'Продажа'
    type_wb_sales_col_name_all = 'Вайлдберриз реализовал Товар (Пр)'
    type_sales_col_name_all = 'К перечислению за товар ИТОГО'

    backs_name = 'Возврат'

    logistic_name = 'Логистика'
    type_delivery_service_col_name = 'Услуги по доставке товара покупателю'

    substituted_col_name = add_col(df, 'Компенсация подмененного товара')
    damages_col_name = add_col(df, 'Компенсация ущерба')
    penalty_col_name = add_col(df, 'Штраф')
    type_sales_col_name = 'К перечислению Продавцу за реализованный Товар'
    type_penalty_col_name = 'Общая сумма штрафов'
    type_acquiring_col_name = 'Возмещение издержек по эквайрингу'
    type_give_col_name = 'Возмещение за выдачу и возврат товаров на ПВЗ'
    type_store_moves_col_name = 'Возмещение издержек по перевозке/по складским операциям с товаром'

    qt_col_name = add_col(df, 'Кол-во')
    qt_logistic_to_col_name = add_col(df, 'Количество доставок')
    qt_logistic_back_col_name = add_col(df, 'Количество возврата')

    df_wb_sales = pivot_expanse(df, sales_name, type_wb_sales_col_name_all, col_name="WB реализовал руб")
    df_wb_backs = pivot_expanse(df, backs_name, type_wb_sales_col_name_all, col_name="WB вернули руб")
    df_sales = pivot_expanse(df, sales_name, type_sales_col_name_all)
    df_backs = pivot_expanse(df, backs_name, type_sales_col_name_all)
    df_logistic = pivot_expanse(df, logistic_name, type_delivery_service_col_name)
    df_compensation_substituted = pivot_expanse(df, substituted_col_name, type_sales_col_name)
    df_compensation_damages = pivot_expanse(df, damages_col_name, type_sales_col_name)
    df_penalty = pivot_expanse(df, penalty_col_name, type_penalty_col_name)
    df_acquiring = pivot_expanse(df, sales_name, type_acquiring_col_name, col_name="Эквайринг")
    df_give_to = pivot_expanse(df, sales_name, type_give_col_name, col_name="При выдачи от")
    df_give_back = pivot_expanse(df, backs_name, type_give_col_name, col_name="При выдачи в")
    df_store_moves = pivot_expanse(df, type_store_moves_col_name, type_store_moves_col_name, col_name='Склады удержали')
    df_qt_sales = pivot_expanse(df, sales_name, qt_col_name, col_name='Продажа, шт.')
    df_qt_backs = pivot_expanse(df, backs_name, qt_col_name, col_name='Возврат, шт.')
    df_qt_logistic_to = pivot_expanse(df, logistic_name, qt_logistic_to_col_name, col_name='Логистика до, шт.')
    df_qt_logistic_back = pivot_expanse(df, logistic_name, qt_logistic_back_col_name, col_name='Логистика от, шт.')

    df = df.drop_duplicates(subset=[article_column_name])
    dfs = [df_wb_sales, df_wb_backs, df_sales, df_backs, df_logistic, df_compensation_substituted,
           df_compensation_damages, df_penalty,
           df_acquiring, df_give_to, df_give_back, df_store_moves, df_qt_sales, df_qt_backs, df_qt_logistic_to,
           df_qt_logistic_back]

    for d in dfs:
        df = pd.merge(df, d, how='outer', on=article_column_name)
    # dfs_names = [sales_name, logistic_name, backs_name, substituted_col_name]
    df = df.fillna(0)

    # df.to_excel("v2.xlsx")
    df['Ч. WB_реализовал'] = df['WB реализовал руб'] - df['WB вернули руб']
    df['Ч. Продажа'] = df['Продажа'] - df['Возврат']
    df['WB_комиссия руб'] = df['Ч. WB_реализовал'] - df['Ч. Продажа']
    df['Ч. Продажа шт.'] = df['Продажа, шт.'] - df['Возврат, шт.']
    df['Ср. Ц. Продажа/ед.'] = df['Ч. Продажа'] / df['Ч. Продажа шт.']
    df['Логистика шт.'] = df['Логистика до, шт.'] + df['Логистика от, шт.']
    df['Логистика. ед'] = df['Логистика'] / df['Логистика шт.']
    df['Удержания_plus'] = df[damages_col_name] + df[substituted_col_name]
    df['Удержания_minus'] = df[penalty_col_name] + df['Эквайринг'] + df['При выдачи от'] + df['При выдачи в'] + df[
        'Склады удержали']
    df['Выручка'] = df[sales_name] - df[backs_name] + df['Удержания_plus'] - df['Удержания_minus']
    df['Выручка-Логистика'] = df['Выручка'] - df[logistic_name]
    df['Дней в продаже'] = [days_between(d1, datetime.today()) for d1 in df['Дата заказа покупателем']]

    if drop_duplicates_in:
        df.drop_duplicates(subset=drop_duplicates_in)

    return df


def merge_stock(df, df_stock, is_get_stock=False):
    if is_get_stock:
        df_stock = pandas_handler.upper_case(df_stock, 'supplierArticle')[0]
        df = df_stock.merge(df, how='outer', left_on='nmId', right_on='Код номенклатуры')
        df = df.fillna(0)
        df = pandas_handler.fill_empty_val_by('Код номенклатуры', df, 'nmId')
        return df
    return df


def merge_storage(df, storage_cost, testing_mode, is_get_storage, is_shushary=False, df_storage=None):
    if not is_get_storage:
        return df

    if df_storage is None or df_storage.empty:
        df_storage = API_WB.get_storage_cost(testing_mode=testing_mode, is_shushary=is_shushary)

    df_storage = pandas_handler.upper_case(df_storage, 'vendorCode')[0]
    # df = df.merge(df_storage, how='outer', left_on='nmId', right_on='nmId')
    df = pandas_handler.df_merge_drop(df, df_storage, 'nmId', 'nmId', how="outer")
    # df.to_excel("merged_storage.xlsx")
    df = df.fillna(0)

    df['Хранение'] = df['quantityFull + Продажа, шт.'] * df['storagePricePerBarcode']
    df['shareCost'] = df['Хранение'] / df['Хранение'].sum()
    df['Хранение'] = df['shareCost'] * storage_cost

    df['Хранение'] = df['Хранение'].fillna(0)
    print(f"df['Хранение'] {df['Хранение'].sum()}")
    # df.to_excel("storage.xlsx")
    df['Хранение.ед'] = df['Хранение'] / df['quantityFull + Продажа, шт.']
    return df


def merge_net_cost(df, df_net_cost, is_net_cost):
    if is_net_cost:
        df_net_cost['net_cost'].replace(np.NaN, 0, inplace=True)
        df_net_cost = pandas_handler.upper_case(df_net_cost, 'article')[0]
        df = pandas_handler.df_merge_drop(df, df_net_cost, 'nmId', 'nm_id', how="outer")
        # df.to_excel("net_cost_merged.xlsx")
    else:
        df['net_cost'] = 0

    return df


def profit_count(df):
    df = df.fillna(0)
    # df.to_excel("profit_count.xlsx")
    df['Маржа'] = df['Выручка-Логистика'] - df['Хранение'].astype(float)
    df['Маржа-себест.'] = df['Маржа'] - df['net_cost'] * df['Ч. Продажа шт.']
    df = df.fillna(0)
    df['Маржа-себест./ шт.'] = np.where(
        df['Продажа, шт.'] != 0,
        df['Маржа-себест.'] / df['Продажа, шт.'],
        np.where(
            df['quantityFull'] != 0,
            df['Маржа-себест.'] / df['quantityFull'],
            0
        )
    )
    # df.to_excel('profit_count.xlsx')
    df.loc[df['Ч. Продажа шт.'] > 0, 'commission'] = round(1 - df['Маржа-себест.'] / df['Ч. Продажа'], 2)
    df['commission'] = df['commission'].replace([np.inf, -np.inf], "")

    return df


def merge_price(df, df_price, is_get_price):
    if is_get_price:
        # df = df.merge(df_price, how='outer', left_on='nmId', right_on='nmID')
        df = pandas_handler.df_merge_drop(df, df_price, 'nmId', 'nmID', how="outer")
        # df.to_excel("price_merged.xlsx")
        df['price_disc'] = df['price'] - df['price'] * df['discount'] / 100
    return df


def get_storage_cost(df):
    if 'Хранение' in df.columns:
        storage_cost = df['Хранение'].sum()
    else:
        df['Хранение'] = 0
        storage_cost = 0
    return storage_cost


def pivot_expanse(df, type_name, sum_name, agg_col_name='Артикул поставщика', type_col_name='Обоснование для оплаты',
                  col_name=None):
    df_type = df[df[type_col_name] == type_name]
    if sum_name not in df_type.columns:
        df[sum_name] = 0
        if col_name:
            df[col_name] = 0
        return df

    df = df_type.groupby(agg_col_name)[sum_name].sum().reset_index()
    if col_name:
        df = df.rename(columns={sum_name: f'{str(col_name)}'})
    else:
        df = df.rename(columns={sum_name: f'{str(type_name)}'})

    return df


@timing_decorator
def process_uploaded_files(uploaded_files):
    zip_buffer = io.BytesIO()

    if len(uploaded_files) == 1 and uploaded_files[0].filename.endswith('.zip'):
        # If there is only one file and it's a zip file, proceed as usual
        file = uploaded_files[0]
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            zip_file.writestr(file.filename, file.read())
    else:
        # If there are multiple files or a single non-zip file, create a zip archive in memory
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for file in uploaded_files:
                zip_file.writestr(file.filename, file.read())

    # Reset the in-memory buffer's position to the beginning
    zip_buffer.seek(0)
    # Set the uploaded_file to the in-memory zip buffer
    file = zip_buffer

    return file


def to_round_df(df_result):
    df_result = df_result.round(decimals=0)
    return df_result


@timing_decorator
def zips_to_list(zip_downloaded):
    print(f"type of zip_downloaded {type(zip_downloaded)}")
    dfs = []

    z = zipfile.ZipFile(zip_downloaded)
    for f in z.namelist():
        # get directory name from file
        content = io.BytesIO(z.read(f))
        zip_file = zipfile.ZipFile(content)
        for i in zip_file.namelist():
            excel_0 = zip_file.read(i)
            df = pd.read_excel(excel_0)
            dfs.append(df)
    return dfs


def replace_incorrect_date(df, date_column='Дата продажи'):
    # Convert the 'Дата продажи' column to datetime format
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

    # Define the specific dates to remove
    specific_dates = ['2022-09-25', '2022-10-02']

    # Filter out rows with the specific dates and modify the original DataFrame in-place
    df = df[~df[date_column].isin(specific_dates)]

    # Print the minimum valid date after replacing dates
    print(f"date_min {df[date_column].min()}")

    # Save the DataFrame to an Excel file
    # df.to_excel("df_date.xlsx", index=False)

    return df


def get_dynamic_sales(df,
                      days_by,
                      INCLUDE_COLUMNS,
                      type_column='Тип документа',
                      type_name='Продажа',
                      nmId='Код номенклатуры',
                      sales_column='К перечислению Продавцу за реализованный Товар',
                      date_column='Дата продажи'):
    """
    Calculate dynamic sales based on the specified number of periods.

    Parameters:
        df (DataFrame): Input DataFrame containing sales data.
        sales_column (str): Name of the column containing sales data.
        date_column (str): Name of the column containing date data.
        days_by (int): days between period for dynamic
    Returns:
        DataFrame: DataFrame with dynamic sales information appended as new columns.
    """
    days_by = int(days_by)
    print(f"get_dynamic_sales by {days_by} days_by...")
    if days_by <= 1:
        print(f"not enough days for deviding period by days_by {days_by}, returning df without dividing")
        return None, INCLUDE_COLUMNS

    # Convert date_column to datetime format only for rows where Продажи != 0
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df.loc[df[sales_column] == 0, date_column] = pd.NaT  # Set dates to NaT for zero sales

    # Find the minimum and maximum dates after replacements
    min_date = df[date_column].min()
    print(min_date)
    max_date = df[date_column].max()
    print(max_date)

    # Calculate the duration of each period
    days = (max_date - min_date) / days_by

    print(f"days {days} ...")
    print(f"max_date - min_date {max_date - min_date} ...")

    sales_columns = []
    dfs_sales = []
    start_date = min_date
    for period in range(days_by):
        # Calculate the start and end dates of the current period
        end_date = start_date + days
        print(f"end_date {end_date}")

        # Filter DataFrame for the current period
        period_df = df[(df[date_column] >= start_date) & (df[date_column] < end_date)]

        # Filter DataFrame to include only sales where 'Тип документа' is equal to 'Продажа'
        period_sales = period_df[period_df[type_column] == type_name]

        # Calculate sum of sales_column for each article_column in the current period
        sales = period_sales.groupby(nmId)[sales_column].sum().reset_index()
        sales_column_name = f'{type_name}_{period + 1}'  # Create a unique name for the sales column
        sales.rename(columns={sales_column: sales_column_name}, inplace=True)

        sales = sales[[nmId, sales_column_name]]
        print(f"sum sales for {sales_column_name} is {sales[sales_column_name].sum()}")

        dfs_sales.append(sales)
        sales_columns.append(sales_column_name)

        start_date = end_date

    INCLUDE_COLUMNS = INCLUDE_COLUMNS + sales_columns
    # print(INCLUDE_COLUMNS)

    return dfs_sales, INCLUDE_COLUMNS


def concatenate_detailing_module(zip_downloaded, df_net_cost):
    dfs = zips_to_list(zip_downloaded)

    result = pd.concat(dfs)

    # List of columns to treat as strings
    columns_to_convert = ['№', 'Баркод', 'ШК', 'Rid', 'Srid']

    # Ensure that specific columns are treated as strings
    result[columns_to_convert] = result[columns_to_convert].astype(str)
    return result


def days_between(d1, d2):
    if d1:
        d1 = datetime.strptime(d1, STRFORMAT_DEFAULT)
        # d2 = datetime.strptime(d2, "%Y-%m-%d")
        return abs((d2 - d1).days)
    return None


@timing_decorator
def promofiling(promo_file, df, allowed_delta_percent=5):
    if not promo_file:
        return pd.DataFrame

    # Read the promo file into a DataFrame
    df_promo = pd.read_excel(promo_file)

    df_promo = pandas_handler.df_merge_drop(df_promo, df, "Артикул WB", "nmId", how='outer')
    df_promo = check_discount(df_promo, allowed_delta_percent)

    return df_promo


def check_discount(df, allowed_delta_percent, default_disc=5):
    plan_price_name = "Плановая цена для акции"
    current_price_name = "Текущая розничная цена"
    new_discount_col = "new_discount"
    promo_discount_name = "Загружаемая скидка для участия в акции"

    promo_discount_name_actual = "Загружаемая скидка для участия в акции исправленная"
    # to fix the wildberries count price percentage discount promo bug
    # df[promo_discount_name_actual] = (1 - df[plan_price_name] / df[current_price_name]) * 100
    # df[promo_discount_name_actual] = df[promo_discount_name_actual].apply(pandas_handler.false_to_null)
    # df[promo_discount_name_actual] = df[promo_discount_name_actual].apply(lambda x: math.ceil(x))

    # Calculate the discount price
    df["discount_price"] = df[current_price_name] * (1 - df[new_discount_col] / 100)

    # Calculate the price difference
    # df["price_difference"] = df[plan_price_name] / df["discount_price"]
    # df.to_excel("df_promo.xlsx")
    false_list = pandas_handler.NAN_LIST

    df[promo_discount_name] = np.where(df[promo_discount_name].isin(false_list), df[new_discount_col],
                                       df[promo_discount_name])

    df["action_price"] = df[current_price_name] * (1 - df[promo_discount_name] / 100)
    df["price_difference"] = df["action_price"] / df["discount_price"]

    # Apply the discount condition
    allowed_ratio = 1 - allowed_delta_percent / 100

    # Store original promo discounts
    df["Загружаемая скидка для участия в акции_old"] = df[promo_discount_name]
    df["Allowed"] = "Yes"

    # Update promo discounts based on allowed delta percent
    df.loc[df["price_difference"] >= allowed_ratio, new_discount_col] = df[promo_discount_name]
    df.loc[df["price_difference"] < allowed_ratio, new_discount_col] = df[new_discount_col]
    df.loc[df["price_difference"] < allowed_ratio, "Allowed"] = "No"
    # df.to_excel("df_promo2.xlsx")

    return df


def add_k(df):
    # Check if the required columns exist in the DataFrame
    clear_sell_qt_name = 'Ч. Продажа шт.'
    losistics_qt_name = 'Логистика шт.'
    if clear_sell_qt_name in df.columns and losistics_qt_name in df.columns:
        def calculate_ratio(row):
            # Apply the specified logic
            if row[clear_sell_qt_name] == 0:
                return 0
            elif row[losistics_qt_name] == 0 and row[clear_sell_qt_name] == 0:
                return 0
            elif row[losistics_qt_name] == 0 and row[clear_sell_qt_name] != 0:
                return 1
            else:
                return row[clear_sell_qt_name] / row[losistics_qt_name]

        # Apply the function to each row in the DataFrame
        df['Ч. Продажа шт./Логистика шт.'] = df.apply(calculate_ratio, axis=1)
    else:
        print("Required columns are not present in the DataFrame.")

    df.loc[df['Ч. Продажа шт.'] > 0, 'Маржа-себест./ шт.'] = df["Маржа-себест."] / df["Ч. Продажа шт."]

    return df


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


@timing_decorator
def dfs_process(df_list, r: SimpleNamespace) -> tuple[pd.DataFrame, List]:
    """dfs_process..."""
    print("""dfs_process...""")

    # element 0 in list is always general df that was through all df_list
    incl_col = list(INITIAL_COLUMNS_DICT.values())

    # API and YANDEX_DISK getting data into namespace
    d = dfs_from_outside(r)

    # must be refactored into def that gets DF class that contains df (first or combined) and dfs_list for dynamics:

    df = choose_df_in(df_list, is_first_df=r.is_first_df)
    df = dfs_forming(df=df, d=d, r=r, include_columns=incl_col)
    df = pandas_handler.df_merge_drop(left_df=df, right_df=d.df_delivery, left_on='Артикул поставщика',
                                      right_on='Артикул')
    df_dynamic_list = choose_dynamic_df_list_in(df_list, is_dynamic=r.is_dynamic)
    is_dynamic_possible = r.is_dynamic and len(df_dynamic_list) > 1
    df_completed_dynamic_list = [dfs_forming(x, d, r, incl_col) for x in df_dynamic_list if is_dynamic_possible]

    return df, df_completed_dynamic_list


def pattern_splitting(df, prefixes_dict):
    df = df[~df['nmId'].isin(pandas_handler.FALSE_LIST)]
    df['prefix'] = df['Артикул поставщика'].astype(str).apply(lambda x: x.split("-")[0])
    prefixes = list(prefixes_dict.keys())
    df['prefix'] = df['prefix'].apply(lambda x: starts_with_prefix(x, prefixes))
    df['prefix'] = df['prefix'].apply(lambda x: prefixes_dict.get(x, x))
    df['pattern'] = df['Артикул поставщика'].apply(get_second_part)
    df['material'] = df['Артикул поставщика'].apply(get_third_part)
    df['material'] = [MATERIAL_DICT[x] if x in MATERIAL_DICT else y for x, y in zip(df['pattern'], df['material'])]
    return df


def dfs_forming(df, d, r, include_columns) -> pd.DataFrame:
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
                       is_shushary=r.is_shushary, df_storage=d.df_storage)
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
    return df


def choose_df_in(df_list, is_first_df):
    if is_first_df:
        return df_list[0]
    return pd.concat(df_list)


def choose_dynamic_df_list_in(df_list, is_dynamic=False):
    if is_dynamic:
        return df_list
    return []


@timing_decorator
def dfs_dynamic(df_dynamic_list, is_dynamic=True, testing_mode=False, is_upload_yandex=True) -> Type[DataFrame]:
    """dfs_dynamic merges DataFrames on 'Артикул поставщика' and expands dynamic columns."""
    print("dfs_dynamic...")

    if not is_dynamic:
        return pd.DataFrame

    if not df_dynamic_list:
        return pd.DataFrame

    # List of dynamic columns to analyze
    columns_dynamic = ["Маржа-себест.", "Ч. Продажа шт.", "quantityFull", "Логистика", "Хранение"]

    # Start by initializing the first DataFrame
    merged_df = df_dynamic_list[1][['Артикул поставщика'] + columns_dynamic].copy()

    # Iterate over the remaining DataFrames
    for i, df in enumerate(df_dynamic_list[2:]):  # Start from 2 to correctly handle suffixes
        # Merge with the next DataFrame on 'Артикул поставщика'
        merged_df = pd.merge(
            merged_df,
            df[['Артикул поставщика'] + columns_dynamic],
            on='Артикул поставщика',
            how='outer',  # Use 'outer' to keep all articles
            suffixes=('', f'_{i}')
        )

        # Drop duplicate rows based on 'Артикул поставщика' after each merge
        merged_df = merged_df.drop_duplicates(subset='Артикул поставщика')

    # Drop duplicate columns if any exist after merging
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Extract the sorted column names, excluding 'Артикул поставщика'
    sorted_columns = ['Артикул поставщика'] + sorted(
        [col for col in merged_df.columns if col != 'Артикул поставщика']
    )

    # Reorder the DataFrame with the sorted column list
    merged_df = merged_df[sorted_columns]

    # Perform ABC and XYZ analysis
    df_merged_dynamic = abc_xyz(merged_df)

    if is_upload_yandex and not testing_mode:
        yandex_disk_handler.upload_to_YandexDisk(file=merged_df, file_name=nameof(df_merged_dynamic),
                                                 path=app.config['REPORT_DYNAMIC'])

    # Return the final DataFrame with ABC and XYZ categories
    return df_merged_dynamic


def abc_xyz(merged_df):
    # Calculate total margin by summing all 'Маржа-себест.' columns
    margin_columns = [col for col in merged_df.columns if 'Маржа-себест.' in col and "Маржа-себест./ шт" not in col]
    merged_df[margin_columns] = merged_df[margin_columns].applymap(pandas_handler.false_to_null)
    merged_df['Total_Margin'] = merged_df[margin_columns].sum(axis=1)
    most_total_marging = sum([x for x in merged_df['Total_Margin'] if x > 0]) / 2
    most_total_marging_loss = sum([x for x in merged_df['Total_Margin'] if x < 0]) / 2

    # Calculate total margin per article
    total_margin_per_article = merged_df.groupby('Артикул поставщика')['Total_Margin'].sum().reset_index()

    # Sort by total margin in descending order for ABC analysis
    total_margin_per_article = total_margin_per_article.sort_values(by='Total_Margin', ascending=False)
    total_margin_per_article['Cumulative_Margin'] = total_margin_per_article['Total_Margin'].cumsum()
    total_margin_per_article['Cumulative_Percentage'] = (total_margin_per_article['Cumulative_Margin'] /
                                                         total_margin_per_article['Total_Margin'].sum()) * 100

    # Classify into ABC categories

    def classify_abc(row):
        if row["Total_Margin"] > most_total_marging:
            return 'A'
        if row["Total_Margin"] > 0:
            return 'B'
        elif row["Total_Margin"] == 0:
            return 'C'
        elif row["Total_Margin"] < 0 and row["Total_Margin"] > most_total_marging_loss:
            return 'D'
        else:
            return 'E'

    total_margin_per_article['ABC_Category'] = total_margin_per_article.apply(classify_abc, axis=1)

    # Add ABC categories to merged_df
    merged_df = merged_df.merge(total_margin_per_article[['Артикул поставщика', 'ABC_Category']],
                                on='Артикул поставщика', how='left')

    # XYZ Analysis with Weighted CV
    sales_quantity_columns = [col for col in merged_df.columns if col.startswith('Ч. Продажа шт.')]

    # Define weights for sales periods
    weights = np.linspace(1, 0.5, len(sales_quantity_columns))  # Adjust this based on your needs
    weights = weights / weights.sum()  # Normalize weights

    def calculate_weighted_cv(row):
        quantities = row[sales_quantity_columns].replace('', 0).astype(
            float).values  # Convert empty strings to NaN and then to float
        if np.all(pd.isna(quantities)):  # Handle all NaN case
            return float('inf')  # Handle division by zero if all quantities are NaN
        weighted_mean = np.average(quantities, weights=weights, returned=False)
        weighted_std_dev = np.sqrt(np.average((quantities - weighted_mean) ** 2, weights=weights))
        if weighted_mean == 0:
            return float('inf')  # Handle division by zero if all quantities are zero
        return weighted_std_dev / weighted_mean

    merged_df['CV'] = merged_df.apply(calculate_weighted_cv, axis=1)

    # Convert CV to numeric, coercing errors to NaN
    merged_df['CV'] = pd.to_numeric(merged_df['CV'], errors='coerce')

    # Create CV_mod column with absolute values, preserving NaN for non-numeric entries
    merged_df['CV_mod'] = merged_df['CV'].apply(lambda x: abs(x) if pd.notna(x) else x)

    # Classify into XYZ Categories
    periods = len(sales_quantity_columns)

    def classify_xyz(row):
        cv = row['CV']
        if cv <= periods / 8 and cv > 0:
            return 'W'
        elif cv <= periods / 4 and cv > 0:
            return 'X'
        elif cv <= periods and cv > 0:
            return 'Y'
        else:
            return 'Z'

    merged_df['XYZ_Category'] = merged_df.apply(classify_xyz, axis=1)

    # Ensure no empty values in Total_Margin and CV, fill them with 0
    merged_df["Total_Margin"].replace(["", np.inf, -np.inf], 0, inplace=True)
    merged_df["CV"].replace(["", np.inf, -np.inf], 0, inplace=True)

    # In case we want to ensure CV has a minimum non-zero value for comparison
    max_cv = merged_df["CV"].max()
    merged_df["CV"].replace(0, max_cv, inplace=True)  # Replace 0 CV with the maximum CV value if necessary
    first_columns = ["Артикул поставщика", "Total_Margin", "ABC_Category", "CV", "XYZ_Category"]
    merged_df = merged_df.reindex(
        columns=first_columns + [col for col in merged_df.columns if col not in first_columns])

    return merged_df


@timing_decorator
def influence_discount_by_dynamic(df, df_dynamic, default_margin=1000, k=1):
    if df_dynamic.empty:
        return df

    dynamic_columns_names = DINAMIC_COLUMNS

    # Select relevant columns from df_dynamic
    df_dynamic = df_dynamic[["Артикул поставщика"] + dynamic_columns_names]

    # Merge df_dynamic into df
    df = pandas_handler.df_merge_drop(df, df_dynamic, "Артикул поставщика", "Артикул поставщика")

    # Calculate the number of periods to adjust Total_Margin for periodic sales
    periods_count = len([x for x in df_dynamic.columns if "Ч. Продажа шт." in x])
    medium_total_margin = df["Total_Margin"] / periods_count if periods_count > 0 else df["Total_Margin"]

    # Calculate discounts based on Total_Margin and CV
    df["ABC_discount"] = medium_total_margin / default_margin  # Adjust this to scale as needed
    df["CV_discount"] = df["CV"].apply(pandas_handler.false_to_null)
    df['ABC_CV_discount'] = k * df["ABC_discount"] / df["CV_discount"].apply(abs)
    df['ABC_CV_discount'] = df['ABC_CV_discount'].apply(pandas_handler.false_to_null)
    df['ABC_CV_discount'] = df['ABC_CV_discount'].apply(pandas_handler.inf_to_null)
    df["new_discount"] = df["new_discount"] - df['ABC_CV_discount']

    return df


def in_positive_digit(df, decimal=0, col_names=None):
    if col_names is None:  # Default empty check
        return df
    if isinstance(col_names, str):  # Handle single column name
        col_names = [col_names]

    for col in col_names:
        if col not in df.columns:  # Ensure the column exists
            continue
        # Set negative values to 0
        df[col] = df[col].apply(lambda x: max(0, x))
        # Round the values
        df[col] = df[col].round(decimal)

    return df


def starts_with_prefix(string, prefixes):
    for prefix in prefixes:
        if string.startswith(prefix):
            if len(string) > 10:
                return ''
            return prefix  # Return the prefix itself, not prefixes[prefix]
    return string


def get_second_part(x):
    try:
        return str(x).split("-")[1]
    except IndexError:
        # If the string doesn't contain the delimiter '-', return None or any other value as needed
        return ''


def get_third_part(x):
    try:
        return str(x).split("-")[2]
    except IndexError:
        # If the string doesn't contain the delimiter '-', return None or any other value as needed
        return ''


def df_concatenate(df_list, is_xyz):
    if not is_xyz: df_list = pd.concat(df_list)

    # Check if concatenate parameter is passed
    return df_list


@timing_decorator
def get_data_from(request) -> SimpleNamespace:
    r = SimpleNamespace()
    r.days_by = int(request.form.get('days_by', app.config['DAYS_PERIOD_DEFAULT']))
    r.uploaded_files = request.files.getlist("file")
    r.testing_mode = request.form.get('is_testing_mode')
    r.is_promo_file = request.files.get("is_promo_file")
    r.is_just_concatenate = 'is_just_concatenate' in request.form
    r.is_discount_template = 'is_discount_template' in request.form
    r.is_dynamic = 'is_dynamic' in request.form
    r.is_chosen_columns = 'is_chosen_columns' in request.form
    r.is_net_cost = 'is_net_cost' in request.form
    r.is_get_storage = 'is_get_storage' in request.form
    r.is_shushary = request.form.get('is_shushary')
    r.is_get_price = request.form.get('is_get_price')
    r.is_get_stock = 'is_get_stock' in request.form
    r.is_funnel = request.form.get('is_funnel')
    r.k_delta = request.form.get('k_delta', 1)
    r.is_mix_discounts = 'is_mix_discounts' in request.form
    r.reset_if_null = request.form.get('reset_if_null')
    r.is_first_df = request.form.get('is_first_df')
    r.k_delta = int(r.k_delta)

    return r


def file_names() -> SimpleNamespace:
    n = SimpleNamespace()
    n.detailing_name = "report_detailing_upload.xlsx"
    n.promo_name = "promo.xlsx"
    n.template_name = "discount_template.xlsx"
    n.df_dynamic_name = "df_dynamic.xlsx"
    return n

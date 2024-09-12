import logging
from app import app
from flask import send_file
import zipfile
import pandas as pd
import numpy as np
import io
from datetime import datetime
from app.modules import pandas_handler, API_WB, yandex_disk_handler, price_module, sales_funnel_module, io_output
from datetime import timedelta

'''Analize detaling WB reports, take all zip files from detailing WB and make one file EXCEL'''

# path = 'detailing/'
# file_names = [f for f in listdir('detailing')]
# print(file_names)

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


def zip_detail_V2(concatenated_dfs, drop_duplicates_in=None):
    article_column_name = 'Артикул поставщика'
    df = concatenated_dfs
    # result.dropna(subset=["Артикул поставщика"], inplace=True)
    # result.to_excel('result.xlsx')

    df = pandas_handler.first_letter_up(df, 'Обоснование для оплаты')
    df = _adding_missing_columns(df)

    sales_name = 'Продажа'
    type_wb_sales_col_name_all = 'Вайлдберриз реализовал Товар (Пр)'
    type_sales_col_name_all = 'К перечислению за товар ИТОГО'

    backs_name = 'Возврат'

    logistic_name = 'Логистика'
    type_delivery_service_col_name = 'Услуги по доставке товара покупателю'

    substituted_col_name = 'Компенсация подмененного товара'
    damages_col_name = 'Компенсация ущерба'
    penalty_col_name = 'Штраф'
    type_sales_col_name = 'К перечислению Продавцу за реализованный Товар'
    type_penalty_col_name = 'Общая сумма штрафов'
    type_acquiring_col_name = 'Возмещение издержек по эквайрингу'
    type_give_col_name = 'Возмещение за выдачу и возврат товаров на ПВЗ'
    type_store_moves_col_name = 'Возмещение издержек по перевозке/по складским операциям с товаром'

    qt_col_name = 'Кол-во'
    qt_logistic_to_col_name = 'Количество доставок'
    qt_logistic_back_col_name = 'Количество возврата'

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

    # warehouse_operation_col_name = 'Возмещение издержек по перевозке/по складским операциям с товаром'
    # type_warehouse_operation_col_name = 'Возмещение издержек по перевозке/по складским операциям с товаром'
    #
    # storage_col_name = 'Хранение'
    # penalty_col_name = 'Штраф'

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
    # df.to_excel("Viruchka.xlsx")
    df['Выручка-Логистика'] = df['Выручка'] - df[logistic_name]
    df['Дней в продаже'] = [days_between(d1, datetime.today()) for d1 in df['Дата заказа покупателем']]

    if drop_duplicates_in:
        df.drop_duplicates(subset=drop_duplicates_in)
    # df.to_excel('V2.xlsx')

    return df


def merge_stock(df, df_stock, testing_mode, is_get_stock, is_delete_shushary=False):
    if is_get_stock:
        df_stock = pandas_handler.upper_case(df_stock, 'supplierArticle')[0]
        df = df_stock.merge(df, how='outer', left_on='nmId', right_on='Код номенклатуры')
        df = df.fillna(0)
        df = pandas_handler.fill_empty_val_by('Код номенклатуры', df, 'nmId')
        return df


def merge_storage(df, storage_cost, testing_mode, is_get_storage, is_delete_shushary=False, df_storage=None):
    if not is_get_storage:
        return df

    if df_storage is None or df_storage.empty:
        df_storage = API_WB.get_average_storage_cost(testing_mode=testing_mode, is_delete_shushary=is_delete_shushary)

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


def merge_price(df, df_price, testing_mode, is_get_price):
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
    df.to_excel("df_date.xlsx", index=False)

    return df


# def get_dynamic_sales_old(df,
#                       type_column='Тип документа',
#                       type_name='Продажа',
#                       article_column='Артикул поставщика',
#                       sales_column='К перечислению Продавцу за реализованный Товар',
#                       date_column='Дата продажи'):
#     """
#     Calculate dynamic sales based on the midpoint of the date range.
#
#     Parameters:
#         df (DataFrame): Input DataFrame containing sales data.
#         article_column (str): Name of the column containing article identifiers.
#         sales_column (str): Name of the column containing sales data.
#         date_column (str): Name of the column containing date data.
#
#     Returns:
#         DataFrame: DataFrame with dynamic sales information appended as new columns.
#     """
#     # Convert date_column to datetime format only for rows where Продажи != 0
#     df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
#     df.loc[df[sales_column] == 0, date_column] = pd.NaT  # Set dates to NaT for zero sales
#
#     # Find the minimum and maximum dates after replacements
#     min_date = df[date_column].min()
#     max_date = df[date_column].max()
#
#     # Calculate the midpoint
#     mid_date = min_date + (max_date - min_date) / 2
#
#     # Split DataFrame into two halves based on the midpoint date
#     first_half = df[df[date_column] <= mid_date]
#     second_half = df[df[date_column] > mid_date]
#
#     # Filter DataFrame to include only sales where 'Тип документа' is equal to 'Продажа'
#     first_half_sales = first_half[first_half[type_column] == type_name]
#     second_half_sales = second_half[second_half[type_column] == type_name]
#
#     # Calculate sum of sales_column for each article_column in each half
#     sales1 = first_half_sales.groupby(article_column)[sales_column].sum().reset_index()
#     sales2 = second_half_sales.groupby(article_column)[sales_column].sum().reset_index()
#
#     # Merge sums back into original DataFrame
#     df = pd.merge(df, sales1, on=article_column, how='left', suffixes=('', '_1'))
#     df = pd.merge(df, sales2, on=article_column, how='left', suffixes=('', '_2'))
#
#     return df


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
        periods (int): Number of periods to divide the date range into.
        article_column (str): Name of the column containing article identifiers.
        sales_column (str): Name of the column containing sales data.
        date_column (str): Name of the column containing date data.

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


# def zip_detail(concatenated_dfs, df_net_cost):
#     result = concatenated_dfs
#     result.dropna(subset=["Артикул поставщика"], inplace=True)
#     # result.to_excel('result.xlsx')
#
#     result = pandas_handler.first_letter_up(result, 'Обоснование для оплаты')
#
#     if 'К перечислению за товар' not in result:
#         result['К перечислению за товар'] = 0
#
#     if 'К перечислению Продавцу за реализованный Товар' not in result:
#         result['К перечислению Продавцу за реализованный Товар'] = 0
#
#     if 'Услуги по доставке товара покупателю' not in result:
#         result['Услуги по доставке товара покупателю'] = 0
#
#     if 'Удержания' not in result:
#         result['Удержания'] = 0
#
#     if 'Платная приемка' not in result:
#         result['Платная приемка'] = 0
#
#     result['К перечислению Продавцу за реализованный Товар'].replace(np.NaN, 0, inplace=True)
#     result['К перечислению за товар'].replace(np.NaN, 0, inplace=True)
#
#     result['К перечислению за товар ИТОГО'] = result['К перечислению за товар'] + result[
#         'К перечислению Продавцу за реализованный Товар']
#
#     df_pivot = result.pivot_table(index=['Артикул поставщика'],
#                                   columns='Обоснование для оплаты',
#                                   values=['К перечислению за товар ИТОГО',
#                                           'Вознаграждение Вайлдберриз (ВВ), без НДС',
#                                           'Количество доставок',
#                                           'Количество возврата',
#                                           'Услуги по доставке товара покупателю',
#                                           ],
#                                   aggfunc={'К перечислению за товар ИТОГО': sum,
#                                            'Вознаграждение Вайлдберриз (ВВ), без НДС': sum,
#                                            'Количество доставок': 'count',
#                                            'Количество возврата': 'count',
#                                            'Услуги по доставке товара покупателю': 'count', },
#                                   margins=False)
#
#     # Calculate the mean date for 'Дата заказа покупателем' and 'Дата продажи'
#     mean_date_order = pd.to_datetime(result['Дата заказа покупателем']).mean()
#     mean_date_sale = pd.to_datetime(result['Дата продажи']).mean()
#
#     # Fill empty cells in specific columns with default values
#     default_values = {
#         'Код номенклатуры': 0,
#         'Предмет': 'None',
#         'Бренд': 'None',
#         'Услуги по доставке товара покупателю': 0,
#         'Платная приемка': 0,
#         'Цена розничная с учетом согласованной скидки': 0,
#         'Дата заказа покупателем': mean_date_order.strftime(STRFORMAT_DEFAULT),
#         'Дата продажи': mean_date_sale.strftime(STRFORMAT_DEFAULT),
#     }
#
#     for col, default_value in default_values.items():
#         result[col].fillna(default_value, inplace=True)
#
#     df_pivot.to_excel("df_pivot_revenue.xlsx")
#
#     df_pivot2 = result.pivot_table(index=['Артикул поставщика'],
#                                    values=['Код номенклатуры',
#                                            'Предмет',
#                                            'Бренд',
#                                            'Услуги по доставке товара покупателю',
#                                            'Удержания',
#                                            'Платная приемка',
#                                            'Цена розничная с учетом согласованной скидки',
#                                            'Дата заказа покупателем',
#                                            'Дата продажи',
#
#                                            ],
#                                    aggfunc={'Код номенклатуры': max,
#                                             'Предмет': max,
#                                             'Бренд': max,
#                                             'Услуги по доставке товара покупателю': sum,
#                                             'Удержания': sum,
#                                             'Платная приемка': sum,
#                                             'Цена розничная с учетом согласованной скидки': max,
#                                             'Дата заказа покупателем': min,
#                                             'Дата продажи': min,
#
#                                             },
#                                    margins=False)
#
#     df_result = df_pivot.merge(df_pivot2, how='left', on='Артикул поставщика')
#     df_result.to_excel("df_result.xlsx")
#     if not isinstance(df_net_cost, bool):
#         if not 'Артикул поставщика' in df_result:
#             df_result['Артикул поставщика'] = ''
#         df_result['Артикул поставщика'].fillna(df_result['supplierArticle'])
#         df_result = df_result.merge(df_net_cost.rename(columns={'article': 'Артикул поставщика'}), how='outer',
#                                     on='Артикул поставщика')
#
#     df_result.replace(np.NaN, 0, inplace=True)
#
#     if ('К перечислению за товар ИТОГО', 'Продажа') not in df_result:
#         df_result[('К перечислению за товар ИТОГО', 'Продажа')] = 0
#
#     if ('К перечислению за товар ИТОГО', 'Возврат') not in df_result:
#         df_result[('К перечислению за товар ИТОГО', 'Возврат')] = 0
#
#     df_result['Продажи'] = df_result[('К перечислению за товар ИТОГО', 'Продажа')]
#
#     df_result['Возвраты, руб.'] = df_result[('К перечислению за товар ИТОГО', 'Возврат')]
#
#     df_result['Маржа'] = df_result['Продажи'] - \
#                          df_result["Услуги по доставке товара покупателю"] - \
#                          df_result['Возвраты, руб.'] - \
#                          df_result['Удержания'] - \
#                          df_result['Платная приемка']
#
#     if 'net_cost' in df_result:
#         df_result['net_cost'].replace(np.NaN, 0, inplace=True)
#     else:
#         df_result['net_cost'] = 0
#
#     df_result['Чист. покупок шт.'] = df_result[('Количество доставок', 'Продажа')] - df_result[
#         ('Количество доставок', 'Возврат')]
#
#     df_result['Маржа / логистика'] = df_result['Маржа'] / df_result["Услуги по доставке товара покупателю"]
#     df_result['Продажи к возвратам'] = df_result['Продажи'] / df_result['Возвраты, руб.']
#     df_result['Маржа / доставковозвратам'] = df_result['Маржа'] / (
#             df_result[('Количество доставок', 'Продажа')] -
#             df_result[('Количество доставок', 'Возврат')])
#
#     df_result['Продаж'] = df_result[('Количество доставок', 'Продажа')]
#     df_result['Возврат шт.'] = df_result[('Количество доставок', 'Возврат')]
#     df_result['Логистика'] = df_result[('Услуги по доставке товара покупателю', 'Логистика')]
#     df_result['Доставки/Возвраты, руб.'] = df_result[('Количество доставок', 'Продажа')] / df_result[
#         ('Количество доставок', 'Возврат')]
#
#     df_result['Маржа-себест.'] = df_result['Маржа'] - df_result['net_cost'] * df_result['Чист. покупок шт.']
#     df_result['Себестоимость продаж'] = df_result['net_cost'] * df_result['Чист. покупок шт.']
#     df_result['Покатали раз'] = df_result[('Услуги по доставке товара покупателю', 'Логистика')]
#     df_result['Покатушка средне, руб.'] = df_result['Услуги по доставке товара покупателю'] / df_result[
#         ('Услуги по доставке товара покупателю', 'Логистика')]
#
#     df_result.replace([np.inf, -np.inf], np.nan, inplace=True)
#
#     if 'Предмет_x' in df_result.columns and 'Предмет_y' in df_result.columns:
#         df_result['Предмет_x'].fillna(df_result['Предмет_y'])
#
#     today = datetime.today()
#     print(f'today {today}')
#     df_result['Дней в продаже'] = [days_between(d1, today) for d1 in df_result['Дата заказа покупателем']]
#
#     # result.to_excel('result.xlsx')
#
#     # df_result = df_result[[
#     #     'Бренд',
#     #     'Предмет_x',
#     #     'Артикул поставщика',
#     #     'Код номенклатуры',
#     #     'Маржа-себест.',
#     #     'Маржа',
#     #     'Чист. покупок шт.',
#     #     'Продажа_1',
#     #     'Продажа_2',
#     #     'Продажи',
#     #     'Возвраты, руб.',
#     #     'Продаж',
#     #     'Возврат шт.',
#     #     'Услуги по доставке товара покупателю',
#     #     'Покатушка средне, руб.',
#     #     'Покатали раз',
#     #     'net_cost',
#     #     'company_id',
#     #     'Маржа / логистика',
#     #     'Продажи к возвратам',
#     #     'Маржа / доставковозвратам',
#     #     'Логистика',
#     #     'Доставки/Возвраты, руб.',
#     #     'Себестоимость продаж',
#     #     'Поставщик',
#     #     'Дата заказа покупателем',
#     #     'Дата продажи',
#     #     'Дней в продаже',
#     #
#     # ]]
#
#     df_result = df_result.reindex(df_result.columns, axis=1)
#     df_result = df_result.round(decimals=0).sort_values(by=['Маржа-себест.'], ascending=False)
#
#     # Clean the "Дата заказа покупателем" column
#     df_result["Дата заказа покупателем"] = df_result["Дата заказа покупателем"].replace({0: np.nan}).dropna()
#
#     # Convert the cleaned "Дата заказа покупателем" column to datetime
#     df_result["Дата заказа покупателем"] = pd.to_datetime(df_result["Дата заказа покупателем"])
#     df_result.to_excel('df_result.xlsx')
#     return df_result


def promofiling(promo_file, df, allowed_delta_percent=5):
    if not promo_file:
        return None

    # Read the promo file into a DataFrame
    df_promo = pd.read_excel(promo_file)

    # Merge df_promo with df
    # df_promo = df_promo.merge(df, how='left', left_on="Артикул WB", right_on="nmId")
    df_promo = pandas_handler.df_merge_drop(df_promo, df, "Артикул WB", "nmId", how='outer')
    df_promo = check_discount(df_promo, allowed_delta_percent)

    return df_promo


def check_discount(df, allowed_delta_percent):
    plan_price_name = "Плановая цена для акции"
    current_price_name = "Текущая розничная цена"
    new_discount_col = "new_discount"
    promo_discount_name = "Загружаемая скидка для участия в акции"

    # Calculate the discount price
    df["discount_price"] = df[current_price_name] * (1 - df[new_discount_col] / 100)

    # Calculate the price difference
    df["price_difference"] = df[plan_price_name] / df["discount_price"]

    # Apply the discount condition
    allowed_ratio = 1 - allowed_delta_percent / 100

    # Store original promo discounts
    df["Загружаемая скидка для участия в акции_old"] = df[promo_discount_name]
    df["Allowed"] = "Yes"

    # Update promo discounts based on allowed delta percent
    df.loc[df["price_difference"] >= allowed_ratio, promo_discount_name] = df[promo_discount_name]
    df.loc[df["price_difference"] < allowed_ratio, promo_discount_name] = df[new_discount_col]
    df.loc[df["price_difference"] < allowed_ratio, "Allowed"] = "No"

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


def dfs_process(dfs, request, testing_mode=False, is_dynamic=False):
    """dfs_process..."""
    print("""dfs_process...""")

    # element 0 in list is always general df that was through all df_list
    INCLUDE_COLUMNS = list(INITIAL_COLUMNS_DICT.values())
    days_by = int(request.form.get('days_by'))
    if not days_by: days_by = int(app.config['DAYS_PERIOD_DEFAULT'])
    print(f"days_by {days_by}")

    is_net_cost = request.form.get('is_net_cost')
    is_get_storage = request.form.get('is_get_storage')
    is_delete_shushary = request.form.get('is_delete_shushary')
    is_get_price = request.form.get('is_get_price')
    is_get_stock = request.form.get('is_get_stock')
    is_funnel = request.form.get('is_funnel')
    k_delta = request.form.get('k_delta')
    is_mix_discounts = request.form.get('is_mix_discounts')
    is_discount_template = request.form.get('is_discount_template')
    reset_if_null = request.form.get('reset_if_null')
    is_first_df = request.form.get('is_first_df')
    print(f"is_discount_template {is_discount_template}")
    if not k_delta: k_delta = 1
    k_delta = int(k_delta)

    # API and YANDEX_DISK getting data
    df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
    df_price, _ = API_WB.get_wb_price_api(testing_mode=testing_mode)
    df_rating = yandex_disk_handler.get_excel_file_from_ydisk(app.config['RATING'])
    request_dict = {'no_sizes': 'no_sizes', 'no_city': 'no_city'}
    df_stock = API_WB.get_wb_stock_api(testing_mode=testing_mode, request=request_dict,
                                       is_delete_shushary=is_delete_shushary)
    if is_funnel:
        df_funnel, funnel_file_name = API_WB.get_wb_sales_funnel_api(request, testing_mode=testing_mode)

    if not is_dynamic:
        dfs = pd.concat(dfs)
        dfs = [dfs]

    if is_dynamic:
        print("is_dynamic...")
        if is_first_df:
            print("is_first_df...")
            dfs = [dfs[0]] + dfs
        else:
            dfs = [pd.concat(dfs)] + dfs

    dfs_final_list = []
    dfs_names = []

    df_storage = API_WB.get_average_storage_cost(testing_mode=testing_mode, is_delete_shushary=is_delete_shushary)

    for i, df in enumerate(dfs):
        df = replace_incorrect_date(df)
        date_min = df["Дата продажи"].min()
        print(f"date_min {date_min}")
        # concatenated_dfs.to_excel('ex.xlsx')
        date_max = df["Дата продажи"].max()

        dfs_sales, INCLUDE_COLUMNS = get_dynamic_sales(df, days_by, INCLUDE_COLUMNS)

        # concatenated_dfs = detailing.get_dynamic_sales(concatenated_dfs)

        storage_cost = get_storage_cost(df)

        df = zip_detail_V2(df, drop_duplicates_in="Артикул поставщика")

        df = merge_stock(df, df_stock, testing_mode=testing_mode, is_get_stock=is_get_stock,
                         is_delete_shushary=is_delete_shushary)

        if not 'quantityFull' in df.columns: df['quantityFull'] = 0
        df['quantityFull'].replace(np.NaN, 0, inplace=True)
        df['quantityFull + Продажа, шт.'] = df['quantityFull'] + df['Продажа, шт.']

        df = merge_storage(df, storage_cost, testing_mode, is_get_storage=is_get_storage,
                           is_delete_shushary=is_delete_shushary, df_storage=df_storage)
        df = merge_net_cost(df, df_net_cost, is_net_cost)
        df = merge_price(df, df_price, testing_mode, is_get_price).drop_duplicates(
            subset='nmId')
        df = profit_count(df)
        # df = df.merge(df_rating, how='outer', left_on='nmId', right_on="Артикул")
        df = pandas_handler.df_merge_drop(df, df_rating, 'nmId', 'Артикул', how="outer")
        # df.to_excel("rating_merged.xlsx")

        df = pandas_handler.fill_empty_val_by(['article', 'vendorCode', 'supplierArticle'], df, 'Артикул поставщика')
        df = pandas_handler.fill_empty_val_by(['brand'], df, 'Бренд')
        df = df.rename(columns={'Предмет_x': 'Предмет'})
        df = pandas_handler.fill_empty_val_by(['category'], df, 'Предмет')

        if dfs_sales:
            print(f"merging dfs_sales ...")
            for d in dfs_sales:
                df = pandas_handler.df_merge_drop(df, d, 'nmId', 'Код номенклатуры', how="outer")
                # df = df.merge(d, how='left', left_on='nmId', right_on='Код номенклатуры')

        # df.to_excel("sales_merged.xlsx")

        # --- DICOUNT ---

        df = get_period_sales(df, date_min, date_max)
        k_norma_revenue = price_module.count_norma_revenue(df)
        df = price_module.discount(df, k_delta=k_delta, k_norma_revenue=k_norma_revenue, reset_if_null=reset_if_null)
        discount_columns = sales_funnel_module.DISCOUNT_COLUMNS
        # discount_columns['buyoutsCount'] = 'Ч. Продажа шт.'

        # df = price_module.k_dynamic(df, days_by=days_by)
        # 27/04/2024 - not yet prepared
        # df[discount_columns['func_discount']] *= df['k_dynamic']

        # Reorder the columns

        #  --- PATTERN SPLITTING ---
        df = df[~df['nmId'].isin(pandas_handler.FALSE_LIST)]
        df['prefix'] = df['Артикул поставщика'].astype(str).apply(lambda x: x.split("-")[0])
        prefixes_dict = PREFIXES_ART_DICT
        prefixes = list(prefixes_dict.keys())
        df['prefix'] = df['prefix'].apply(lambda x: starts_with_prefix(x, prefixes))
        df['prefix'] = df['prefix'].apply(lambda x: prefixes_dict.get(x, x))
        df['pattern'] = df['Артикул поставщика'].apply(get_second_part)
        df['material'] = df['Артикул поставщика'].apply(get_third_part)
        df['material'] = [MATERIAL_DICT[x] if x in MATERIAL_DICT else y for x, y in zip(df['pattern'], df['material'])]

        if is_funnel and not df_funnel.empty:
            # df = df.merge(df_funnel, how='outer', left_on='nmId', right_on="nmID")
            df = pandas_handler.df_merge_drop(df, df_funnel, "nmId", "nmID")
            df = sales_funnel_module.calculate_discount(df, discount_columns=discount_columns)
            df = price_module.mix_discounts(df, is_mix_discounts)

        df = add_k(df)

        # print(INCLUDE_COLUMNS)
        include_column = [col for col in INCLUDE_COLUMNS if col in df.columns]
        df = df[include_column + [col for col in df.columns if col not in INCLUDE_COLUMNS]]
        df = pandas_handler.round_df_if(df, half=10)
        if 'new_discount' not in df.columns: df['new_discount'] = df['n_discount']
        dfs_final_list.append(df)
        dfs_names.append(f"df_{str(i)}.xlsx")

    return dfs_final_list, dfs_names


def dfs_dynamic(df_list, is_dynamic=True):
    """dfs_dynamic merges DataFrames on 'Артикул поставщика' and expands dynamic columns."""
    print("dfs_dynamic...")

    # List of dynamic columns to analyze
    columns_dynamic = ["Маржа-себест.", "Ч. Продажа шт.", "quantityFull", "Логистика", "Хранение"]

    if len(df_list) > 1 and is_dynamic:
        # Start by initializing the first DataFrame
        merged_df = df_list[1][['Артикул поставщика'] + columns_dynamic].copy()

        # Iterate over the remaining DataFrames
        for i, df in enumerate(df_list[2:]):  # Start from 2 to correctly handle suffixes
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
        merged_df = abc_xyz(merged_df)

        # Return the final DataFrame with ABC and XYZ categories
        return merged_df

    else:
        # Return the list if not dynamic or only one DataFrame
        return ""


def abc_xyz(merged_df):
    # Calculate total margin by summing all 'Маржа-себест.' columns
    margin_columns = [col for col in merged_df.columns if 'Маржа-себест.' in col and "Маржа-себест./ шт" not in col]
    merged_df['Total_Margin'] = merged_df[margin_columns].sum(axis=1)

    # Calculate total margin per article
    total_margin_per_article = merged_df.groupby('Артикул поставщика')['Total_Margin'].sum().reset_index()

    # Sort by total margin in descending order for ABC analysis
    total_margin_per_article = total_margin_per_article.sort_values(by='Total_Margin', ascending=False)
    total_margin_per_article['Cumulative_Margin'] = total_margin_per_article['Total_Margin'].cumsum()
    total_margin_per_article['Cumulative_Percentage'] = (total_margin_per_article['Cumulative_Margin'] /
                                                         total_margin_per_article['Total_Margin'].sum()) * 100

    # Classify into ABC categories
    def classify_abc(row):
        if row['Cumulative_Percentage'] <= 10 and row["Total_Margin"] > 0:
            return 'A'
        elif row['Cumulative_Percentage'] <= 40 and row["Total_Margin"] >= 0:
            return 'B'
        elif row['Cumulative_Percentage'] <= 70 and row["Total_Margin"] <= 0:
            return 'C'
        else:
            return 'D'

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
        quantities = row[sales_quantity_columns].values
        weighted_mean = np.average(quantities, weights=weights)
        weighted_std_dev = np.sqrt(np.average((quantities - weighted_mean) ** 2, weights=weights))
        if weighted_mean == 0:
            return float('inf')  # Handle division by zero if all quantities are zero
        return weighted_std_dev / weighted_mean

    merged_df['CV'] = merged_df.apply(calculate_weighted_cv, axis=1)

    # Classify into XYZ Categories
    def classify_xyz(row):
        cv = row['CV']
        if cv <= 1 and cv > 0:
            return 'X'
        elif cv <= 3 and cv > 0:
            return 'Y'
        else:
            return 'Z'

    merged_df['XYZ_Category'] = merged_df.apply(classify_xyz, axis=1)

    # Ensure no empty values in Total_Margin and CV, fill them with 0
    merged_df["Total_Margin"].replace(["", np.inf, -np.inf], 0, inplace=True)
    merged_df["CV"].replace(["", np.inf, -np.inf], 0, inplace=True)

    # In case we want to ensure CV has a minimum non-zero value for comparison
    max_cv = merged_df["CV"].max()
    merged_df["CV"] = merged_df["CV"].replace(0, max_cv)  # Replace 0 CV with the maximum CV value

    return merged_df


def influence_discount_by_dynamic(df, df_dynamic, default_margin=1000, k=1):
    dynamic_columns_names = DINAMIC_COLUMNS
    if df_dynamic.empty:
        return df

    # Select relevant columns from df_dynamic
    df_dynamic = df_dynamic[["Артикул поставщика"] + dynamic_columns_names]

    # Merge df_dynamic into df
    df = pandas_handler.df_merge_drop(df, df_dynamic, "Артикул поставщика", "Артикул поставщика")

    # Calculate the number of periods to adjust Total_Margin for periodic sales
    periods_count = len([x for x in df_dynamic.columns if "Ч. Продажа шт." in x])
    medium_total_margin = df["Total_Margin"] / periods_count if periods_count > 0 else df["Total_Margin"]

    # Calculate discounts based on Total_Margin and CV
    df["ABC_discount"] = medium_total_margin / default_margin  # Adjust this to scale as needed
    df["CV_discount"] = df["CV"]
    df["new_discount"] = df["new_discount"] - k * df["ABC_discount"] / df["CV_discount"].apply(abs)

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

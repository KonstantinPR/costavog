import logging
from app import app
import zipfile
import pandas as pd
import numpy as np
import io
from datetime import datetime
from app.modules import pandas_handler, API_WB, yandex_disk_handler
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


def get_period_sales(df, date_min, date_max, default_amount_days=DEFAULT_AMOUNT_DAYS, days_fading=0.25):
    days_period = (date_max - date_min).days
    # days_period = 7

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


def zip_detail_V2(concatenated_dfs, drop_duplicates_in=None):
    article_column_name = 'Артикул поставщика'
    df = concatenated_dfs
    # result.dropna(subset=["Артикул поставщика"], inplace=True)
    # result.to_excel('result.xlsx')

    df = pandas_handler.first_letter_up(df, 'Обоснование для оплаты')

    if 'К перечислению за товар' not in df:
        df['К перечислению за товар'] = 0

    if 'К перечислению Продавцу за реализованный Товар' not in df:
        df['К перечислению Продавцу за реализованный Товар'] = 0

    df['К перечислению Продавцу за реализованный Товар'].replace(np.NaN, 0, inplace=True)

    df['К перечислению за товар'].replace(np.NaN, 0, inplace=True)

    df['К перечислению за товар ИТОГО'] = df['К перечислению за товар'] + df[
        'К перечислению Продавцу за реализованный Товар']

    sales_name = 'Продажа'
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
    dfs = [df_sales, df_backs, df_logistic, df_compensation_substituted, df_compensation_damages, df_penalty,
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

    df['Ч. Продажа'] = df['Продажа'] - df['Возврат']
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


def merge_stock(df, testing_mode, is_get_stock, is_delete_shushary=False):
    if is_get_stock:
        request_dict = {'no_sizes': 'no_sizes', 'no_city': 'no_city'}
        df_stock = API_WB.get_wb_stock_api(testing_mode=testing_mode, request=request_dict,
                                           is_delete_shushary=is_delete_shushary)
        df_stock = pandas_handler.upper_case(df_stock, 'supplierArticle')
        df = df_stock.merge(df, how='outer', left_on='nmId', right_on='Код номенклатуры')
        df = df.fillna(0)
        df = pandas_handler.fill_empty_val_by('Код номенклатуры', df, 'nmId')
        return df


def merge_storage(df, storage_cost, testing_mode, is_get_storage, is_delete_shushary=False):
    if not is_get_storage:
        return df

    df_storage = API_WB.get_average_storage_cost(testing_mode=testing_mode,
                                                 is_delete_shushary=is_delete_shushary)
    df_storage = pandas_handler.upper_case(df_storage, 'vendorCode')
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


def merge_net_cost(df, is_net_cost):
    if is_net_cost:
        df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
        df_net_cost['net_cost'].replace(np.NaN, 0, inplace=True)
        df_net_cost = pandas_handler.upper_case(df_net_cost, 'article')
        df = pandas_handler.df_merge_drop(df, df_net_cost, 'nmId', 'nm_id', how="outer")
        # df.to_excel("net_cost_merged.xlsx")
    else:
        df['net_cost'] = 0

    return df


def profit_count(df):
    df = df.fillna(0)
    df.to_excel("profit_count.xlsx")
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


def merge_price(df, testing_mode, is_get_price):
    if is_get_price:
        df_price, _ = API_WB.get_wb_price_api(testing_mode=testing_mode)
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
    df_list = []

    z = zipfile.ZipFile(zip_downloaded)
    for f in z.namelist():
        # get directory name from file
        content = io.BytesIO(z.read(f))
        zip_file = zipfile.ZipFile(content)
        for i in zip_file.namelist():
            excel_0 = zip_file.read(i)
            df = pd.read_excel(excel_0)
            df_list.append(df)
    return df_list


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
    df_list = zips_to_list(zip_downloaded)

    result = pd.concat(df_list)

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


def zip_detail(concatenated_dfs, df_net_cost):
    result = concatenated_dfs
    result.dropna(subset=["Артикул поставщика"], inplace=True)
    # result.to_excel('result.xlsx')

    result = pandas_handler.first_letter_up(result, 'Обоснование для оплаты')

    if 'К перечислению за товар' not in result:
        result['К перечислению за товар'] = 0

    if 'К перечислению Продавцу за реализованный Товар' not in result:
        result['К перечислению Продавцу за реализованный Товар'] = 0

    if 'Услуги по доставке товара покупателю' not in result:
        result['Услуги по доставке товара покупателю'] = 0

    if 'Удержания' not in result:
        result['Удержания'] = 0

    if 'Платная приемка' not in result:
        result['Платная приемка'] = 0

    result['К перечислению Продавцу за реализованный Товар'].replace(np.NaN, 0, inplace=True)
    result['К перечислению за товар'].replace(np.NaN, 0, inplace=True)

    result['К перечислению за товар ИТОГО'] = result['К перечислению за товар'] + result[
        'К перечислению Продавцу за реализованный Товар']

    df_pivot = result.pivot_table(index=['Артикул поставщика'],
                                  columns='Обоснование для оплаты',
                                  values=['К перечислению за товар ИТОГО',
                                          'Вознаграждение Вайлдберриз (ВВ), без НДС',
                                          'Количество доставок',
                                          'Количество возврата',
                                          'Услуги по доставке товара покупателю',
                                          ],
                                  aggfunc={'К перечислению за товар ИТОГО': sum,
                                           'Вознаграждение Вайлдберриз (ВВ), без НДС': sum,
                                           'Количество доставок': 'count',
                                           'Количество возврата': 'count',
                                           'Услуги по доставке товара покупателю': 'count', },
                                  margins=False)

    # Calculate the mean date for 'Дата заказа покупателем' and 'Дата продажи'
    mean_date_order = pd.to_datetime(result['Дата заказа покупателем']).mean()
    mean_date_sale = pd.to_datetime(result['Дата продажи']).mean()

    # Fill empty cells in specific columns with default values
    default_values = {
        'Код номенклатуры': 0,
        'Предмет': 'None',
        'Бренд': 'None',
        'Услуги по доставке товара покупателю': 0,
        'Платная приемка': 0,
        'Цена розничная с учетом согласованной скидки': 0,
        'Дата заказа покупателем': mean_date_order.strftime(STRFORMAT_DEFAULT),
        'Дата продажи': mean_date_sale.strftime(STRFORMAT_DEFAULT),
    }

    for col, default_value in default_values.items():
        result[col].fillna(default_value, inplace=True)

    df_pivot.to_excel("df_pivot_revenue.xlsx")

    df_pivot2 = result.pivot_table(index=['Артикул поставщика'],
                                   values=['Код номенклатуры',
                                           'Предмет',
                                           'Бренд',
                                           'Услуги по доставке товара покупателю',
                                           'Удержания',
                                           'Платная приемка',
                                           'Цена розничная с учетом согласованной скидки',
                                           'Дата заказа покупателем',
                                           'Дата продажи',

                                           ],
                                   aggfunc={'Код номенклатуры': max,
                                            'Предмет': max,
                                            'Бренд': max,
                                            'Услуги по доставке товара покупателю': sum,
                                            'Удержания': sum,
                                            'Платная приемка': sum,
                                            'Цена розничная с учетом согласованной скидки': max,
                                            'Дата заказа покупателем': min,
                                            'Дата продажи': min,

                                            },
                                   margins=False)

    df_result = df_pivot.merge(df_pivot2, how='left', on='Артикул поставщика')
    df_result.to_excel("df_result.xlsx")
    if not isinstance(df_net_cost, bool):
        if not 'Артикул поставщика' in df_result:
            df_result['Артикул поставщика'] = ''
        df_result['Артикул поставщика'].fillna(df_result['supplierArticle'])
        df_result = df_result.merge(df_net_cost.rename(columns={'article': 'Артикул поставщика'}), how='outer',
                                    on='Артикул поставщика')

    df_result.replace(np.NaN, 0, inplace=True)

    if ('К перечислению за товар ИТОГО', 'Продажа') not in df_result:
        df_result[('К перечислению за товар ИТОГО', 'Продажа')] = 0

    if ('К перечислению за товар ИТОГО', 'Возврат') not in df_result:
        df_result[('К перечислению за товар ИТОГО', 'Возврат')] = 0

    df_result['Продажи'] = df_result[('К перечислению за товар ИТОГО', 'Продажа')]

    df_result['Возвраты, руб.'] = df_result[('К перечислению за товар ИТОГО', 'Возврат')]

    df_result['Маржа'] = df_result['Продажи'] - \
                         df_result["Услуги по доставке товара покупателю"] - \
                         df_result['Возвраты, руб.'] - \
                         df_result['Удержания'] - \
                         df_result['Платная приемка']

    if 'net_cost' in df_result:
        df_result['net_cost'].replace(np.NaN, 0, inplace=True)
    else:
        df_result['net_cost'] = 0

    df_result['Чист. покупок шт.'] = df_result[('Количество доставок', 'Продажа')] - df_result[
        ('Количество доставок', 'Возврат')]

    df_result['Маржа / логистика'] = df_result['Маржа'] / df_result["Услуги по доставке товара покупателю"]
    df_result['Продажи к возвратам'] = df_result['Продажи'] / df_result['Возвраты, руб.']
    df_result['Маржа / доставковозвратам'] = df_result['Маржа'] / (
            df_result[('Количество доставок', 'Продажа')] -
            df_result[('Количество доставок', 'Возврат')])

    df_result['Продаж'] = df_result[('Количество доставок', 'Продажа')]
    df_result['Возврат шт.'] = df_result[('Количество доставок', 'Возврат')]
    df_result['Логистика'] = df_result[('Услуги по доставке товара покупателю', 'Логистика')]
    df_result['Доставки/Возвраты, руб.'] = df_result[('Количество доставок', 'Продажа')] / df_result[
        ('Количество доставок', 'Возврат')]

    df_result['Маржа-себест.'] = df_result['Маржа'] - df_result['net_cost'] * df_result['Чист. покупок шт.']
    df_result['Себестоимость продаж'] = df_result['net_cost'] * df_result['Чист. покупок шт.']
    df_result['Покатали раз'] = df_result[('Услуги по доставке товара покупателю', 'Логистика')]
    df_result['Покатушка средне, руб.'] = df_result['Услуги по доставке товара покупателю'] / df_result[
        ('Услуги по доставке товара покупателю', 'Логистика')]

    df_result.replace([np.inf, -np.inf], np.nan, inplace=True)

    if 'Предмет_x' in df_result.columns and 'Предмет_y' in df_result.columns:
        df_result['Предмет_x'].fillna(df_result['Предмет_y'])

    today = datetime.today()
    print(f'today {today}')
    df_result['Дней в продаже'] = [days_between(d1, today) for d1 in df_result['Дата заказа покупателем']]

    # result.to_excel('result.xlsx')

    # df_result = df_result[[
    #     'Бренд',
    #     'Предмет_x',
    #     'Артикул поставщика',
    #     'Код номенклатуры',
    #     'Маржа-себест.',
    #     'Маржа',
    #     'Чист. покупок шт.',
    #     'Продажа_1',
    #     'Продажа_2',
    #     'Продажи',
    #     'Возвраты, руб.',
    #     'Продаж',
    #     'Возврат шт.',
    #     'Услуги по доставке товара покупателю',
    #     'Покатушка средне, руб.',
    #     'Покатали раз',
    #     'net_cost',
    #     'company_id',
    #     'Маржа / логистика',
    #     'Продажи к возвратам',
    #     'Маржа / доставковозвратам',
    #     'Логистика',
    #     'Доставки/Возвраты, руб.',
    #     'Себестоимость продаж',
    #     'Поставщик',
    #     'Дата заказа покупателем',
    #     'Дата продажи',
    #     'Дней в продаже',
    #
    # ]]

    df_result = df_result.reindex(df_result.columns, axis=1)
    df_result = df_result.round(decimals=0).sort_values(by=['Маржа-себест.'], ascending=False)

    # Clean the "Дата заказа покупателем" column
    df_result["Дата заказа покупателем"] = df_result["Дата заказа покупателем"].replace({0: np.nan}).dropna()

    # Convert the cleaned "Дата заказа покупателем" column to datetime
    df_result["Дата заказа покупателем"] = pd.to_datetime(df_result["Дата заказа покупателем"])
    df_result.to_excel('df_result.xlsx')
    return df_result


def promofiling(promo_file, df, allowed_delta_percent=10):
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

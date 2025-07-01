from pandas import DataFrame
import pandas as pd
import numpy as np
from datetime import datetime
from app.modules import pandas_handler, API_WB
from app.modules.pattern_splitting_module import starts_with_prefix, get_second_part, get_third_part, \
    empty_for_not_found
from app.modules.zip_detail_V2_module import adding_missing_columns, add_col, pivot_expanse, days_between
from app.modules.detailing_upload_dict_module import DEFAULT_AMOUNT_DAYS, PREFIXES_ART_DICT, MATERIAL_DICT


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


def get_storage_cost(df):
    if 'Хранение' in df.columns:
        storage_cost = df['Хранение'].sum()
    else:
        df['Хранение'] = 0
        storage_cost = 0
    return storage_cost


def zip_detail_V2(concatenated_dfs, drop_duplicates_in=None):
    article_column_name = 'Артикул поставщика'
    df = concatenated_dfs
    # result.dropna(subset=["Артикул поставщика"], inplace=True)
    # result.to_excel('result.xlsx')

    df = pandas_handler.first_letter_up(df, 'Обоснование для оплаты')
    df = adding_missing_columns(df)
    df = df.sort_values(by='Дата заказа покупателем', ascending=True)

    sales_name = 'Продажа'
    type_wb_sales_col_name_all = 'Вайлдберриз реализовал Товар (Пр)'
    type_sales_col_name_all = 'К перечислению за товар ИТОГО'

    backs_name = 'Возврат'

    logistic_name = 'Логистика'
    logistic_correction_name = 'Коррекция логистики'
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
    df_logistic_correction = pivot_expanse(df, logistic_correction_name, type_delivery_service_col_name)
    df_compensation_substituted = pivot_expanse(df, substituted_col_name, type_sales_col_name)
    df_compensation_damages = pivot_expanse(df, damages_col_name, type_sales_col_name)
    df_penalty = pivot_expanse(df, penalty_col_name, type_penalty_col_name)
    df_acquiring = pivot_expanse(df, sales_name, type_acquiring_col_name, col_name="Эквайринг")
    df_give_to = pivot_expanse(df, sales_name, type_give_col_name, col_name="При выдачи от")
    df_give_back = pivot_expanse(df, backs_name, type_give_col_name, col_name="При выдачи в")

    # похоже не влияет на нашу прибыль (удерживается из комиссии вайлдберриз)
    df_store_moves = pivot_expanse(df, type_store_moves_col_name, type_store_moves_col_name, col_name='Склады удержали')

    df_qt_sales = pivot_expanse(df, sales_name, qt_col_name, col_name='Продажа, шт.')
    df_qt_backs = pivot_expanse(df, backs_name, qt_col_name, col_name='Возврат, шт.')
    df_qt_logistic_to = pivot_expanse(df, logistic_name, qt_logistic_to_col_name, col_name='Логистика до, шт.')
    df_qt_logistic_back = pivot_expanse(df, logistic_name, qt_logistic_back_col_name, col_name='Логистика от, шт.')

    df = df.drop_duplicates(subset=[article_column_name])
    dfs = [df_wb_sales, df_wb_backs, df_sales, df_backs, df_logistic, df_compensation_substituted,
           df_compensation_damages, df_penalty,
           df_acquiring, df_give_to, df_give_back, df_store_moves, df_qt_sales, df_qt_backs, df_qt_logistic_to,
           df_qt_logistic_back, df_logistic_correction]

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
    # df['Удержания_minus'] = df[penalty_col_name] + df['Эквайринг'] + df['При выдачи от'] + df['При выдачи в'] + df[
    #     'Склады удержали']

    # Похоже 'Склады удержали' не влияет на минус и удерживается неявно из комиссии вайлдберриз напрямую
    df['Удержания_minus'] = df[penalty_col_name] + df['Эквайринг'] + df['При выдачи от'] + df['При выдачи в']

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


def merge_storage(df, storage_cost, testing_mode, is_get_storage, is_shushary=False, df_storage=None,
                  files_period_days=1):
    default_period_days = 7
    if not is_get_storage:
        return df

    if df_storage is None or df_storage.empty:
        df_storage = API_WB.get_storage_cost(testing_mode=testing_mode, is_shushary=is_shushary)

    df_storage = pandas_handler.upper_case(df_storage, 'vendorCode')[0]
    # df = df.merge(df_storage, how='outer', left_on='nmId', right_on='nmId')
    df = pandas_handler.df_merge_drop(df, df_storage, 'nmId', 'nmId', how="outer")
    # df.to_excel("merged_storage.xlsx")
    df = df.fillna(0)
    if 'Days_between_First_Now' not in df.columns:
        df['Days_between_First_Now'] = default_period_days
    else:
        df.loc[df['Days_between_First_Now'] > files_period_days, 'Days_between_First_Now'] = files_period_days
        df.loc[df['Days_between_First_Now'] == 0, 'Days_between_First_Now'] = files_period_days

    df['Хранение'] = df['quantityFull + Продажа, шт.'] * df['storagePricePerBarcode'] * df['Days_between_First_Now']
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


def merge_price(df, df_price, is_get_price):
    if is_get_price:
        # df = df.merge(df_price, how='outer', left_on='nmId', right_on='nmID')
        df = pandas_handler.df_merge_drop(df, df_price, 'nmId', 'nmID', how="outer")
        # df.to_excel("price_merged.xlsx")
        df['price_disc'] = df['price'] - df['price'] * df['discount'] / 100
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


def get_period_sales(df, date_min, date_max, default_amount_days=DEFAULT_AMOUNT_DAYS, days_fading=0.25):
    days_period = (date_max - date_min).days

    df['days_period'] = days_period
    df['smooth_days'] = (df['days_period'] / default_amount_days) ** days_fading
    print(f"smooth_days {df['smooth_days'].mean()}")
    return df


def replace_incorrect_date(df, date_column='Дата продажи'):
    # Convert the 'Дата продажи' column to datetime format

    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

    # Define the specific dates to remove
    specific_dates = ['2022-09-25', '2022-10-02']

    # Filter out rows with the specific dates and modify the original DataFrame in-place

    df = df[~df[date_column].isin(specific_dates)]

    # Print the minimum valid date after replacing dates
    # print(f"date_min {df[date_column].min()}")

    # Save the DataFrame to an Excel file
    # df.to_excel("df_date.xlsx", index=False)

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


def pattern_splitting(df, prefixes_dict):
    df = df[~df['nmId'].isin(pandas_handler.FALSE_LIST)]
    df['prefix'] = df['Артикул поставщика'].astype(str).apply(lambda x: x.split("-")[0])
    prefixes = list(prefixes_dict.keys())
    df['prefix'] = df['prefix'].apply(lambda x: starts_with_prefix(x, prefixes))
    df['prefix'] = df['prefix'].apply(lambda x: prefixes_dict.get(x, x))

    df['prefix'] = df['prefix'].apply(lambda x: empty_for_not_found(x, prefixes))
    df['pattern'] = df['Артикул поставщика'].apply(get_second_part)
    df['material'] = df['Артикул поставщика'].apply(get_third_part)
    df['material'] = [MATERIAL_DICT[x] if x in MATERIAL_DICT else y for x, y in zip(df['pattern'], df['material'])]
    return df

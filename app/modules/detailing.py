from app import app
import zipfile
import pandas as pd
import numpy as np
import io
from datetime import datetime
import requests

'''Analize detaling WB reports, take all zip files from detailing WB and make one file EXCEL'''

# path = 'detailing/'
# file_names = [f for f in listdir('detailing')]
# print(file_names)

STRFORMAT_DEFAULT = '%Y-%m-%d'


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


def concatenate_detailing_modul(zip_downloaded, df_net_cost):
    df_list = zips_to_list(zip_downloaded)
    result = pd.concat(df_list)
    return result


def days_between(d1, d2):
    if d1:
        d1 = datetime.strptime(d1, STRFORMAT_DEFAULT)
        # d2 = datetime.strptime(d2, "%Y-%m-%d")
        return abs((d2 - d1).days)
    return None


def zip_detail(zip_downloaded, df_net_cost):
    df_list = zips_to_list(zip_downloaded)
    result = pd.concat(df_list)
    result.dropna(subset=["Артикул поставщика"], inplace=True)
    result.to_excel('result.xlsx')

    if 'Обоснование для оплаты' in result:
        result['Обоснование для оплаты'] = [str(s)[0].upper() + str(s)[1:] if isinstance(s, str) else s for s in
                                            result['Обоснование для оплаты']]
    else:
        print("Column 'Обоснование для оплаты' not found in the result dictionary.")

    if 'К перечислению за товар' not in result:
        result['К перечислению за товар'] = 0

    if 'К перечислению Продавцу за реализованный Товар' not in result:
        result['К перечислению Продавцу за реализованный Товар'] = 0

    if 'Услуги по доставке товара покупателю' not in result:
        result['Услуги по доставке товара покупателю'] = 0

    if 'Хранение' not in result:
        result['Хранение'] = 0

    if 'Удержания' not in result:
        result['Удержания'] = 0

    if 'Платная приемка' not in result:
        result['Платная приемка'] = 0

    result['К перечислению Продавцу за реализованный Товар'].replace(np.NaN, 0, inplace=True)
    result['К перечислению за товар'].replace(np.NaN, 0, inplace=True)

    result['К перечислению за товар ИТОГО'] = result['К перечислению за товар'] + result[
        'К перечислению Продавцу за реализованный Товар']

    result.to_excel("dlkjfg.xlsx")

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
        'Хранение': 0,
        'Удержания': 0,
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
                                           'Хранение',
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
                                            'Хранение': sum,
                                            'Удержания': sum,
                                            'Платная приемка': sum,
                                            'Цена розничная с учетом согласованной скидки': max,
                                            'Дата заказа покупателем': min,
                                            'Дата продажи': min,

                                            },
                                   margins=False)

    df_result = df_pivot.merge(df_pivot2, how='left', on='Артикул поставщика')

    if not isinstance(df_net_cost, bool):
        if 'Артикул поставщика' in df_result:
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
                         df_result['Хранение'] - \
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
    df_result['Маржа-себест. за шт. руб'] = df_result['Маржа-себест.'] / df_result['Чист. покупок шт.']
    df_result['Себестоимость продаж'] = df_result['net_cost'] * df_result['Чист. покупок шт.']
    df_result['Покатали раз'] = df_result[('Услуги по доставке товара покупателю', 'Логистика')]
    df_result['Покатушка средне, руб.'] = df_result['Услуги по доставке товара покупателю'] / df_result[
        ('Услуги по доставке товара покупателю', 'Логистика')]

    df_result.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_result['Предмет_x'].fillna(df_result['Предмет_y'])
    today = datetime.today()
    print(f'today {today}')
    df_result['Дней в продаже'] = [days_between(d1, today) for d1 in df_result['Дата заказа покупателем']]

    df_result = df_result[[
        'Бренд',
        'Предмет_x',
        'Артикул поставщика',
        'Код номенклатуры',
        'Маржа-себест.',
        'Маржа',
        'Чист. покупок шт.',
        'Продажи',
        'Возвраты, руб.',
        'Продаж',
        'Возврат шт.',
        'Услуги по доставке товара покупателю',
        'Покатушка средне, руб.',
        'Маржа-себест. за шт. руб',
        'Покатали раз',
        'net_cost',
        'company_id',
        'Маржа / логистика',
        'Продажи к возвратам',
        'Маржа / доставковозвратам',
        'Логистика',
        'Доставки/Возвраты, руб.',
        'Себестоимость продаж',
        'Поставщик',
        'Дата заказа покупателем',
        'Дата продажи',
        'Дней в продаже',

    ]]

    df_result = df_result.reindex(df_result.columns, axis=1)
    df_result = df_result.round(decimals=0).sort_values(by=['Маржа-себест.'], ascending=False)

    # Clean the "Дата заказа покупателем" column
    df_result["Дата заказа покупателем"] = df_result["Дата заказа покупателем"].replace({0: np.nan}).dropna()

    # Convert the cleaned "Дата заказа покупателем" column to datetime
    df_result["Дата заказа покупателем"] = pd.to_datetime(df_result["Дата заказа покупателем"])

    return df_result

# def get_wb_sales_api(date_from: datetime, days_step: int):
#     """get sales as api wb sales describe"""
#     path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/sales?dateFrom="
#     date_from = date_from
#     flag = "Z&flag=0&"
#     api_key = app.config['WB_API_TOKEN']
#     path_all = f"{path_start}{date_from}{flag}key={api_key}"
#     response = requests.get(path_all)
#     data = response.json()
#     df = pd.DataFrame(data)
#
#     return df

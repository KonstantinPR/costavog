import os
import zipfile
import pandas as pd
import numpy as np
import os
import io
from app.modules import io_output
from os import listdir
import datetime
import requests

'''Analize detaling WB reports, take all zip files from detailing WB and make one file EXCEL'''


# path = 'detailing/'
# file_names = [f for f in listdir('detailing')]
# print(file_names)


def to_round_df(df_result):
    df_result = df_result.round(decimals=0)
    return df_result


def get_wb_stock():
    response = requests.get(
        'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?dateFrom=2017-03-25T21%3A00%3A00.000Z&key=DYNwYTcxYjUtMDNjZi00Zjc1LTg3NDQtN2RiM2E2MWRmYzA1')

    data = response.json()
    df = pd.DataFrame(data)


    df_pivot = df.pivot_table(index=['supplierArticle'],
                              values=['quantity',
                                      'quantityFull',
                                      'inWayToClient',
                                      'inWayFromClient',
                                      'Price',
                                      'Discount',
                                      'daysOnSite'],
                              aggfunc={'quantity': sum,
                                       'quantityFull': sum,
                                       'inWayToClient': sum,
                                       'inWayFromClient': sum,
                                       'Price': max,
                                       'Discount': max,
                                       'daysOnSite': max},
                              margins=False)

    df_pivot['finishPrice'] = df_pivot['Price'] - df_pivot['Price'] * (df_pivot['Discount'] / 100)
    df = to_round_df(df_pivot)

    return df


def zip_detail(zip_downloaded, df_net_cost):
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

    result = pd.concat(df_list)

    if 'К перечислению за товар' not in result:
        result['К перечислению за товар'] = 0

    if 'Услуги по доставке товара покупателю' not in result:
        result['Услуги по доставке товара покупателю'] = 0

    if not 'К перечислению Продавцу за реализованный Товар' in result:
        result['К перечислению Продавцу за реализованный Товар'] = ''

    df_pivot = result.pivot_table(index=['Артикул поставщика'],
                                  columns='Обоснование для оплаты',
                                  values=['К перечислению Продавцу за реализованный Товар',
                                          'К перечислению за товар',
                                          'Вознаграждение Вайлдберриз (ВВ), без НДС',
                                          'Количество доставок',
                                          'Количество возврата',
                                          'Услуги по доставке товара покупателю',
                                          ],
                                  aggfunc={'К перечислению Продавцу за реализованный Товар': sum,
                                           'К перечислению за товар': sum,
                                           'Вознаграждение Вайлдберриз (ВВ), без НДС': sum,
                                           'Количество доставок': 'count',
                                           'Количество возврата': 'count',
                                           'Услуги по доставке товара покупателю': 'count'},
                                  margins=False)

    df_pivot2 = result.pivot_table(index=['Артикул поставщика'],
                                   values=['Код номенклатуры',
                                           'Предмет',
                                           'Бренд',
                                           'Услуги по доставке товара покупателю'],
                                   aggfunc={'Код номенклатуры': max,
                                            'Предмет': max,
                                            'Бренд': max,
                                            'Услуги по доставке товара покупателю': sum},
                                   margins=False)

    df_result = df_pivot.merge(df_pivot2, how='left', on='Артикул поставщика')

    if not isinstance(df_net_cost, bool):
        df_result = df_result.merge(df_net_cost.rename(columns={'article': 'Артикул поставщика'}), how='left',
                                    on='Артикул поставщика')

        print(df_result)

        df_result.replace(np.NaN, 0, inplace=True)

        if ('К перечислению за товар', 'Продажа') not in df_result:
            df_result[('К перечислению за товар', 'Продажа')] = 0

        if ('К перечислению за товар', 'Возврат') not in df_result:
            df_result[('К перечислению за товар', 'Возврат')] = 0

        print(df_result)

        df_result['Продажи'] = df_result[('К перечислению Продавцу за реализованный Товар', 'Продажа')] + \
                               df_result[('К перечислению за товар', 'Продажа')]

        df_result['Возвраты, руб.'] = df_result[('К перечислению Продавцу за реализованный Товар', 'Возврат')] + \
                                      df_result[('К перечислению за товар', 'Возврат')]

        df_result['Маржа'] = df_result['Продажи'] - \
                             df_result["Услуги по доставке товара покупателю"] - \
                             df_result['Возвраты, руб.']

        if 'net_cost' in df_result:
            df_result['net_cost'].replace(np.NaN, 0, inplace=True)
        else:
            df_result['net_cost'] = 0

        df_result['Покупок шт.'] = df_result[('Количество доставок', 'Продажа')] - df_result[
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

        df_result['Маржа-себест.'] = df_result['Маржа'] - df_result['net_cost'] * df_result['Покупок шт.']
        df_result['Маржа-себест. за шт. руб'] = df_result['Маржа-себест.'] / df_result['Покупок шт.']
        df_result['Себестоимость продаж'] = df_result['net_cost'] * df_result['Покупок шт.']
        df_result['Покатали раз'] = df_result[('Услуги по доставке товара покупателю', 'Логистика')]
        df_result['Покатушка средне, руб.'] = df_result['Услуги по доставке товара покупателю'] / df_result[
            ('Услуги по доставке товара покупателю', 'Логистика')]

        df_result.replace([np.inf, -np.inf], np.nan, inplace=True)

        df_result = df_result[[
            'Бренд',
            'Предмет',
            'Артикул поставщика',
            'Код номенклатуры',
            'Маржа-себест.',
            'Маржа',
            'Покупок шт.',
            'Продажи',
            'Возвраты, руб.',
            'Услуги по доставке товара покупателю',
            'Покатушка средне, руб.',
            'Маржа-себест. за шт. руб',
            'Покатали раз',
            'net_cost',
            'company_id',
            'Маржа / логистика',
            'Продажи к возвратам',
            'Маржа / доставковозвратам',
            'Продаж',
            'Возврат шт.',
            'Логистика',
            'Доставки/Возвраты, руб.',
            'Себестоимость продаж']]

        df_result = df_result.round(decimals=0).sort_values(by=['Маржа-себест.'], ascending=False)

        return df_result



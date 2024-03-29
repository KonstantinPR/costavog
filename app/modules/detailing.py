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
        d1 = datetime.strptime(d1, "%Y-%m-%d")
        # d2 = datetime.strptime(d2, "%Y-%m-%d")
        return abs((d2 - d1).days)
    return None


def zip_detail(zip_downloaded, df_net_cost):
    df_list = zips_to_list(zip_downloaded)
    result = pd.concat(df_list)
    result.to_excel('result.xlsx')

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
                                           'Услуги по доставке товара покупателю': 'count', },
                                  margins=False)

    df_pivot2 = result.pivot_table(index=['Артикул поставщика'],
                                   values=['Код номенклатуры',
                                           'Предмет',
                                           'Бренд',
                                           'Услуги по доставке товара покупателю',
                                           'Цена розничная с учетом согласованной скидки',
                                           'Дата заказа покупателем',
                                           'Дата продажи',

                                           ],
                                   aggfunc={'Код номенклатуры': max,
                                            'Предмет': max,
                                            'Бренд': max,
                                            'Услуги по доставке товара покупателю': sum,
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
        df_result.to_excel('df_result.xlsx')

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

        return df_result


def get_wb_sales_api(date_from: datetime, days_step: int):
    """get sales as api wb sales describe"""
    path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/sales?dateFrom="
    date_from = date_from
    flag = "Z&flag=0&"
    api_key = app.config['WB_API_TOKEN']
    path_all = f"{path_start}{date_from}{flag}key={api_key}"
    response = requests.get(path_all)
    data = response.json()
    df = pd.DataFrame(data)

    return df

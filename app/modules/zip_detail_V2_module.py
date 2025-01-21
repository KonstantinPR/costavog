import numpy as np
from datetime import datetime
from app.modules.detailing_upload_dict_module import STRFORMAT_DEFAULT


def adding_missing_columns(df):
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


def days_between(d1, d2):
    if d1:
        d1 = datetime.strptime(d1, STRFORMAT_DEFAULT)
        # d2 = datetime.strptime(d2, "%Y-%m-%d")
        return abs((d2 - d1).days)
    return None

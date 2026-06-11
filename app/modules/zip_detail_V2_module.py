import numpy as np
from datetime import datetime
import pandas as pd


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
        df[col_name] = 0
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


def sum_exists(df, *column_names):
    """
    Safely sum multiple columns, ignoring non-existent ones.
    Usage: df['new_col'] = safe_sum(df, 'col1', 'col2', 'col3')
    """
    existing = [col for col in column_names if col in df.columns]

    if not existing:
        print(f"[WARNING] safe_sum: No valid columns found. Columns checked: {column_names}")
        return 0

    missing = [col for col in column_names if col not in df.columns]
    if missing:
        print(f"[DEBUG] safe_sum: Ignoring missing columns: {missing}")

    return df[existing].sum(axis=1)

def days_between(d1, d2):
    """
    Вычисляет количество дней между двумя датами.
    Поддерживает строки с датой, датой+время, а также datetime объекты.
    """
    try:
        # Обработка d1
        if d1 is None or d1 == "":
            return None

        # Если d1 уже datetime объект
        if isinstance(d1, (datetime, pd.Timestamp)):
            date1 = d1 if isinstance(d1, datetime) else d1.to_pydatetime()
        else:
            # Преобразуем в строку и убираем лишние пробелы
            d1_str = str(d1).strip()

            # Формат: YYYY-MM-DD HH:MM:SS
            if ' ' in d1_str and len(d1_str.split(' ')[0].split('-')) == 3:
                date_part = d1_str.split(' ')[0]
                date1 = datetime.strptime(date_part, '%Y-%m-%d')
            # Формат: DD.MM.YYYY HH:MM:SS
            elif ' ' in d1_str and '.' in d1_str.split(' ')[0]:
                date_part = d1_str.split(' ')[0]
                date1 = datetime.strptime(date_part, '%d.%m.%Y')
            # Только дата в формате YYYY-MM-DD
            elif '-' in d1_str and len(d1_str.split('-')) == 3:
                date1 = datetime.strptime(d1_str, '%Y-%m-%d')
            # Только дата в формате DD.MM.YYYY
            elif '.' in d1_str and len(d1_str.split('.')) == 3:
                date1 = datetime.strptime(d1_str, '%d.%m.%Y')
            else:
                # Логируем проблему (можно заменить на print для отладки)
                print(f"Предупреждение: Не удалось распознать формат даты: {d1_str}")
                return None

        # Обработка d2 (сегодняшняя дата)
        if isinstance(d2, (datetime, pd.Timestamp)):
            date2 = d2 if isinstance(d2, datetime) else d2.to_pydatetime()
        else:
            date2 = datetime.strptime(str(d2), '%Y-%m-%d')

        delta = date2 - date1
        return abs(delta.days)

    except Exception as e:
        print(f"Ошибка при обработке даты '{d1}': {e}")
        return None

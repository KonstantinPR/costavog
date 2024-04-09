import zipfile
import pandas as pd
import numpy as np
import io
from datetime import datetime
from app.modules import pandas_handler

'''Analize detaling WB reports, take all zip files from detailing WB and make one file EXCEL'''

# path = 'detailing/'
# file_names = [f for f in listdir('detailing')]
# print(file_names)

STRFORMAT_DEFAULT = '%Y-%m-%d'


def zip_detail_V2(concatenated_dfs):
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

    compensation_substituted_col_name = 'Компенсация подмененного товара'
    type_sales_col_name = 'К перечислению Продавцу за реализованный Товар'

    qt_col_name = 'Кол-во'
    qt_logistic_to_col_name = 'Количество доставок'
    qt_logistic_back_col_name = 'Количество возврата'

    df_sales = pivot_expance(df, sales_name, type_sales_col_name_all)
    df_backs = pivot_expance(df, backs_name, type_sales_col_name_all)
    df_logistic = pivot_expance(df, logistic_name, type_delivery_service_col_name)
    df_compensation_substituted = pivot_expance(df, compensation_substituted_col_name, type_sales_col_name)
    df_qt_sales = pivot_expance(df, sales_name, qt_col_name, col_name='Продажа, шт.')
    df_qt_backs = pivot_expance(df, backs_name, qt_col_name, col_name='Возврат, шт.')
    df_qt_logistic_to = pivot_expance(df, logistic_name, qt_logistic_to_col_name, col_name='Логистика до, шт.')
    df_qt_logistic_back = pivot_expance(df, logistic_name, qt_logistic_back_col_name, col_name='Логистика от, шт.')

    df = df.drop_duplicates(subset=[article_column_name])
    dfs = [df_sales, df_backs, df_logistic, df_compensation_substituted, df_qt_sales, df_qt_backs, df_qt_logistic_to,
           df_qt_logistic_back]
    for d in dfs:
        df = pd.merge(df, d, how='outer', on=article_column_name)
    # dfs_names = [sales_name, logistic_name, backs_name, compensation_substituted_col_name]
    df = df.fillna(0)

    warehouse_operation_col_name = 'Возмещение издержек по перевозке/по складским операциям с товаром'
    type_warehouse_operation_col_name = 'Возмещение издержек по перевозке/по складским операциям с товаром'

    storage_col_name = 'Хранение'
    penalty_col_name = 'Штраф'

    df['Ч. Продажа шт.'] = df['Продажа, шт.'] - df['Возврат, шт.']
    df['Логистика. ед'] = df['Логистика'] / df['Продажа, шт.']
    df['Логистика шт.'] = df['Логистика до, шт.'] + df['Логистика от, шт.']
    df['Маржа'] = df[sales_name] - df[logistic_name] - df[backs_name] - df[compensation_substituted_col_name]
    df['Дней в продаже'] = [days_between(d1, datetime.today()) for d1 in df['Дата заказа покупателем']]

    # df.to_excel('V2.xlsx')

    return df


def pivot_expance(df, type_name, sum_name, agg_col_name='Артикул поставщика', type_col_name='Обоснование для оплаты',
                  col_name=None):
    df_type = df[df[type_col_name] == type_name]
    df = df_type.groupby(agg_col_name)[sum_name].sum().reset_index()
    if col_name:
        df = df.rename(columns={sum_name: f'{str(col_name)}'})
    else:
        df = df.rename(columns={sum_name: f'{str(type_name)}'})
    return df


def process_uploaded_files(uploaded_files):
    if len(uploaded_files) == 1 and uploaded_files[0].filename.endswith('.zip'):
        # If there is only one file and it's a zip file, proceed as usual
        uploaded_file = uploaded_files[0]
    else:
        # If there are multiple files or a single non-zip file, create a zip archive in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for file in uploaded_files:
                zip_file.writestr(file.filename, file.read())

        # Reset the in-memory buffer's position to the beginning
        zip_buffer.seek(0)

        # Set the uploaded_file to the in-memory zip buffer
        uploaded_file = zip_buffer

    return uploaded_file


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


def get_dynamic_sales(df,
                      type_column='Тип документа',
                      type_name='Продажа',
                      article_column='Артикул поставщика',
                      sales_column='К перечислению Продавцу за реализованный Товар',
                      date_column='Дата продажи'):
    """
    Calculate dynamic sales based on the midpoint of the date range.

    Parameters:
        df (DataFrame): Input DataFrame containing sales data.
        article_column (str): Name of the column containing article identifiers.
        sales_column (str): Name of the column containing sales data.
        date_column (str): Name of the column containing date data.

    Returns:
        DataFrame: DataFrame with dynamic sales information appended as new columns.
    """
    # Convert date_column to datetime format only for rows where Продажи != 0
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df.loc[df[sales_column] == 0, date_column] = pd.NaT  # Set dates to NaT for zero sales

    # Replace incorrect dates with min_date
    sorted_df = df.sort_values(by=date_column)
    sorted_df[date_column] = pd.to_datetime(sorted_df[date_column])
    min_date = sorted_df[date_column].min()
    for i in range(len(sorted_df) - 1):
        curr_date = sorted_df.iloc[i][date_column]
        next_date = sorted_df.iloc[i + 1][date_column]

        if pd.isnull(curr_date) or (next_date - curr_date).days > 7:
            sorted_df.at[i, date_column] = min_date

    # Find the minimum and maximum dates after replacements
    min_date = df[date_column].min()
    max_date = df[date_column].max()

    # Calculate the midpoint
    mid_date = min_date + (max_date - min_date) / 2

    # Split DataFrame into two halves based on the midpoint date
    first_half = df[df[date_column] <= mid_date]
    second_half = df[df[date_column] > mid_date]

    # Filter DataFrame to include only sales where 'Тип документа' is equal to 'Продажа'
    first_half_sales = first_half[first_half[type_column] == type_name]
    second_half_sales = second_half[second_half[type_column] == type_name]

    # Calculate sum of sales_column for each article_column in each half
    sales1 = first_half_sales.groupby(article_column)[sales_column].sum().reset_index()
    sales2 = second_half_sales.groupby(article_column)[sales_column].sum().reset_index()

    # Merge sums back into original DataFrame
    df = pd.merge(df, sales1, on=article_column, how='left', suffixes=('', '_1'))
    df = pd.merge(df, sales2, on=article_column, how='left', suffixes=('', '_2'))

    return df


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

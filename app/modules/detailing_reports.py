import json

from app import app
from functools import reduce
import math
import requests
from app.models import Product, db
import datetime
import pandas as pd
import numpy as np
from app.modules import yandex_disk_handler
from app.modules import io_output
import time

VISIBLE_COL = [
    'brand_name',
    'Предмет',
    'nm_id',
    'supplierArticle',
    'Согласованная скидка, %',
    'discount',
    'Согл. скидк - disc',
    'price_disc',
    'Перечисление руб',
    'Прибыль_sum',
    'quantityFull',
    'Остаток в розничных ценах',
    'Логистика руб',
    'Логистика шт',
    'net_cost',
    'quantity_Возврат_sum',
    'quantity_Продажа_sum',
    'Продажи_уч_возврат_sum',
    'k_discount',
    'k_is_sell',
    'k_revenue',
    'k_logistic',
    'k_net_cost',
    'k_qt_full',
    'Номенклатура (код 1С)'
]

IMPORTANT_COL_DESC = [
    'brand_name',
    # 'subject_name',
    # updated 20/09/2022
    'Предмет',
    'nm_id',
    'supplierArticle',
]

IMPORTANT_COL_REPORT = [
    'Согласованная скидка, %',
    'discount',
    'Согл. скидк - disc',
    'price_disc'
    'Перечисление руб',
    'quantityFull',
    'Остаток в розничных ценах',
    'Логистика руб',
    'Логистика шт',
    'net_cost',
    'quantity_Возврат_sum',
    'quantity_Продажа_sum',
    'k_discount',
    'k_is_sell',
    'k_revenue',
    'k_logistic',
    'k_net_cost',
    'k_qt_full',
]

NEW_COL_ON_REVENUE = [

]

DEFAULT_NET_COST = 1000

DATE_FORMAT = "%Y-%m-%d"
DAYS_DELAY_REPORT = 5
DATE_PARTS = 3
K_SMOOTH = 1


def _revenue_per_one(rev, sel, net, log):
    rev_per_one = 0
    if sel and rev:
        return int(rev / sel)
    if log:
        return net - log
    return rev_per_one


def _revenue_net_dif(rev_per, net):
    rev_net_dif = 1
    if rev_per and net:
        rev_net_dif = rev_per / net
    return rev_net_dif


def _revenue_potential_cost(rev_per, net, qt, k_dif):
    rev_pot = net * k_dif * qt
    if rev_per:
        return rev_per * qt
    return rev_pot


def key_indicators_module(file_content):
    key_indicators = {}
    df = file_content

    df['market_cost'] = df['price_disc'] * df['quantityFull']
    key_indicators['market_cost'] = df['market_cost'].sum()
    key_indicators['Перечисление руб'] = df['Перечисление руб'].sum()
    key_indicators['retail_price_Пр_Взвр'] = df['retail_price_withdisc_rub_Продажа_sum'].sum() - df[
        'retail_price_withdisc_rub_Возврат_sum'].sum()
    key_indicators['comission_and_exp_all'] = 1 - (
            key_indicators['Перечисление руб'] / key_indicators['retail_price_Пр_Взвр'])
    print(f"comission_to_wb {key_indicators['comission_and_exp_all']}")
    key_indicators['our_income_for_all'] = key_indicators['market_cost'] * (1 - key_indicators['comission_and_exp_all'])

    key_indicators['net_cost_med'] = (df[df["net_cost"] != 0]["net_cost"] * df[df["net_cost"] != 0][
        "quantityFull"]).sum() / df[df["net_cost"] != 0]["quantityFull"].sum()

    df['net_cost'] = np.where(df.net_cost == 0, key_indicators['net_cost_med'], df.net_cost)

    df['nets_cost'] = df['net_cost'] * df['quantityFull']
    key_indicators['nets_cost'] = df['nets_cost'].sum()
    df['sells_qt_with_back'] = df['quantity_Продажа_sum'] - df['quantity_Возврат_sum']
    key_indicators['sells_qt_with_back'] = df['sells_qt_with_back'].sum()
    df['revenue_per_one'] = [_revenue_per_one(rev, sel, net, log) for
                             rev, sel, net, log in
                             zip(df['Прибыль_sum'],
                                 df['sells_qt_with_back'],
                                 df['net_cost'],
                                 df['Логистика руб'],
                                 )]
    df['revenue_net_dif'] = [_revenue_net_dif(rev_per, net) for
                             rev_per, net in
                             zip(df['revenue_per_one'],
                                 df['net_cost'],
                                 )]

    key_indicators['revenue_net_dif_med'] = df[df["revenue_net_dif"] != 1]["revenue_net_dif"].mean()

    df['revenue_potential_cost'] = [
        _revenue_potential_cost(rev_per, net, qt, k_dif=key_indicators['revenue_net_dif_med']) for
        rev_per, net, qt in
        zip(df['revenue_per_one'],
            df['net_cost'],
            df['quantityFull'],
            )]

    key_indicators['revenue_potential_cost'] = df['revenue_potential_cost'].sum()

    for k, v in key_indicators.items():
        if not 'revenue_net_dif_med' in k:
            print(f'{k} {key_indicators[k]}')
            if key_indicators[k] > 1000:
                key_indicators[k] = int(v)

    df = df.from_dict(key_indicators, orient='index', columns=['key_indicator'])

    return df


def revenue_processing_module(request):
    """forming via wb api table dynamic revenue and correcting discount"""
    # --- REQUEST PROCESSING ---
    if request.form.get('date_from'):
        date_from = request.form.get('date_from')
    else:
        date_from = datetime.datetime.today() - datetime.timedelta(
            days=app.config['DAYS_STEP_DEFAULT']) - datetime.timedelta(DAYS_DELAY_REPORT)
        date_from = date_from.strftime(DATE_FORMAT)

    # print(f"type is {type(date_from)}")

    if request.form.get('date_end'):
        date_end = request.form.get('date_end')
    else:
        date_end = datetime.datetime.today() - datetime.timedelta(DAYS_DELAY_REPORT)
        date_end = date_end.strftime(DATE_FORMAT)
        # date_end = time.strftime(date_format)- datetime.timedelta(3)

    if request.form.get('days_step'):
        days_step = request.form.get('days_step')
    else:
        days_step = app.config['DAYS_STEP_DEFAULT']

    if request.form.get('part_by'):
        date_parts = request.form.get('part_by')
    else:
        date_parts = DATE_PARTS

    if request.form.get('k_smooth'):
        k_smooth = int(request.form.get('k_smooth'))
    else:
        k_smooth = K_SMOOTH

    # --- GET DATA VIA WB API /// ---
    df_sales = get_wb_sales_realization_api(date_from, date_end, days_step)
    df_sales.to_excel('df_sales_excel.xlsx')
    # df_sales = pd.read_excel("df_sales_excel.xlsx")
    df_stock = get_wb_stock_api()
    df_sales.to_excel('wb_stock.xlsx')
    # df_stock = pd.read_excel("wb_stock.xlsx")

    # --- GET NET_COST FROM DB /// ---
    # df_net_cost = pd.read_sql(
    #     db.session.query(Product).filter_by(company_id=app.config['CURRENT_COMPANY_ID']).statement, db.session.bind)

    # --- GET NET_COST FROM YADISK /// ---
    df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])

    df_sales_pivot = get_wb_sales_realization_pivot(df_sales)
    df_sales_pivot.to_excel('sales_pivot.xlsx')
    # таблица с итоговыми значениями с префиксом _sum
    df_sales_pivot.columns = [f'{x}_sum' for x in df_sales_pivot.columns]
    days_bunch = get_days_bunch_from_delta_date(date_from, date_end, date_parts, DATE_FORMAT)
    period_dates_list = get_period_dates_list(date_from, date_end, days_bunch, date_parts)
    df_sales_list = dataframe_divide(df_sales, period_dates_list, date_from)

    # df_pivot_list = []
    df_pivot_list = [get_wb_sales_realization_pivot(d) for d in df_sales_list]

    df = df_pivot_list[0]
    date = iter(period_dates_list[1:])
    df_p = df_pivot_list[1:]
    for d in df_p:
        df_pivot = df.merge(d, how="outer", on='nm_id', suffixes=(None, f'_{str(next(date))[:10]}'))
        df = df_pivot

    # df.to_excel("predsharlotka.xlsx")

    df_price = get_wb_price_api()
    # df_price.to_excel("df_price.xlsx")
    df = df.merge(df_price, how='outer', on='nm_id')
    # df.to_excel("sharlotka.xlsx")

    df_complete = df.merge(df_stock, how='outer', on='nm_id')
    df = df_complete.merge(df_net_cost, how='outer', left_on='nm_id', right_on='nm_id')

    df = get_revenue_by_part(df, period_dates_list)

    df = df.rename(columns={'Прибыль': f"Прибыль_{str(period_dates_list[0])[:10]}"})
    df_revenue_col_name_list = df_revenue_column_name_list(df)

    df.fillna(0, inplace=True, downcast='infer')

    # Формируем обобщающие показатели с префиксом _sum перед присоединением общей таблицы продаж
    df['Прибыль_max'] = df[df_revenue_col_name_list].max(axis=1)
    df['Прибыль_min'] = df[df_revenue_col_name_list].min(axis=1)
    df['Прибыль_sum'] = df[df_revenue_col_name_list].sum(axis=1)
    df['Прибыль_mean'] = df[df_revenue_col_name_list].mean(axis=1)
    df['Прибыль_first'] = df[df_revenue_col_name_list[0]]
    df['Прибыль_last'] = df[df_revenue_col_name_list[len(df_revenue_col_name_list) - 1]]
    df['Прибыль_growth'] = df['Прибыль_last'] - df['Прибыль_first']
    df['Логистика руб'] = df[[col for col in df.columns if "_rub_Логистика" in col]].sum(axis=1)
    df['Логистика шт'] = df[[col for col in df.columns if "_amount_Логистика" in col]].sum(axis=1)
    df['price_disc'] = df['price'] * (1 - df['discount'] / 100)

    # чтобы были видны итоговые значения из первоначальной таблицы с продажами
    df = df.merge(df_sales_pivot, how='outer', on='nm_id')
    df['Продажи_уч_возврат_sum'] = df['quantity_Продажа_sum'] - df['quantity_Возврат_sum']

    df['Перечисление руб'] = df[[col for col in df.columns if "ppvz_for_pay_Продажа_sum" in col]].sum(axis=1) - \
                             df[[col for col in df.columns if "ppvz_for_pay_Возврат_sum" in col]].sum(axis=1) - \
                             df[[col for col in df.columns if "delivery_rub_Логистика" in col]].sum(axis=1)
    # Принятие решения о скидке на основе сформированных данных ---
    # коэффициент влияния на скидку
    df['k_discount'] = 1
    # если не было продаж и текущая цена выше себестоимости, то увеличиваем скидку (коэффициент)
    df = get_k_discount(df, df_revenue_col_name_list)
    df['Согласованная скидка, %'] = round((df['discount'] - (1 - df['k_discount']) * 100) * df['k_discount'], 0)
    df['Согласованная скидка, %'] = [3 if 0 < x < 3 else x for x in df['Согласованная скидка, %']]
    df['Согласованная скидка, %'] = [0 if x < 0 or x == 0 else x for x in df['Согласованная скидка, %']]
    df['Согласованная скидка, %'] = round(df['Согласованная скидка, %'] + \
                                          (df['Согласованная скидка, %'] - df['discount']) / k_smooth, 0)
    df['Согл. скидк - disc'] = df['Согласованная скидка, %'] - df['discount']
    df['Остаток в розничных ценах'] = df['price_disc'] * df['quantityFull']
    # df['Согласованная скидка, %'] = round(df['discount'] + (df['k_discount'] / (1 - df['discount'] / 100)), 0)
    df['Номенклатура (код 1С)'] = df['nm_id']
    df['supplierArticle'] = np.where(df['supplierArticle'] is None, df['article'], df['supplierArticle'])

    # df = detailing_reports.df_revenue_speed(df, period_dates_list)

    list_re_col_names_art = ['article', 'sa_name', 'sa_name_sum']
    df = combine_duplicate_column(df, 'supplierArticle', list_re_col_names_art)
    list_re_col_names_brand = ['brand_name_sum']
    df = combine_duplicate_column(df, 'brand_name', list_re_col_names_brand)
    list_re_col_names_subject = ['subject_name_sum']
    df = combine_duplicate_column(df, 'subject_name', list_re_col_names_subject)
    drop_list = list_re_col_names_brand + list_re_col_names_art + list_re_col_names_subject
    df = df.drop(drop_list, axis=1)
    df = df.drop_duplicates(subset=['Номенклатура (код 1С)'])
    df = df_stay_column_not_null(df)

    # реорганизуем порядок следования столбцов для лучшей читаемости
    # df = df_reorder_important_col_desc_first(df)
    # df = df_reorder_important_col_report_first(df)
    # df = df_reorder_revenue_col_first(df)
    print(df.columns)
    df = df[VISIBLE_COL]
    df = df.sort_values(by='Прибыль_sum')

    file_name = f"wb_dynamic_revenue_report-{str(date_from)}-{str(date_end)}.xlsx"
    file_content = io_output.io_output(df)
    # добавляем полученный файл на яндекс.диск
    yandex_disk_handler.upload_to_yandex_disk(file_content, file_name)

    return df, file_name


def combine_duplicate_column(df, col_name_in: str, list_re_col_names: list):
    """insert in values of col_name_in dataframe column values from list_re_col_name if 0"""
    for col_name_from in list_re_col_names:
        df[col_name_in] = [
            _insert_missing_values(val_col_in, val_col_from) for
            val_col_in, val_col_from in
            zip(df[col_name_in], df[col_name_from])
        ]
    return df


def _insert_missing_values(val_col_in, val_col_from):
    if val_col_in:
        return val_col_in
    return val_col_from


# /// --- K REVENUE FORMING ---
def k_is_sell(sell_sum, net_cost):
    if not net_cost: net_cost = DEFAULT_NET_COST
    k_net_cost = math.sqrt(DEFAULT_NET_COST / net_cost)
    # нет продаж и товара много

    if sell_sum == 0:
        return 1.01
    if sell_sum > 10 * k_net_cost:
        return 0.96
    if sell_sum > 5 * k_net_cost:
        return 0.97
    if sell_sum > 3 * k_net_cost:
        return 0.98

    return 1.01


def k_qt_full(qt):
    k = 1
    if qt <= 3:
        k = 0.97
    if qt <= 5:
        k = 0.98
    if 10 < qt <= 50:
        k = 1.01
    if 50 < qt <= 100:
        k = 1.03
    if 100 < qt <= 1000:
        k = 1.04
    return k


def k_revenue(selqt, sum, mean, last):
    # если одна или менее продаж (совсем мало)
    if selqt <= 1:
        return 1

    # если прибыль растет - можно чуть увеличить цену
    if sum > 0 and mean > 0 and last > 0:
        return 0.99
    # если прибыль отрицательная и падает - минимизируем покатушки - сильно поднимаем цены
    if sum < 0 and mean < 0 and last < 0:
        return 0.96
    # если последний период отрицательный - чуть поднимаем цену для минимизации эффекта покатушек
    if sum > 0 and mean > 0 and last < 0:
        return 0.98
    return 1


def k_logistic(log_rub, to_rub, from_rub, net_cost):
    if not net_cost: net_cost = DEFAULT_NET_COST
    k_net_cost = math.sqrt(DEFAULT_NET_COST / net_cost)

    if to_rub > 0:
        if log_rub > 0.50 * to_rub:
            return 0.96
        if log_rub > 0.25 * to_rub:
            return 0.98

    if log_rub > k_net_cost * net_cost and to_rub == 0:
        return 0.99

    return 1


def k_net_cost(net_cost, price_disc):
    if net_cost == 0:
        net_cost = DEFAULT_NET_COST
    k_net_cost = math.sqrt(DEFAULT_NET_COST / net_cost) * 2
    if k_net_cost < 1:  k_net_cost = 1

    if price_disc <= net_cost:
        return 0.93
    if price_disc <= net_cost * k_net_cost:
        return 0.95
    if price_disc <= net_cost * 1.3 * k_net_cost:
        return 0.98
    if price_disc <= net_cost * 1.1 * k_net_cost:
        return 0.97
    if price_disc >= net_cost * 4 * k_net_cost:
        return 1.05
    if price_disc >= net_cost * 3 * k_net_cost:
        return 1.03
    if price_disc >= net_cost * 2 * k_net_cost:
        return 1.01

    return 1


def get_k_discount(df, df_revenue_col_name_list):
    # если не было продаж увеличиваем скидку
    df['k_is_sell'] = [k_is_sell(x, y) for x, y in zip(df['quantity_Продажа_sum'], df['net_cost'])]
    # постоянно растет или падает прибыль, отрицательная или положительная
    df['k_revenue'] = [k_revenue(w, x, y, z) for w, x, y, z in
                       zip(df['quantity_Продажа_sum'], df['Прибыль_sum'], df['Прибыль_mean'], df['Прибыль_last'])]
    # Защита от покатушек - поднимаем цену
    df['k_logistic'] = [k_logistic(w, x, y, z) for w, x, y, z in
                        zip(df['Логистика руб'], df['ppvz_for_pay_Продажа_sum'], df['ppvz_for_pay_Возврат_sum'],
                            df['net_cost'])]
    # Защита от цены ниже себестоимости - тогда повышаем
    df['k_net_cost'] = [k_net_cost(x, y) for x, y in zip(df['net_cost'], df['price_disc'])]
    df['k_qt_full'] = [k_qt_full(x) for x in df['quantityFull']]
    df['k_discount'] = (df['k_is_sell'] + df['k_revenue'] + df['k_logistic'] + df['k_net_cost'] + df[
        'k_qt_full']) / 5

    return df


# --- K REVENUE FORMING /// ---

# /// --- NEW COLUMN ON REVENUE ANILIZE ---

def df_revenue_growth(df, df_revenue_col_name_list):
    growth1 = df[df_revenue_col_name_list[0]] - df[df_revenue_col_name_list[1]]
    growth2 = df[df_revenue_col_name_list[1]] - df[df_revenue_col_name_list[2]]
    growth = (growth2 - growth1) / growth2
    return growth


def df_revenue_column_name_list(df):
    df_revenue_col_name_list = [col for col in df.columns if f'Прибыль_' in col]
    return df_revenue_col_name_list


# --- NEW COLUMN ON REVENUE ANILIZE /// ---

def dataframe_divide(df, period_dates_list, date_from, date_format="%Y-%m-%d"):
    df['rr_dt'] = [x[0:10] + " 00:00:00" for x in df['rr_dt']]
    df['rr_dt'] = pd.to_datetime(df['rr_dt'])
    # df = df.set_index(df['rr_dt'])
    # df = df.sort_index()
    print(df)

    if isinstance(date_from, str):
        date_from = datetime.datetime.strptime(date_from, date_format)

    df_list = []

    for date_end in period_dates_list:
        print(f"from df date {date_from}")
        print(f"end df date {date_end}")

        # df = df[date_from:date_end]

        d = df[(df['rr_dt'] > date_from) & (df['rr_dt'] <= date_end)]
        print(f"d {d}")
        date_from = date_end
        df_list.append(d)

    return df_list


def get_period_dates_list(date_from, date_end, days_bunch, date_parts=1, date_format="%Y-%m-%d"):
    period_dates_list = []
    date_from = datetime.datetime.strptime(date_from, date_format)
    date_end = datetime.datetime.strptime(date_end, date_format)
    date_end_local = date_from + datetime.timedelta(days_bunch)

    print(type(date_end_local))
    print(type(date_end))

    print(f"date_end_local {date_end_local}\n")
    while date_end_local <= date_end:
        period_dates_list.append(date_end_local)
        print(f"type per list {period_dates_list}")
        print(f"date_parts {date_parts}")
        print(f"date_end_local {date_end_local}")
        print(f"days bunch {days_bunch}")
        date_end_local = date_end_local + datetime.timedelta(days_bunch)
        date_end_local = datetime.datetime(date_end_local.year, date_end_local.month, date_end_local.day)
        print(f"date_local_end {date_end_local}\n")
        print(type(date_end_local))
        print(f"date_end {date_end}\n")

    return period_dates_list


def get_days_bunch_from_delta_date(date_from, date_end, date_parts, date_format="%Y-%m-%d"):
    print(date_from)
    print(date_end)
    date_format = "%Y-%m-%d"
    if not date_parts:
        date_parts = 1
    delta = datetime.datetime.strptime(date_end, date_format) - datetime.datetime.strptime(date_from, date_format)
    delta = delta.days

    days_bunch = int(int(delta) / int(date_parts))
    return days_bunch


def combine_date_to_revenue(date_from, date_end, days_step=7):
    df = get_wb_sales_realization_api(date_from, date_end, days_step)
    df_sales = get_wb_sales_realization_pivot(df)
    df_stock = get_wb_stock_api(date_from)
    df_net_cost = pd.read_sql(
        db.session.query(Product).filter_by(company_id=app.config['CURRENT_COMPANY_ID']).statement, db.session.bind)
    df = df_sales.merge(df_stock, how='outer', on='nm_id')
    df = df.merge(df_net_cost, how='outer', left_on='supplierArticle', right_on='article')
    df = get_revenue(df)
    return df


def get_wb_sales_realization_api(date_from: str, date_end: str, days_step: int):
    """get sales as api wb sales realization describe"""
    t = time.process_time()
    path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?"
    date_from = date_from
    api_key = app.config['WB_API_TOKEN']
    # print(time.process_time() - t)
    limit = 100000
    path_all = f"{path_start}dateFrom={date_from}&key={api_key}&limit={limit}&rrdid=0&dateto={date_end}"
    # path_all_test = f"https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom=2022-06-01&key={api_key}&limit=1000&rrdid=0&dateto=2022-06-25"
    # print(time.process_time() - t)
    response = requests.get(path_all)
    # print(response)
    # print(time.process_time() - t)
    data = response.json()
    # print(time.process_time() - t)
    df = pd.DataFrame(data)
    # print(time.process_time() - t)

    return df


# def df_merge(df_list, ):
#     df_merged = reduce(lambda left, right: pd.merge(left, right, on=['DATE'],
#                                                     how='outer'), df_list).fillna('void')
#     return df_merged

def revenue_correcting(x, y, z, w):
    if z > 0:
        return x - y
    else:
        return x


def get_important_columns(df):
    df = df[[
        'brand_name',
        'subject_name',
        'nm_id',
        'supplierArticle',
        'Прибыль',
        'ppvz_for_pay_Продажа',
        'retail_price_withdisc_rub_Продажа',
        'ppvz_for_pay_Возврат',
        'ppvz_for_pay_Логистика',
        'quantity_Продажа',
        'quantity_Возврат',
        'quantity_Логистика',
        'net_cost',
        'quantityFull',
        'delivery_rub_Возврат',
        'delivery_rub_Логистика',
        'delivery_rub_Продажа',
        'penalty_Возврат',
        'penalty_Логистика',
        'penalty_Продажа',
        'retail_price_withdisc_rub_Возврат',
        'retail_price_withdisc_rub_Логистика',
        'delivery_amount_Логистика',
        'return_amount_Логистика',
        'daysOnSite',
        'article',
        'company_id',
        'sa_name',
    ]]
    print(df)
    return df


def df_reorder_important_col_desc_first(df):
    important_col_list = IMPORTANT_COL_DESC
    n = 0
    col_list = df.columns.tolist()
    for col in important_col_list:
        if col in col_list:
            idx = col_list.index(col)
            col_list[idx], col_list[n] = col_list[n], col_list[idx]
            n += 1
    df = df.reindex(columns=col_list)
    return df


def df_reorder_important_col_report_first(df):
    important_col_list = IMPORTANT_COL_REPORT
    n = len(IMPORTANT_COL_DESC)
    col_list = df.columns.tolist()
    for col in important_col_list:
        if col in col_list:
            idx = col_list.index(col)
            col_list[idx], col_list[n] = col_list[n], col_list[idx]
            n += 1
    df = df.reindex(columns=col_list)
    return df


def df_reorder_revenue_col_first(df):
    n = len(IMPORTANT_COL_DESC) + len(IMPORTANT_COL_REPORT)
    col_list = df.columns.tolist()
    for col in col_list:
        if "Прибыль" in col:
            idx = col_list.index(col)
            col_list[idx], col_list[n] = col_list[n], col_list[idx]
            n += 1
    df = df.reindex(columns=col_list)
    return df


def df_stay_column_not_null(df):
    df = df.loc[:, df.any()]
    return df


def get_revenue_by_part(df: pd.DataFrame, period_dates_list: list = None) -> pd.DataFrame:
    """break up revenue report in parts by date periods"""
    df.replace(np.NaN, 0, inplace=True)

    for date in period_dates_list:
        if period_dates_list.index(date) == 0:
            date = ''
        else:
            date = f"_{str(date)[:10]}"

        df.to_excel('result2.xlsx')
        df[f'Прибыль{date}'] = df[f'ppvz_for_pay_Продажа{date}'] - \
                               df[f'ppvz_for_pay_Возврат{date}'] - \
                               df[f'delivery_rub_Логистика{date}'] - \
                               df[f'quantity_Продажа{date}'] * df['net_cost'] + \
                               df[f'quantity_Возврат{date}'] * df['net_cost']

    return df


def get_revenue(df):
    df.replace(np.NaN, 0, inplace=True)

    df['Прибыль'] = df['ppvz_for_pay_Продажа'] - \
                    df['ppvz_for_pay_Возврат'] - \
                    df['delivery_rub_Логистика'] - \
                    df['quantity_Продажа'] * df['net_cost'] + \
                    df['quantity_Возврат'] * df['net_cost']

    return df


def df_column_set_to_str(df):
    for col in df.columns:
        if isinstance(col, tuple):
            df.rename(columns={col: '_'.join(col)}, inplace=True)
    return df


def _merge_old_column_name(df):
    # соединяем старые названия возврата - корректный вовзрат и продажа - корректная продажа
    if 'ppvz_for_pay_Корректная продажа' in df:
        df['ppvz_for_pay_Продажа'] = df['ppvz_for_pay_Корректная продажа'] + df['ppvz_for_pay_Продажа']
    if 'ppvz_for_pay_Корректный возврат' in df:
        df['ppvz_for_pay_Возврат'] = df['ppvz_for_pay_Корректный возврат'] + df['ppvz_for_pay_Возврат']
    return df


def get_wb_sales_realization_pivot(df):
    print(f"THIS IS DF IN SALES REALIZATION PIVOT {df}")
    df1 = df.pivot_table(index=['nm_id'],
                         columns='supplier_oper_name',
                         values=['ppvz_for_pay',
                                 'delivery_rub',
                                 'penalty',
                                 'quantity',
                                 'delivery_amount',
                                 'return_amount',
                                 'retail_price_withdisc_rub',
                                 'ppvz_sales_commission',
                                 ],
                         aggfunc={'ppvz_for_pay': sum,
                                  'delivery_rub': sum,
                                  'penalty': sum,
                                  'quantity': sum,
                                  'delivery_amount': sum,
                                  'return_amount': sum,
                                  'retail_price_withdisc_rub': sum,
                                  'ppvz_sales_commission': sum,
                                  },
                         margins=False)

    df2 = df.pivot_table(index=['nm_id'],
                         values=['sa_name',
                                 'brand_name',
                                 'subject_name'],
                         aggfunc={'sa_name': max,
                                  'brand_name': max,
                                  'subject_name': max,
                                  },
                         margins=False)

    df = df1.merge(df2, how='left', on='nm_id')
    df = df_column_set_to_str(df)
    df.replace(np.NaN, 0, inplace=True)
    df = _merge_old_column_name(df)

    return df


def get_wb_price_api():
    headers = {
        'accept': 'application/json',
        'Authorization': app.config['WB_API_TOKEN2'],
    }

    response = requests.get('https://suppliers-api.wildberries.ru/public/api/v1/info', headers=headers)
    data = response.json()
    df = pd.DataFrame(data)
    df = df.rename(columns={'nmId': 'nm_id'})
    return df


def get_all_cards_api_wb(textSearch: str = None):
    limit = 1000
    total = 1000
    updatedAt = None
    nmId = None
    dfs = []
    while total >= limit:
        headers = {
            'accept': 'application/json',
            'Authorization': app.config['WB_API_TOKEN2'],
        }

        data = {
            "sort": {
                "cursor": {
                    "limit": total,
                    "updatedAt": updatedAt,
                    "nmID": nmId,
                },
                "filter": {
                    "textSearch": textSearch,
                    "withPhoto": -1
                }
            }
        }

        data = json.dumps(data)
        url = 'https://suppliers-api.wildberries.ru/content/v1/cards/cursor/list'

        response = requests.post(url, data=data, headers=headers)

        print(type(response))
        print(response)
        df_json = response.json()
        print(type(df_json))
        print(df_json)
        print(df_json['data']['cursor']['total'])
        print(df_json['data']['cursor']['updatedAt'])
        print(df_json['data']['cursor']['nmID'])

        total = df_json['data']['cursor']['total']
        updatedAt = df_json['data']['cursor']['updatedAt']
        nmId = df_json['data']['cursor']['nmID']
        # df = pd.DataFrame(df_json['data']['cards'])
        dfs = dfs + df_json['data']['cards']

    # df = pd.concat(dfs)
    # dfs = dfs.explode('sizes')
    # df = dfs.join(pd.json_normalize(dfs.pop('sizes')))
    # df_n = pd.json_normalize(dfs, 'sizes')
    # df = dfs.join(df_n)
    df = pd.json_normalize(dfs, 'sizes', ["vendorCode", "colors", "brand", 'nmID'])

    # df['sizes'] = [list_dict_to_str(x) for x in df['sizes']]

    # df = df.rename(columns={'nmId': 'nm_id'})
    return df


# def list_dict_to_str(x):
#     key_value = ""
#     for i in x:
#         for index, (key, value) in enumerate(i.items()):
#             new_value = value
#             if index % 2:
#                 sep = ","
#             else:
#                 sep = ":"
#             key_value = [key_value + f"{new_value}{sep}"]
#
#     return key_value


def get_wb_stock_api():
    """to modify wb stock"""

    df = df_wb_stock_api()

    df = df.pivot_table(index=['nmId'],
                        values=['quantity',
                                'daysOnSite',
                                'supplierArticle',
                                ],
                        aggfunc={'quantity': sum,
                                 'daysOnSite': max,
                                 'supplierArticle': max,
                                 },
                        margins=False)

    df = df.reset_index().rename_axis(None, axis=1)
    df = df.rename(columns={'nmId': 'nm_id'})
    df.replace(np.NaN, 0, inplace=True)

    return df


def df_wb_stock_api(date_from: str = '2018-06-24T21:00:00.000Z'):
    """
    get wb stock via api put in df
    :return: df
    """
    t = time.process_time()
    date_from = date_from

    api_key = app.config['WB_API_TOKEN']
    path_start = "https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?"
    # print(time.process_time() - t)
    path_all = f"{path_start}dateFrom=2018-06-24T21:00:00.000Z&key={api_key}"
    response = requests.get(path_all)
    data = response.json()
    df = pd.DataFrame(data)
    return df


def get_wb_sales_realization_pivot2(df):
    df_pivot_sells_sum = df[df['supplier_oper_name'] == 'Продажа'].pivot_table(index=['nm_id'],
                                                                               values=['ppvz_for_pay'],
                                                                               aggfunc={'ppvz_for_pay': sum},
                                                                               margins=False)

    df_pivot_correct_sells_sum = df[df['supplier_oper_name'] == 'Продажа'].pivot_table(index=['nm_id'],
                                                                                       values=['ppvz_for_pay'],
                                                                                       aggfunc={'ppvz_for_pay': sum},
                                                                                       margins=False)

    df_pivot_returns_sells_sum = df[df['supplier_oper_name'] == 'Возврат'].pivot_table(
        index=['nm_id'],
        values=['ppvz_for_pay'],
        aggfunc={'ppvz_for_pay': sum},
        margins=False)

    df_pivot_correct_return_returns_sum = df[df['supplier_oper_name'] == 'Корректный возврат'].pivot_table(
        index=['nm_id'],
        values=['ppvz_for_pay'],
        aggfunc={'ppvz_for_pay': sum},
        margins=False)

    df_pivot_penalty_sum = df[df['supplier_oper_name'] == 'Штрафы'].pivot_table(
        index=['nm_id'],
        values=['penalty'],
        aggfunc={'penalty': sum},
        margins=False)

    df_pivot_logistic_sum = df[df['supplier_oper_name'] == 'Логистика'].pivot_table(index=['nm_id'],
                                                                                    values=['delivery_rub'],
                                                                                    aggfunc={'delivery_rub': sum},
                                                                                    margins=False)

    df_pivot_reversal_sales_sum = df[df['supplier_oper_name'] == 'Продажа'].pivot_table(index=['nm_id'],
                                                                                        values=['ppvz_for_pay'],
                                                                                        aggfunc={'ppvz_for_pay': sum},
                                                                                        margins=False)

    dfs = [df_pivot_sells_sum,
           df_pivot_correct_sells_sum,
           df_pivot_returns_sells_sum,
           df_pivot_correct_return_returns_sum,
           df_pivot_penalty_sum,
           df_pivot_logistic_sum,
           df_pivot_reversal_sales_sum, ]

    df = reduce(lambda left, right: pd.merge(left, right, on=['nm_id'],
                                             how='outer'), dfs)

    return df

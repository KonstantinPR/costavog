from app import app
from flask_login import current_user
from functools import reduce
import math
import requests
from app.models import Product, db
import datetime
import pandas as pd
import numpy as np
from app.modules import yandex_disk_handler, API_WB
from app.modules import io_output

VISIBLE_COL = [
    'brand_name',
    'Предмет',
    'nm_id',
    'Артикул WB',
    'supplierArticle',
    'Согласованная скидка, %',
    'discount',
    'Согл. скидк - disc',
    'price_disc',
    'Перечисление руб',
    'Прибыль_sum',
    'quantity',
    'Остаток в розничных ценах',
    'Логистика руб',
    'Логистика шт',
    'Логистика ед. средн',
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
    'k_rating',
    'Номенклатура (код 1С)',
    'Рейтинг',
    'Кол-во отзывов',
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
    'quantity',
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
# Задержка чтобы не брать количество дней в конце периода
# на которые возможно еще не существует данных (зависит от API)
DAYS_DELAY_REPORT = 2
DATE_PARTS = 3
K_SMOOTH = 1
MIN_DAYS_ON_SITE_TO_ANALIZE = 28


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
    df.to_excel('key_indocators_begin.xlsx')
    df['market_cost'] = df['price_disc'] * df['quantity']
    key_indicators['market_cost'] = df['market_cost'].sum()
    key_indicators['Перечисление руб'] = df['Перечисление руб'].sum()
    key_indicators['retail_price_Пр_Взвр'] = df['retail_price_withdisc_rub_Продажа_sum'].sum() - df[
        'retail_price_withdisc_rub_Возврат_sum'].sum()
    key_indicators['comission_and_exp_all'] = 1 - (
            key_indicators['Перечисление руб'] / key_indicators['retail_price_Пр_Взвр'])
    print(f"comission_to_wb {key_indicators['comission_and_exp_all']}")
    key_indicators['our_income_for_all'] = key_indicators['market_cost'] * (1 - key_indicators['comission_and_exp_all'])

    key_indicators['net_cost_med'] = (df[df["net_cost"] != 0]["net_cost"] * df[df["net_cost"] != 0][
        "quantity"]).sum() / df[df["net_cost"] != 0]["quantity"].sum()

    df['net_cost'] = np.where(df.net_cost == 0, key_indicators['net_cost_med'], df.net_cost)

    df['nets_cost'] = df['net_cost'] * df['quantity']
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
            df['quantity'],
            )]

    key_indicators['revenue_potential_cost'] = df['revenue_potential_cost'].sum()

    for k, v in key_indicators.items():
        if not 'revenue_net_dif_med' in k:
            print(f'{k} {key_indicators[k]}')
            if key_indicators[k] > 1000:
                key_indicators[k] = int(v)

    df = df.from_dict(key_indicators, orient='index', columns=['key_indicator'])

    return df


def divide_handle(rub, sht):
    try:
        result = rub / sht
    except ZeroDivisionError:
        result = 0
        print("Error: Division by zero occurred")
    except Exception as e:
        result = 0
        print("Error: An exception occurred during the division:", e)
    return result


def df_forming_goal_column(df, df_revenue_col_name_list, k_smooth):
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
    df['Логистика ед. средн'] = [divide_handle(rub, sht) for rub, sht in zip(df['Логистика руб'], df['Логистика шт'])]
    df['price_disc'] = df['price'] * (1 - df['discount'] / 100)

    df['Продажи_уч_возврат_sum'] = df['quantity_Продажа_sum'] - df['quantity_Возврат_sum']

    df['Перечисление руб'] = df[[col for col in df.columns if "ppvz_for_pay_Продажа_sum" in col]].sum(axis=1) - \
                             df[[col for col in df.columns if "ppvz_for_pay_Возврат_sum" in col]].sum(axis=1) - \
                             df[[col for col in df.columns if "delivery_rub_Логистика_sum" in col]].sum(axis=1)
    # Принятие решения о скидке на основе сформированных данных ---
    # коэффициент влияния на скидку
    df['k_discount'] = 1
    # если не было продаж и текущая цена выше себестоимости, то увеличиваем скидку (коэффициент)

    df = get_k_discount(df, df_revenue_col_name_list)

    df['Согласованная скидка, %'] = round((df['discount'] - (1 - df['k_discount']) * 100) * df['k_discount'], 4)

    df['Согласованная скидка, %'] = [3 if 1 <= x < 3 else x for x in df['Согласованная скидка, %']]

    df['Согласованная скидка, %'] = round(df['Согласованная скидка, %'] + \
                                          (df['Согласованная скидка, %'] - df['discount']) / k_smooth, 0)



    df['Согл. скидк - disc'] = df['Согласованная скидка, %'] - df['discount']
    df['Остаток в розничных ценах'] = df['price_disc'] * df['quantity']
    # df['Согласованная скидка, %'] = round(df['discount'] + (df['k_discount'] / (1 - df['discount'] / 100)), 0)
    df['Номенклатура (код 1С)'] = df['nm_id']
    df['supplierArticle'] = np.where(df['supplierArticle'] is None, df['article'], df['supplierArticle'])
    return df


def request_date_from(request):
    if request.form.get('date_from'):
        date_from = request.form.get('date_from')
    else:
        date_from = datetime.datetime.today() - datetime.timedelta(
            days=app.config['DAYS_STEP_DEFAULT']) - datetime.timedelta(DAYS_DELAY_REPORT)
        date_from = date_from.strftime(DATE_FORMAT)
    return date_from


def request_date_end(request):
    if request.form.get('date_end'):
        date_end = request.form.get('date_end')
    else:
        date_end = datetime.datetime.today() - datetime.timedelta(DAYS_DELAY_REPORT)
        date_end = date_end.strftime(DATE_FORMAT)
        # date_end = time.strftime(date_format)- datetime.timedelta(3)
    return date_end


def request_days_step(request):
    if request.form.get('days_step'):
        days_step = request.form.get('days_step')
    else:
        days_step = app.config['DAYS_STEP_DEFAULT']
    return days_step


def request_date_parts(request):
    if request.form.get('part_by'):
        date_parts = request.form.get('part_by')
    else:
        date_parts = DATE_PARTS
    return date_parts


def request_k_smooth(request):
    if request.form.get('k_smooth'):
        k_smooth = int(request.form.get('k_smooth'))
    else:
        k_smooth = K_SMOOTH
    return k_smooth


def revenue_processing_module(request):
    """forming via wb api table dynamic revenue and correcting discount"""
    # --- REQUEST PROCESSING ---

    date_from = request_date_from(request)
    date_end = request_date_end(request)
    days_step = request_days_step(request)
    date_parts = request_date_parts(request)
    k_smooth = request_k_smooth(request)

    # --- GET DATA VIA WB API /// ---

    df_sales = API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
    # df_sales.to_excel('df_sales_excel.xlsx')
    # df_sales = pd.read_excel("df_sales_excel.xlsx")

    df_stock = API_WB.get_wb_stock_api_extanded()
    # df_sales.to_excel('wb_stock.xlsx')
    # df_stock = pd.read_excel("wb_stock.xlsx")

    # --- GET DATA FROM YADISK /// ---
    df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
    df_rating = yandex_disk_handler.get_excel_file_from_ydisk(app.config['RATING'])
    # df_rating.to_excel('df_rating.xlsx')
    df_sales_pivot = get_wb_sales_realization_pivot(df_sales)
    df_sales_pivot.to_excel('sales_pivot.xlsx')
    # таблица с итоговыми значениями с префиксом _sum
    df_sales_pivot.columns = [f'{x}_sum' for x in df_sales_pivot.columns]
    days_bunch = get_days_bunch_from_delta_date(date_from, date_end, date_parts, DATE_FORMAT)
    period_dates_list = get_period_dates_list(date_from, date_end, days_bunch, date_parts)
    df_sales_list = dataframe_divide(df_sales, period_dates_list, date_from)

    df_pivot_list = [get_wb_sales_realization_pivot(d) for d in df_sales_list]

    df = df_pivot_list[0]
    date = iter(period_dates_list[1:])
    df_p = df_pivot_list[1:]
    for d in df_p:
        df_pivot = df.merge(d, how="outer", on='nm_id', suffixes=(None, f'_{str(next(date))[:10]}'))
        df = df_pivot

    df_price = get_wb_price_api()
    df = df.merge(df_price, how='outer', on='nm_id')
    df = df.merge(df_rating, how='outer', left_on='nm_id', right_on="Артикул")

    df_complete = df.merge(df_stock, how='outer', on='nm_id')
    df = df_complete.merge(df_net_cost, how='outer', left_on='nm_id', right_on='nm_id')
    df.to_excel('df_complete.xlsx')
    df = get_revenue_by_part(df, period_dates_list)

    df = df.rename(columns={'Прибыль': f"Прибыль_{str(period_dates_list[0])[:10]}"})
    df_revenue_col_name_list = df_revenue_column_name_list(df)

    df.fillna(0, inplace=True, downcast='infer')
    # чтобы были видны итоговые значения из первоначальной таблицы с продажами
    df = df.merge(df_sales_pivot, how='outer', on='nm_id')

    df = df_forming_goal_column(df, df_revenue_col_name_list, k_smooth)

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
    df['Артикул WB'] = df['nm_id']
    df = df_stay_column_not_null(df)
    df = combine_duplicate_column(df, "Предмет", ["subject_name"])

    df = df[VISIBLE_COL + [col for col in df.columns if col not in VISIBLE_COL]]
    df = df.sort_values(by='Прибыль_sum')

    file_name_specific = f"wb_dynamic_revenue_report_to_{str(date_end)}_from_{str(date_from)}.xlsx"
    file_name_common = f"wb_dynamic_revenue_report.xlsx"
    file_content = io_output.io_output(df)
    # добавляем полученный файл на яндекс.диск
    yandex_disk_handler.upload_to_YandexDisk(file_content, file_name_specific)
    yandex_disk_handler.upload_to_YandexDisk(file_content, file_name_common, app_config_path=app.config['YANDEX_PATH'])

    return df, file_name_specific


def combine_duplicate_column(df, col_name_in: str, list_re_col_names: list):
    """insert in values of col_name_in dataframe column values from list_re_col_name if 0"""
    for col_name_from in list_re_col_names:
        df[col_name_in] = [_insert_missing_values(val_col_in, val_col_from) for
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
    '''v 1.0'''
    if not net_cost: net_cost = DEFAULT_NET_COST
    k_net_cost = math.sqrt(DEFAULT_NET_COST / net_cost)

    if sell_sum > 100 * k_net_cost:
        return 0.94
    if sell_sum > 50 * k_net_cost:
        return 0.95
    if sell_sum > 20 * k_net_cost:
        return 0.96
    if sell_sum > 10 * k_net_cost:
        return 0.97
    if sell_sum > 5 * k_net_cost:
        return 0.98
    if sell_sum > 3 * k_net_cost:
        return 0.99
    if sell_sum > 1 * k_net_cost:
        return 1

    return 1.01


def k_qt_full(qt):
    k = 1
    if qt <= 3:
        k = 0.98
    if qt <= 5:
        k = 0.99
    if 10 < qt <= 50:
        k = 1.01
    if 50 < qt <= 100:
        k = 1.03
    if qt > 100:
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
        return 0.95
    if price_disc <= net_cost * k_net_cost:
        return 0.96
    if price_disc <= net_cost * 1.1 * k_net_cost:
        return 0.97
    if price_disc <= net_cost * 1.3 * k_net_cost:
        return 0.98
    if price_disc <= net_cost * 1.4 * k_net_cost:
        return 0.99
    if price_disc >= net_cost * 4 * k_net_cost:
        return 1.04
    if price_disc >= net_cost * 3 * k_net_cost:
        return 1.03
    if price_disc >= net_cost * 2 * k_net_cost:
        return 1.02
    if price_disc > net_cost * k_net_cost:
        return 1.01
    if price_disc == 0:
        return 1

    return 1


def k_rating(rating, qt_rating):
    if rating == 5:
        return 0.98
    if rating == 4:
        return 0.99
    if rating == 3:
        return 1
    if rating == 2:
        return 1.01
    if rating == 1:
        return 1.02
    return 1


def get_k_discount(df, df_revenue_col_name_list):
    # если не было продаж увеличиваем скидку
    df['k_is_sell'] = [k_is_sell(x, y) for x, y in zip(df['Продажи_уч_возврат_sum'], df['net_cost'])]
    # постоянно растет или падает прибыль, отрицательная или положительная
    df['k_revenue'] = [k_revenue(w, x, y, z) for w, x, y, z in
                       zip(df['quantity_Продажа_sum'], df['Прибыль_sum'], df['Прибыль_mean'], df['Прибыль_last'])]
    # Защита от покатушек - поднимаем цену
    df['k_logistic'] = [k_logistic(w, x, y, z) for w, x, y, z in
                        zip(df['Логистика руб'], df['ppvz_for_pay_Продажа_sum'], df['ppvz_for_pay_Возврат_sum'],
                            df['net_cost'])]
    # Защита от цены ниже себестоимости - тогда повышаем
    df['k_net_cost'] = [k_net_cost(x, y) for x, y in zip(df['net_cost'], df['price_disc'])]
    df['k_qt_full'] = [k_qt_full(x) for x in df['quantity']]
    df['k_rating'] = [k_rating(x, y) for x, y in zip(df['Рейтинг'], df['Кол-во отзывов'])]
    # df['k_discount'] = (df['k_is_sell'] + df['k_revenue'] + df['k_logistic'] + df['k_net_cost'] + df[
    #     'k_qt_full']) / 5
    df['k_discount'] = 1

    # IMPORTANT !!! days on site was delete some 12/07/2023
    # df.loc[(df['daysOnSite'] > MIN_DAYS_ON_SITE_TO_ANALIZE) & (df['quantity'] > 0), 'k_discount'] = \
    #     (df['k_is_sell'] + df['k_revenue'] + df['k_logistic'] + df['k_net_cost'] + df['k_qt_full'] + df['k_rating']) / 6

    weight_dict = {}
    weight_dict['k_is_sell'] = 1.3
    weight_dict['k_revenue'] = 1
    weight_dict['k_logistic'] = 1
    weight_dict['k_net_cost'] = 1.2
    weight_dict['k_qt_full'] = 1
    weight_dict['k_rating'] = 1

    weighted_sum = (
            df['k_is_sell'] * weight_dict['k_is_sell'] +
            df['k_revenue'] * weight_dict['k_revenue'] +
            df['k_logistic'] * weight_dict['k_logistic'] +
            df['k_net_cost'] * weight_dict['k_net_cost'] +
            df['k_qt_full'] * weight_dict['k_qt_full'] +
            df['k_rating'] * weight_dict['k_rating']
        # Add other coefficients here with their respective weights
    )

    # Calculate the total weight as the sum of individual weights
    total_weight = sum(weight_dict.values())

    # Update the 'k_discount' column based on the weighted sum and total weight
    df.loc[(df['quantity'] > 0) | (df['Прибыль_sum'] != 0), 'k_discount'] = weighted_sum / total_weight

    return df




def df_revenue_column_name_list(df):
    df_revenue_col_name_list = [col for col in df.columns if f'Прибыль_' in col]
    return df_revenue_col_name_list


# --- NEW COLUMN ON REVENUE ANILIZE /// ---

def dataframe_divide(df, period_dates_list, date_from, date_format="%Y-%m-%d"):
    df['rr_dt'] = [x[0:10] + " 00:00:00" for x in df['rr_dt']]
    df['rr_dt'] = pd.to_datetime(df['rr_dt'])
    # df = df.set_index(df['rr_dt'])
    # df = df.sort_index()
    # print(df)

    if isinstance(date_from, str):
        date_from = datetime.datetime.strptime(date_from, date_format)

    df_list = []

    for date_end in period_dates_list:
        print(f"from df date {date_from}")
        print(f"end df date {date_end}")

        # df = df[date_from:date_end]

        d = df[(df['rr_dt'] > date_from) & (df['rr_dt'] <= date_end)]
        # print(f"d {d}")
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
        'quantity',
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
    df.to_excel("wb_sales_realization_pivot_begin.xlsx")
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


def get_wb_price_api(g=None):
    headers = {
        'accept': 'application/json',
        # 'Authorization': app.config['WB_API_TOKEN2'],
        'Authorization': app.config['WB_API_TOKEN'],
    }

    response = requests.get('https://suppliers-api.wildberries.ru/public/api/v1/info', headers=headers)
    data = response.json()
    df = pd.DataFrame(data)
    df = df.rename(columns={'nmId': 'nm_id'})
    return df

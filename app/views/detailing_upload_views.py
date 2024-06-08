import logging
from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import io_output, yandex_disk_handler, pandas_handler, detailing_upload_module, price_module, API_WB
from app.modules import sales_funnel_module
from app.modules import detailing_api_module
import numpy as np

from decimal import Decimal


@app.route('/upload_detailing', methods=['POST', 'GET'])
@login_required
def upload_detailing():
    """Analize detailing of excel that can be downloaded in wb portal in zips, you can put any number zips"""

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if not request.method == 'POST':
        return render_template('upload_detailing.html', doc_string=upload_detailing.__doc__)

    yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                    path_or_config=app.config['REPORT_DETAILING_UPLOAD'])

    days_by = int(request.form.get('days_by'))
    if not days_by: days_by = int(app.config['DAYS_PERIOD_DEFAULT'])
    print(f"days_by {days_by}")
    uploaded_files = request.files.getlist("file")
    testing_mode = request.form.get('is_testing_mode')
    is_net_cost = request.form.get('is_net_cost')
    is_get_storage = request.form.get('is_get_storage')
    change_discount = request.form.get('change_discount')
    is_just_concatenate = request.form.get('is_just_concatenate')
    is_delete_shushary = request.form.get('is_delete_shushary')
    is_get_price = request.form.get('is_get_price')
    is_get_stock = request.form.get('is_get_stock')
    is_funnel = request.form.get('is_funnel')
    k_delta = int(request.form.get('k_delta'))
    is_mix_discounts = request.form.get('is_mix_discounts')
    if not k_delta: k_delta = 1

    INCLUDE_COLUMNS = list(detailing_upload_module.INITIAL_COLUMNS_DICT.values())
    default_amount_days = 14

    if not uploaded_files:
        flash("Вы ничего не выбрали. Необходим zip архив с zip архивами, скаченными с сайта wb раздела детализаций")
        return render_template('upload_detailing.html')

    uploaded_file = detailing_upload_module.process_uploaded_files(uploaded_files)

    # print(df_net_cost)
    df_list = detailing_upload_module.zips_to_list(uploaded_file)
    concatenated_dfs = pd.concat(df_list)

    concatenated_dfs = pandas_handler.upper_case(concatenated_dfs, 'Артикул поставщика')

    # Check if concatenate parameter is passed
    if is_just_concatenate == 'is_just_concatenate':
        return send_file(io_output.io_output(concatenated_dfs), download_name=f'concat.xlsx', as_attachment=True)

    concatenated_dfs = detailing_upload_module.replace_incorrect_date(concatenated_dfs)

    date_min = concatenated_dfs["Дата продажи"].min()
    print(f"date_min {date_min}")
    # concatenated_dfs.to_excel('ex.xlsx')
    date_max = concatenated_dfs["Дата продажи"].max()

    dfs_sales, INCLUDE_COLUMNS = detailing_upload_module.get_dynamic_sales(concatenated_dfs, days_by, INCLUDE_COLUMNS)

    # concatenated_dfs = detailing.get_dynamic_sales(concatenated_dfs)

    storage_cost = detailing_upload_module.get_storage_cost(concatenated_dfs)

    df = detailing_upload_module.zip_detail_V2(concatenated_dfs)

    df = detailing_upload_module.merge_stock(df, testing_mode=testing_mode, is_get_stock=is_get_stock,
                                             is_delete_shushary=is_delete_shushary)

    if not 'quantityFull' in df.columns: df['quantityFull'] = 0
    df['quantityFull'].replace(np.NaN, 0, inplace=True)
    df['quantityFull + Продажа, шт.'] = df['quantityFull'] + df['Продажа, шт.']

    df = detailing_upload_module.merge_storage(df, storage_cost, testing_mode, is_get_storage=is_get_storage,
                                               is_delete_shushary=is_delete_shushary)
    df = detailing_upload_module.merge_net_cost(df, is_net_cost)
    df = detailing_upload_module.merge_price(df, testing_mode, is_get_price).drop_duplicates(subset='nmID')
    df = detailing_upload_module.profit_count(df)
    df_rating = yandex_disk_handler.get_excel_file_from_ydisk(app.config['RATING'])
    df = df.merge(df_rating, how='outer', left_on='nmId', right_on="Артикул")

    df = pandas_handler.fill_empty_val_by(['article', 'vendorCode', 'supplierArticle'], df, 'Артикул поставщика')
    df = pandas_handler.fill_empty_val_by(['brand'], df, 'Бренд')
    df = df.rename(columns={'Предмет_x': 'Предмет'})
    df = pandas_handler.fill_empty_val_by(['category'], df, 'Предмет')

    if dfs_sales:
        print(f"merging dfs_sales ...")
        for d in dfs_sales:
            df = df.merge(d, how='left', left_on='nmId', right_on='Код номенклатуры')

    # df.to_excel('dfs_sales.xlsx')
    # --- DICOUNT ---

    days_period = (date_max - date_min).days
    df['days_period'] = days_period
    df['smooth_days'] = df['days_period'] / default_amount_days
    print(f"smooth_days {df['smooth_days'].mean()}")
    df = price_module.discount(df, k_delta=k_delta)
    discount_columns = sales_funnel_module.DISCOUNT_COLUMNS
    discount_columns['buyoutsCount'] = 'Ч. Продажа шт.'
    df = sales_funnel_module.calculate_discount(df, discount_columns=discount_columns)
    df = price_module.mix_discounts(df, is_mix_discounts)

    df = price_module.k_dynamic(df, days_by=days_by)
    # 27/04/2024 - not yet prepared
    # df[discount_columns['func_discount']] *= df['k_dynamic']

    # Reorder the columns

    #  --- PATTERN SPLITTING ---
    df = df[~df['nmId'].isin(pandas_handler.FALSE_LIST)]
    df['prefix'] = df['Артикул поставщика'].astype(str).apply(lambda x: x.split("-")[0])
    prefixes_dict = detailing_upload_module.PREFIXES_ART_DICT
    prefixes = list(prefixes_dict.keys())
    df['prefix'] = df['prefix'].apply(lambda x: starts_with_prefix(x, prefixes))
    df['prefix'] = df['prefix'].apply(lambda x: prefixes_dict.get(x, x))
    df['pattern'] = df['Артикул поставщика'].apply(get_second_part)
    df['material'] = df['Артикул поставщика'].apply(get_third_part)
    MATERIAL_DICT = detailing_upload_module.MATERIAL_DICT
    df['material'] = [MATERIAL_DICT[x] if x in MATERIAL_DICT else y for x, y in zip(df['pattern'], df['material'])]

    # print(INCLUDE_COLUMNS)
    include_column = [col for col in INCLUDE_COLUMNS if col in df.columns]
    df = df[include_column + [col for col in df.columns if col not in INCLUDE_COLUMNS]]
    df = pandas_handler.round_df_if(df, half=10)

    if is_funnel:
        df_funnel, file_name = API_WB.get_wb_sales_funnel_api(request, testing_mode=testing_mode)
        df = df.merge(df_funnel, how='outer', left_on='nmId', right_on="nmID")

    file_name = "report_detailing_upload.xlsx"
    file = io_output.io_output(df)
    yandex_disk_handler.upload_to_YandexDisk(file, file_name=file_name, path=app.config['REPORT_DETAILING_UPLOAD'])
    file = io_output.io_output(df)

    flash("Отчет успешно выгружен в excel файл")
    return send_file(file, download_name=f'report_detailing_{str(date_max)}_to_{str(date_min)}.xlsx',
                     as_attachment=True)


def starts_with_prefix(string, prefixes):
    for prefix in prefixes:
        if string.startswith(prefix):
            if len(string) > 10:
                return ''
            return prefix  # Return the prefix itself, not prefixes[prefix]
    return string


def get_second_part(x):
    try:
        return str(x).split("-")[1]
    except IndexError:
        # If the string doesn't contain the delimiter '-', return None or any other value as needed
        return ''


def get_third_part(x):
    try:
        return str(x).split("-")[2]
    except IndexError:
        # If the string doesn't contain the delimiter '-', return None or any other value as needed
        return ''

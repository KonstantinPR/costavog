from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import io_output, yandex_disk_handler, pandas_handler, detailing_upload_module, price_module
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

    days_by = request.form.get('days_by')
    if not days_by: days_by = int(app.config['DAYS_PERIOD_DEFAULT'])
    print(f"days_by {days_by}")
    uploaded_files = request.files.getlist("file")
    testing_mode = request.form.get('is_testing_mode') == 'on'
    is_net_cost = request.form.get('is_net_cost')
    is_get_storage = request.form.get('is_get_storage')
    change_discount = request.form.get('change_discount')
    is_just_concatenate = request.form.get('is_just_concatenate')
    is_delete_shushary = request.form.get('is_delete_shushary')
    is_get_price = request.form.get('is_get_price')
    is_get_stock = request.form.get('is_get_stock')

    INCLUDE_COLUMNS = list(detailing_upload_module.INITIAL_COLUMNS_DICT.values())

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
    concatenated_dfs.to_excel('ex.xlsx')
    date_max = concatenated_dfs["Дата продажи"].max()

    dfs_sales, INCLUDE_COLUMNS = detailing_upload_module.get_dynamic_sales(concatenated_dfs, days_by,
                                                                           INCLUDE_COLUMNS)

    # concatenated_dfs = detailing.get_dynamic_sales(concatenated_dfs)

    storage_cost = detailing_upload_module.get_storage_cost(concatenated_dfs)

    df = detailing_upload_module.zip_detail_V2(concatenated_dfs)

    df = detailing_upload_module.merge_stock(df, testing_mode=testing_mode, is_get_stock=is_get_stock)

    if not 'quantityFull' in df.columns: df['quantityFull'] = 0
    df['quantityFull'].replace(np.NaN, 0, inplace=True)
    df['quantityFull + Продажа, шт.'] = df['quantityFull'] + df['Продажа, шт.']

    df = detailing_upload_module.merge_storage(df, storage_cost, testing_mode, is_get_storage=is_get_storage)
    df = detailing_upload_module.merge_net_cost(df, is_net_cost)
    df = detailing_upload_module.merge_price(df, testing_mode, is_get_price)
    df = detailing_upload_module.profit_count(df)
    df_rating = yandex_disk_handler.get_excel_file_from_ydisk(app.config['RATING'])
    df = df.merge(df_rating, how='outer', left_on='nmId', right_on="Артикул")

    df = pandas_handler.fill_empty_val_by(['article', 'vendorCode', 'supplierArticle'], df, 'Артикул поставщика')
    df = pandas_handler.fill_empty_val_by(['brand'], df, 'Бренд')
    df = df.rename(columns={'Предмет_x': 'Предмет'})
    df = pandas_handler.fill_empty_val_by(['category'], df, 'Предмет')

    if dfs_sales:
        [df.merge(d, how='left', left_on='nmId', right_on='Код номенклатуры') for d in dfs_sales]

    df = price_module.discount(df)

    # Reorder the columns
    df = df[[col for col in INCLUDE_COLUMNS if col in df.columns]]
    df = df[~df['nmId'].isin(pandas_handler.FALSE_LIST)]

    # print(INCLUDE_COLUMNS)
    file = io_output.io_output(df)
    flash("Отчет успешно выгружен в excel файл")
    return send_file(file, download_name=f'report_detailing_{str(date_max)}_to_{str(date_min)}.xlsx',
                     as_attachment=True)

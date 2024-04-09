from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import io_output, yandex_disk_handler, pandas_handler, detailing_upload_module, API_WB
import numpy as np


@app.route('/upload_detailing', methods=['POST', 'GET'])
@login_required
def upload_detailing():
    """Processing detailing in excel that can be downloaded in wb portal in zip, put all zip in one zip and upload it,
    or can be chosen some and added to input as well"""
    testing_mode = True
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        uploaded_files = request.files.getlist("file")
        is_net_cost = request.form.get('is_net_cost')
        is_get_storage = request.form.get('is_get_storage')
        is_just_concatenate = request.form.get('is_just_concatenate')
        is_delete_shushary = request.form.get('is_delete_shushary')

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

        # concatenated_dfs = detailing.get_dynamic_sales(concatenated_dfs)

        if 'Хранение' in concatenated_dfs.columns:
            storage_cost = concatenated_dfs['Хранение'].sum()
        else:
            concatenated_dfs['Хранение'] = 0
            storage_cost = 0

        df = detailing_upload_module.zip_detail_V2(concatenated_dfs)

        date_min = df["Дата заказа покупателем"].min()
        date_max = df["Дата заказа покупателем"].max()

        is_get_stock = request.form.get('is_get_stock')

        if is_get_stock:
            request_dict = {'no_sizes': 'no_sizes', 'no_city': 'no_city'}
            df_stock = API_WB.get_wb_stock_api(testing_mode=testing_mode, request=request_dict,
                                               is_delete_shushary=is_delete_shushary)
            df_stock = pandas_handler.upper_case(df_stock, 'supplierArticle')
            df = df_stock.merge(df, how='outer', left_on='nmId', right_on='Код номенклатуры')
            df = pandas_handler.fill_empty_val_by('Код номенклатуры', df, 'nmId')

        if not 'quantity' in df.columns:
            df['quantity'] = 0
        df['quantity'].replace(np.NaN, 0, inplace=True)
        df['quantity + Продажа, шт.'] = df['quantity'] + df['Продажа, шт.']

        is_get_price = request.form.get('is_get_price')

        if is_get_storage:
            df_storage = API_WB.get_average_storage_cost(testing_mode=testing_mode,
                                                         is_delete_shushary=is_delete_shushary)
            df_storage = pandas_handler.upper_case(df_storage, 'vendorCode')
            df = df.merge(df_storage, how='outer', left_on='nmId', right_on='nmId')
            df = df.fillna(0)
            df['Хранение'] = storage_cost * df['shareCost']
            df['Хранение'] = df['Хранение'].fillna(0)
            df['Хранение.ед'] = df['Хранение'] / df['quantity + Продажа, шт.']

        if is_net_cost:
            df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
            df_net_cost['net_cost'].replace(np.NaN, 0, inplace=True)
            df_net_cost = pandas_handler.upper_case(df_net_cost, 'article')
            df = df.merge(df_net_cost, how='outer', left_on='nmId', right_on='nm_id')
            df['Маржа-себест.'] = df['Маржа'] - df['net_cost'] * df['Продажа, шт.']
        else:
            df_net_cost = False
        df = df.fillna(0)
        df['Маржа-себест.-хран.'] = df['Маржа-себест.'] - df['Хранение']
        df['Маржа-себест.-хран./ шт.'] = np.where(
            df['Продажа, шт.'] != 0,
            df['Маржа-себест.-хран.'] / df['Продажа, шт.'],
            np.where(
                df['quantity'] != 0,
                df['Маржа-себест.-хран.'] / df['quantity'],
                0
            )
        )
        # df.to_excel("detail_with_storage.xlsx")

        # nm_columns = ['nmId', 'nm_id']
        # df = pandas_handler.fill_empty_val_by(nm_columns, df, "Код номенклатуры")

        if is_get_price:
            df_price = API_WB.get_wb_price_api(testing_mode=testing_mode)
            df = df.merge(df_price, how='outer', left_on='nmId', right_on='nm_id')
            df['price_disc'] = df['price'] - df['price'] * df['discount'] / 100
            # df.to_excel("detail_with_price.xlsx")

        columns_to_check_existence = [
            'Бренд',
            'Предмет_x',
            'Артикул поставщика',
            'nmId',
            'volume',
            'quantity',
            'quantity + Продажа, шт.',
            'price_disc',
            'net_cost',
            'Дней в продаже',
            'Маржа-себест.-хран.',
            'Маржа-себест.',
            'Маржа',
            'Ч. Продажа шт.',
            # 'Продажа_1',
            # 'Продажа_2',
            'Продажа',
            'Возврат',
            'Возврат, шт.',
            'Хранение',
            'shareCost',
            'Хранение.ед',
            'Логистика',
            'Логистика. ед',
            'Маржа-себест.-хран./ шт.',
            'Логистика шт.',
            'Поставщик',
            'Дата заказа покупателем',
            'Дата продажи',
        ]

        nm_columns = ['article', 'vendorCode', 'supplierArticle']
        df = pandas_handler.fill_empty_val_by(nm_columns, df, 'Артикул поставщика')

        # Reorder the columns
        df = df[[col for col in columns_to_check_existence if col in df.columns]]

        if 'Артикул поставщика' in df.columns:
            df.dropna(subset=["Артикул поставщика"], inplace=True)

        file = io_output.io_output(df)
        flash("Отчет успешно выгружен в excel файл")
        return send_file(file, download_name=f'report_detailing_{str(date_max)}_to_{str(date_min)}.xlsx',
                         as_attachment=True)

    return render_template('upload_detailing.html', doc_string=upload_detailing.__doc__)

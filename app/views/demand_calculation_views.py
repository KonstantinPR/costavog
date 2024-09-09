import datetime

from flask_login import login_required
from werkzeug.datastructures import FileStorage
import logging
from app import app
from flask import render_template, request, send_file, flash
from app.modules import io_output, demand_calculation_module, request_handler, pandas_handler
import pandas as pd


@app.route('/demand_calculation_excel', methods=['POST', 'GET'])
@login_required
def demand_calculation_excel():
    """
    Excel с потребностями:
    Присылает excel с потребностями.
    Напечатает те артикулы, которые передали в txt с шапкой vendorCode.
    В строке пишем через пробел подстроки, которые ищем в артикуле, которые хотим вывести в excel.
    Если передали файл - то приоритет у файла, строка работать не будет. Если файла нет - работает строка.
    """

    if request.method == 'POST':
        df = request_handler.to_df(request)
        search_string = str(request.form['search_string'])
        min_stock = int(request.form['min_stock'])
        testing_mode = request.form.get('testing_mode')
        is_from_yadisk = request.form.get('is_from_yadisk')
        qt_correct = request.form.get('qt_correct')

        df = demand_calculation_module.demand_calculation_to_df(df, search_string, min_stock=min_stock,
                                                                testing_mode=testing_mode,
                                                                is_from_yadisk=is_from_yadisk)
        df_clear = demand_calculation_module.clear_demand(df, qt_correct=qt_correct)

        file_name = f"demand_calculation_{str(datetime.date.today())}.xlsx"
        clear_name = f"clear_demand_{str(datetime.date.today())}.xlsx"
        dfs = [df, df_clear]
        names = [file_name, clear_name]

        zip_files, name_files = pandas_handler.files_to_zip(list_files=dfs, list_names=names)

        # df.to_excel("df_output.xlsx")

        return send_file(zip_files, download_name=name_files, as_attachment=False)
    return render_template('upload_demand_calculation_with_image_catalog.html',
                           doc_string=demand_calculation_excel.__doc__)


@app.route('/demand_calculation_with_image_catalog', methods=['POST', 'GET'])
@login_required
def demand_calculation_with_image_catalog():
    """
    Каталог PDF с потребностями:
    Делает PDF каталог с потребностями. С фото артикула и другой информацией.
    Работает на локальном яндекс.диске.
    Напечатает те артикулы, которые передали в txt с шапкой vendorCode, techSize, Кол-во  или через копипасту.
    Если не указаны размеры - тогда по 1й, если не указаны размеры - то скачается через API анализ какие размеры нужны
    (долго).
    В строке пишем через пробел подстроки, которые ищем в артикуле, которые хотим вывести в каталог.
    Если передали файл - то приоритет у файла, строка работать не будет. Если файла нет - работает строка.
    """

    if request.method == 'POST':

        df = request_handler.to_df(request)
        file_name = request_handler.file_name_from_request(request)
        search_string = str(request.form['search_string'])
        search_string_list = search_string.split()

        if not search_string_list and df.empty:
            flash("ОШИБКА. Прикрепите файл или заполните инпут строку !")
            return render_template('upload_demand_calculation_with_image_catalog.html',
                                   doc_string=demand_calculation_with_image_catalog.__doc__)

        if not all(col in df for col in ['vendorCode', 'techSize', 'Кол-во']):
            df = demand_calculation_module.demand_calculation_to_df(df, search_string)

        pdf = demand_calculation_module.demand_calculation_df_to_pdf(df, file_name=file_name)

        return send_file(pdf, as_attachment=True)

    return render_template('upload_demand_calculation_with_image_catalog.html',
                           doc_string=demand_calculation_with_image_catalog.__doc__)

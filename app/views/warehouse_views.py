import logging
from app import app
from flask import render_template, request, send_file, flash
from app.modules import warehouse_module, io_output, data_transforming_module
import pandas as pd
from flask_login import login_required
import datetime


@app.route('/arrivals_of_products', methods=['POST', 'GET'])
@login_required
def arrivals_of_products():
    """
    Вытягивает все транзакции с указанных путей.
    На выбор доп. опции: с рекурсией - захватит все вложенные файлы
    Чекбокс - для вертикализирования размеров. Т.е из строки размеров, например 40, 42, 44, написанных в строку
    получим запись, где каждый размер с переносом строки.
    """

    if request.method == 'POST':
        # create an empty list to store the DataFrames of the Excel files
        path = app.config["FULL_PATH_ARRIVALS"]
        print(path)
        list_paths_files = warehouse_module.get_list_paths_files(path, file_names=["Приход"])
        df = warehouse_module.df_from_list_paths_excel_files(list_paths_files)

        if not df:
            flash("DataFrame пустой, возможно неверно настроены пути или папки не существуют")
            return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)

        df = pd.concat(df)

        if 'checkbox_is_vertical' in request.form:
            df = data_transforming_module.vertical_size(df)

        df_output = io_output.io_output(df)
        file_name = f"arrivals_of_products_on_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx"
        return send_file(df_output, as_attachment=True, download_name=file_name)

    return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)

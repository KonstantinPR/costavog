# /// CATALOG ////////////
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
import pandas as pd
from app.modules import text_handler, io_output
import requests



def is_url_image(arts):
    art_paths = []
    for a in arts:
        ext = 'jpg'
        path_img = f'https://elenachezelle.ru/img-catalog/{a}-1.{ext}'
        r = requests.head(path_img)
        if not r.status_code == 200:
            ext = 'JPG'
        art_paths.append(path_img)

    print(art_paths)
    return art_paths


@app.route('/catalog', methods=['GET', 'POST'])
def catalog():
    """Обработка файла excel  - шапка нужна, Номенклатура, Характеристика, Кол-во"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_input_order = pd.read_excel(uploaded_files[0])
        df_input_order.rename(columns={'Артикул поставщика': 'Номенклатура',
                                       'Размер': 'Характеристика',
                                       'Количество': 'Кол-во',
                                       }, inplace=True)
        arts = df_input_order["Номенклатура"].tolist()
        size = df_input_order["Характеристика"].tolist()
        qt = df_input_order["Кол-во"].tolist()

        # art_paths = is_url_image(arts)

        return render_template('catalog.html', art=arts, size=size, qt=qt, tables=[
            df_input_order.to_html(classes='table table-bordered', header="true", index=False)])

    return render_template("upload_catalog.html")

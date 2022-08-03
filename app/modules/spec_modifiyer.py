from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
from app.models import Product, db
import datetime
import pandas as pd
from app.modules import detailing, detailing_reports, yandex_disk_handler, decorators
from app.modules import io_output
import time
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
from app.modules import text_handler, io_output
import numpy as np
from flask_login import login_required, current_user, login_user, logout_user
from dataclasses import dataclass

col_names = [

    'Номер карточки',
    'Категория товара',
    'Бренд',
    'Артикул поставщика',
    'Артикул цвета',
    'Пол',
    'Размер',
    'Рос. размер',
    'Штрихкод товара',
    'Розничная цена',
    'Состав',
    'Комплектация',
    'Фото',
    'Страна производства',
    'Тнвэд',
    'Основной цвет',
    'Доп. цвета',
    'Ключевые слова',
    'Описание',
    'Вес (г)',
    'Вид застежки',
    'Вид каблука',
    'Вид мыска',
    'Высота голенища',
    'Высота каблука',
    'Высота обуви',
    'Высота платформы',
    'Высота подошвы',
    'Декоративные элементы',
    'Коллекция',
    'Любимые герои',
    'Материал подкладки обуви',
    'Материал подошвы обуви',
    'Материал стельки',
    'Модель балеток',
    'Модель босоножек/сандалий',
    'Модель ботинок',
    'Модель туфель',
    'Назначение обуви',
    'Наличие мембраны',
    'Обхват голенища',
    'Оптимальная скорость спортсмена',
    'Оптимальный вес спортсмена',
    'Ортопедия',
    'Особенности модели',
    'Перепад с пятки на носок',
    'Полнота обуви (EUR)',
    'Рисунок',
    'Сезон',
    'Стилистика',
    'Тип покрытия',
    'Тип пронации',

]

heel_shape = {

    'ки': 'кирпичик',
    'ст': 'столбик',
    'ко': 'конусовидный',
    'шп': 'шпилька',
    'та': 'танкетка',

}


@decorators.flask_request_to_df
def data_transcript(flask_request) -> pd.DataFrame:
    df = flask_request
    for col_name in col_names:
        col_names_spec = [col for col in df.columns if col_name in col]

    print(df.columns)
    print(col_names_spec)

    for col_name_spec in col_names_spec:
        df[col_name_spec] = df[col_name_spec].replace(heel_shape)

    return df

import flask
import pandas as pd
from app.modules import decorators
import numpy as np
from random import randrange
from flask import flash, render_template

from app.modules.spec_modifiyer import BEST_SIZES


def str_input_to_full_str(df, request: flask.Request, size_col_name: str, input_name: str, html_template: str):
    """
    Преобразует строку типа 40 56 2, где 40 начало, 56 конец, а 2 шаг в строку 40 42 44 46 ... 56

    :param df: Передаваемый Датафрейм DataFrame pandas с артикулами
    :param request: для извлечения из request input с сокращенной записью размера
    :param size_col_name: имя столбца с размерами в Датафрейме
    :param input_name: имя поля, в которе вводим сокращенную строку размера
    :param html_template: имя шаблона html, на каоторое перенаправляем в случае проблем
    :return df: Датафрейм df
    """
    if request.form[input_name] and not size_col_name in df:
        str_reduction_size = str(request.form[input_name])
        reduction_sizes_list = sizes_str_to_list(str_reduction_size)
        if len(reduction_sizes_list) <= 1:
            flash("Не указано с какого и до какого размера создаем список размеров")
            return render_template(html_template)
        df[size_col_name] = ""
        df = str_reduction_size_to_full(df, reduction_sizes_list, size_col_name)
    return df


def str_reduction_size_to_full(df, reduction_sizes_list: list, size_col_name="Размеры"):
    str_size_list_full = []

    size_from = int(reduction_sizes_list[0])
    size_to = int(reduction_sizes_list[1])
    if len(reduction_sizes_list) <= 2:
        size_step = 1
    else:
        size_step = int(reduction_sizes_list[2])

    for i in range(size_from, size_to + size_step, size_step):
        str_size_list_full.append(str(i))

    str_sizes_for_df = ' '.join(str_size_list_full)
    print(str_sizes_for_df)

    df[size_col_name] = [str_sizes_for_df for value in df[size_col_name]]

    return df


def vertical_size(df, col: str = 'Размеры', col_re='Размер'):
    if col in df.columns:
        re_size = [str(x).split(' ') if ' ' in str(x) else str(x) for x in df[col]]
        print(re_size)
        df = df.assign(temp_col=re_size).explode('temp_col', ignore_index=True)
        df = df.drop(columns=col, axis=1)
        df = df.rename({'temp_col': col_re}, axis='columns')
        print(df)

    # OLD RIGHT
    # lst_art = []
    # lst_sizes = [x.split() for x in df['Размеры']]
    # for i in range(len(lst_sizes)):
    #     for j in range(len(lst_sizes[i])):
    #         lst_art.append(df['Артикул полный'][i])
    #
    # lst_sizes = sum(lst_sizes, [])
    # df = pd.DataFrame({'Артикул полный': lst_art, 'Размеры': lst_sizes})

    return df


def to_keep_for_photo(df, size_col_name='Размер', art_col_name='Артикул товара', is_photo_col_name='На фото'):
    f"""На вход подается Data Frame с вертикальными размерами и столбцами 
    {art_col_name}, {size_col_name}, {is_photo_col_name}.
    На выходе получаем отмеченные в столбце {is_photo_col_name} как 1 размеры для фотосессии"""

    qt_col_name = 'Кол-во'
    arts = list(set(df.loc[df[is_photo_col_name] == 1, art_col_name]))

    for size in BEST_SIZES:
        for idx, value in enumerate(df[is_photo_col_name]):
            if value and df[art_col_name][idx] in arts and str(df[size_col_name][idx]) == str(size):
                arts.remove(df[art_col_name][idx])
                df[is_photo_col_name][idx] = 2

    df[is_photo_col_name] = ['' if x == 1 else x for x in df[is_photo_col_name]]
    df[is_photo_col_name] = df[is_photo_col_name].replace(2, 1)
    df[qt_col_name] = 1
    re_cols = [art_col_name, size_col_name, qt_col_name, is_photo_col_name]
    df = df[re_cols]

    return df


def sizes_str_to_list(sizes_str):
    str_reduction_size_list = sizes_str.split()
    print(str_reduction_size_list)
    return str_reduction_size_list

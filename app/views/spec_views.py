# /// CATALOG ////////////

import flask
from app import app
from flask import render_template, request, send_file
import pandas as pd
from app.modules import io_output, spec_modifiyer, yandex_disk_handler, df_worker, detailing_reports
from flask_login import login_required
import numpy as np


@app.route('/data_to_spec_wb_transcript', methods=['GET', 'POST'])
@login_required
def data_to_spec_wb_transcript():
    """
    Заполняется спецификация на основе справочников с яндекс.диска в таскере.
    На входе excel с шапкой: Артикул товара, Размеры (размерный ряд товара), Цена - не обящатеьльна
    """

    if request.method == 'POST':
        size_col_name = "Размеры"
        art_col_name = "Артикул товара"
        df_income_date = spec_modifiyer.request_to_df(flask.request)
        df_income_date = df_income_date[0]
        df_income_date = df_income_date.drop_duplicates(subset=art_col_name)
        df_income_date = df_income_date.reset_index(drop=True)
        # df_characters = yandex_disk_handler.get_excel_file_from_ydisk(app.config['CHARACTERS_PRODUCTS'])
        spec_type = spec_modifiyer.spec_definition(df_income_date)
        print(spec_type)
        df_spec_example = yandex_disk_handler.get_excel_file_from_ydisk(app.config[spec_type],
                                                                        to_str=['Лекало', 'Префикс'])
        # df_art_prefixes = yandex_disk_handler.get_excel_file_from_ydisk(app.config['ECO_FURS_WOMEN'])
        df_colors = yandex_disk_handler.get_excel_file_from_ydisk(app.config['COLORS'])
        df_income_date = spec_modifiyer.str_input_to_full_str(df_income_date, request, size_col_name,
                                                              input_name='size_forming',
                                                              html_template='upload_vertical_sizes.html')
        df_verticaling_sizes = spec_modifiyer.vertical_size(df_income_date)
        # df_check_exist_art = spec_modifier.check_art_existing(df_verticaling_sizes)
        # df_merge_spec = spec_modifiyer.merge_spec(df_verticaling_sizes, df_spec_example, 'Артикул товара')
        df_art_prefixes_adding = spec_modifiyer.picking_prefixes(df_verticaling_sizes, df_spec_example)
        df_colors_adding = spec_modifiyer.picking_colors(df_art_prefixes_adding, df_colors)
        df_pattern_merge = spec_modifiyer.merge_spec(df_colors_adding, df_spec_example, 'Лекало', 'Лекало')
        df_clear = spec_modifiyer.df_clear(df_pattern_merge)
        df_added_some_col = spec_modifiyer.col_adding(df_clear)
        df_to_str = spec_modifiyer.col_str(df_added_some_col, ['Баркод товара'])

        all_cards_wb = 'all_cards_wb.xlsx'
        df_all_card_on_wb = pd.read_excel(all_cards_wb)
        # df_all_card_on_wb = detailing_reports.get_all_cards_api_wb()
        # df_all_card_on_wb.to_excel(all_cards_wb)
        # df = io_output.io_output(df_all_card_on_wb)
        # yandex_disk_handler.upload_to_yandex_disk(file=df,
        #                                           file_name=all_cards_wb,
        #                                           app_config_path=app.config['YANDEX_ALL_CARDS_WB'])

        print(df_to_str)

        # df_output = df_to_str.merge(df_all_card_on_wb,
        #                             how='left',
        #                             left_on=['Артикул товара', 'Размер'],
        #                             right_on=['vendorCode', 'techSize'], )

        df_output = df_to_str.merge(df_all_card_on_wb,
                                    left_on=['Артикул товара', 'Размер'],
                                    right_on=['vendorCode', 'techSize'],
                                    how='outer',
                                    suffixes=['', '_'],
                                    indicator=True)

        df_output = df_output[df_output['_merge'] == 'left_only']

        print('df_output.xlsx')

        df = io_output.io_output(df_output)
        return send_file(df, as_attachment=True, attachment_filename='spec_created.xlsx', )

    return render_template('upload_data_to_spec_wb_transcript.html', doc_string=data_to_spec_wb_transcript.__doc__)


@app.route('/data_to_spec_merging', methods=['GET', 'POST'])
@login_required
def data_to_spec_merging():
    """Смержить 2 excel файла, порядок в алфавитном - в первом оставляем, если уже были"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_from = pd.read_excel(uploaded_files[0])
        df_to = pd.read_excel(uploaded_files[1])
        df = spec_modifiyer.merge_spec(df_to, df_from, left_on='Артикул товара', right_on='Артикул товара')
        df = io_output.io_output(df)

        return send_file(df, as_attachment=True, attachment_filename='spec.xlsx')

    return render_template('upload_specs.html')


@app.route('/take_off_boxes', methods=['GET', 'POST'])
@login_required
def take_off_boxes():
    """
    На 08.12.2022 - не актуально - функция сложна в реализации и редко используется.
    Удаляет коробки с товарами, которых много, на входе эксель таблица с артикулами и кол-вом ограничителем,
    шапка первого: Артикул товара (полный с размером), второго: Артикул, третьего: Можно
    """
    if request.method == 'POST':
        dfs = spec_modifiyer.request_to_df(flask.request)
        df = spec_modifiyer.merge_spec(dfs[0], dfs[1], how='left')
        print(df)
        df = df_worker.df_take_off_boxes(df)
        df = io_output.io_output(df)
        return send_file(df, as_attachment=True, attachment_filename='table_take_off_boxes.xlsx')

    return render_template('upload_take_off_boxes.html', doc_string=take_off_boxes.__doc__)

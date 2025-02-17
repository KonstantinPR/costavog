from app import app
from flask import render_template, request, send_file
import pandas as pd
from app.modules import io_output, spec_modifiyer, yandex_disk_handler, API_WB, data_transforming_module, \
    request_handler, pandas_handler
from flask_login import login_required
import time


@app.route('/data_to_spec_wb_transcript', methods=['GET', 'POST'])
@login_required
def data_to_spec_wb_transcript():
    """
    08.12.2024 - После создания надо смержить через раздел data_to_spec_merging в первую таблицу со второй,
     а затем вручную вставить в текущий шаблон вайлдберриз в 5ю строку.

    Не подтягивать по API артикулы c WB
    В спецификации должны быть товары одного вида - например только шубки или только обувь или только джинсы и т.д.
    Заполняется спецификация на основе справочников с яндекс.диска в TASKER.
    На входе excel с шапкой: ARTICLE, Размеры (размерный ряд товара в строку), Цена - не обязательная (оптовая).
    В строку вбиваем размеры в формате 40 56 2 (где 40 первый размер, 56 последний, 2 шаг - т.е. на выходе получим
    40 42 44 46 ... 56 (строка может быть пустой - если в прикрепляемом файле будет заполненный столбец с размерами).
    """

    if not request.method == 'POST':
        return render_template('upload_data_to_spec_wb_transcript.html', doc_string=data_to_spec_wb_transcript.__doc__)

    DEFAULT_SPEC_FOLDER = "/TASKER/CHARACTERS/"

    size_col_name = "Размеры"
    art_col_name = "ARTICLE"

    df_income_date = request_handler.to_df(request, input_column=art_col_name)
    df_income_date = df_income_date.drop_duplicates(subset=art_col_name)
    df_income_date = df_income_date.reset_index(drop=True)
    # print(df_income_date)
    # df_characters = yandex_disk_handler.get_excel_file_from_ydisk(app.config['CHARACTERS_PRODUCTS'])
    spec_type = spec_modifiyer.spec_definition(df_income_date, col_name=art_col_name)

    time.sleep(1)
    print(spec_type)

    path_spec = str(DEFAULT_SPEC_FOLDER) + str(spec_type)
    df_spec_example = yandex_disk_handler.get_excel_file_from_ydisk(path_spec, to_str=['Лекало', 'Префикс'])
    if df_spec_example.empty:
        df_spec_example = yandex_disk_handler.get_excel_file_from_ydisk(app.config[spec_type],
                                                                        to_str=['Лекало', 'Префикс'])

    # df_art_prefixes = yandex_disk_handler.get_excel_file_from_ydisk(app.config['ECO_FURS_WOMEN'])
    df_colors = yandex_disk_handler.get_excel_file_from_ydisk(app.config['COLORS'])
    df_income_date = data_transforming_module.str_input_to_full_str(df_income_date, request,
                                                                    size_col_name,
                                                                    input_name='size_forming',
                                                                    html_template='upload_vertical_sizes.html')
    df_verticaling_sizes = data_transforming_module.vertical_size(df_income_date)
    # df_check_exist_art = spec_modifier.check_art_existing(df_verticaling_sizes)
    # df_merge_spec = spec_modifiyer.merge_spec(df_verticaling_sizes, df_spec_example, 'Артикул товара')
    df_art_prefixes_adding = spec_modifiyer.picking_prefixes(df_verticaling_sizes, df_spec_example,
                                                             col_name=art_col_name)
    df_colors_adding = spec_modifiyer.picking_colors(df_art_prefixes_adding, df_colors, df_col_name=art_col_name)
    # df_pattern_merge = spec_modifiyer.merge_spec(df_colors_adding, df_spec_example, 'Лекало', 'Лекало')
    df_pattern_merge = spec_modifiyer.merge_spec(df_spec_example, df_colors_adding, 'Лекало', 'Лекало')
    # df_pattern_merge = spec_modifiyer.merge_spec(df_spec_example, df_colors_adding)
    df_clear = spec_modifiyer.df_clear(df_pattern_merge, col_name=art_col_name)
    # df_clear.to_excel("df_clear.xlsx")
    df_added_some_col = spec_modifiyer.col_adding(df_clear, col_name=art_col_name)
    df = spec_modifiyer.col_str(df_added_some_col, ['Баркод товара'])
    df = spec_modifiyer.sizes_translate(df, spec_type)

    # to check if art is already in wb:
    to_check_art_wb = request.form.get('to_check_art_wb')

    if to_check_art_wb:
        all_cards_wb_df = API_WB.get_all_cards_api_wb()
        name_excel_all_cards_wb = "all_cards_wb.xlsx"
        all_cards_wb_df.to_excel(name_excel_all_cards_wb)
        df = df.merge(all_cards_wb_df,
                      left_on=[art_col_name, 'Размер'],
                      right_on=['vendorCode', 'techSize'],
                      how='outer',
                      suffixes=['', '_'],
                      indicator=True)
        df = df[df['_merge'] == 'left_only']

    df_output = df.drop_duplicates(subset=[art_col_name, 'Размер'], keep=False)
    df_output['Артикул продавца'] = df.loc[:, art_col_name]
    if "Название" in df:
        df_output['Наименование'] = df.loc[:, 'Название']
    if "Цена розничная" in df:
        df_output['Цена'] = df.loc[:, 'Цена розничная']

    # to_fill_template_02 = request.form.get('to_fill_template_02')
    # if to_fill_template_02:
    #     template_02 = yandex_disk_handler.get_excel_file_from_ydisk(path=app.config['CLOTHES_TEMPLATE'])
    #     # df_output = template_02.merge(df_output, how='left', on="Артикул поставщика")

    # print('df_output.xlsx')

    # df = io_output.io_output(df_output)
    df = io_output.io_output(df_output)
    return send_file(df, as_attachment=True, download_name='spec_created.xlsx', )


@app.route('/data_to_spec_merging', methods=['GET', 'POST'])
@login_required
def data_to_spec_merging():
    """Смержить 2 excel файла, порядок в алфавитном - в первом оставляем, если уже были.
    Если галку поставили для заполнить новую спецификацию, заполняет первый файл вторым ровно в той последовательнисти
    как в первом (затем ручками вставляем таблицу в новую спецпфикцию, к примеру как с 25.08.2024 header на третьей
    строке, а ниже примечание, значит вставляем начиная с пятой строки на лист Товар"""

    if not request.method == 'POST':
        return render_template('upload_specs.html', doc_string=data_to_spec_merging.__doc__)

    fill_new_spec = request.form.get("fill_new_spec")

    uploaded_files = request.files.getlist("file")
    col_merge = request.form['col_merge']
    merge_option = request.form['merge_option']
    sheet_name = request.form['sheet_name']

    if fill_new_spec:
        df = spec_modifiyer.fill_new_spec(uploaded_files, sheet_name=sheet_name)
        output_file = 'merged_data.xlsx'
        df = io_output.io_output(df)

        return send_file(df, as_attachment=True, download_name=output_file)

    if merge_option == "merge":
        # Initialize an empty DataFrame to store the merged data
        merged_data = pd.read_excel(uploaded_files[0], sheet_name=sheet_name)
        # print(merged_data)
        for uploaded_file in uploaded_files[1:]:
            df_to_merge = pd.read_excel(uploaded_file, sheet_name=sheet_name)
            # Merge the data based on the specified column
            df = spec_modifiyer.merge_spec(merged_data, df_to_merge, left_on=col_merge, right_on=col_merge)
    elif merge_option == "concatenate":
        # Initialize an empty DataFrame to store the concatenated data
        concatenated_data = pd.DataFrame()
        for uploaded_file in uploaded_files:
            if not sheet_name:  # If sheet_name is empty, default to the first sheet
                df = pd.read_excel(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
            # Select only the relevant columns in the concatenate case
            # relevant_columns = ["Штрихкод", "Артикул", "Размер", "Кол-во"]
            # df = df[relevant_columns]
            # Convert Штрихкод column to string format to preserve its original value
            concatenated_data = pd.concat([concatenated_data, df])
            df = concatenated_data

    df["Баркоды"] = df["Баркоды"].astype(str)
    df["Размер"] = df["Размер"].apply(lambda x: int(x)).astype(str)

    # You can optionally save the resulting DataFrame to an Excel file or return it for download.
    # For saving to an Excel file:
    output_file = 'merged_data.xlsx'
    df = io_output.io_output(df)

    return send_file(df, as_attachment=True, download_name=output_file)

# 12.07.2024
# @app.route('/take_off_boxes', methods=['GET', 'POST'])
# @login_required
# def take_off_boxes():
#     """
#     На 08.12.2022 - не актуально - функция сложна в реализации и редко используется.
#     Удаляет коробки с товарами, которых много, на входе эксель таблица с артикулами и кол-вом ограничителем,
#     шапка первого: Артикул товара (полный с размером), второго: Артикул, третьего: Можно
#     """
#     if request.method == 'POST':
#         dfs = base_module.request_excel_to_df(flask.request)
#         df = spec_modifiyer.merge_spec(dfs[0], dfs[1], how='left')
#         # print(df)
#         df = df_worker.df_take_off_boxes(df)
#         df = io_output.io_output(df)
#         return send_file(df, as_attachment=True, download_name='table_take_off_boxes.xlsx')
#
#     return render_template('upload_take_off_boxes.html', doc_string=take_off_boxes.__doc__)

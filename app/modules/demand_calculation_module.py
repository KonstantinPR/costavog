import pandas as pd
from app.modules import API_WB, yandex_disk_handler, df_worker, img_processor, pdf_processor
import numpy as np
import os


def demand_calculation_df_to_pdf(df, file_name="output_file"):
    df_unique = pd.DataFrame(df['vendorCode'].unique(), columns=['vendorCode'])
    qt_sum = int(df['Кол-во'].sum())
    img_name_list_files = img_processor.download_images_from_yandex_to_folder(df_unique, art_col_name="vendorCode")
    path_pdf, no_photo_list = pdf_processor.images_into_pdf_2(df, art_col_name='vendorCode',
                                                              size_col_name='techSize', qt_sum=qt_sum,
                                                              file_name=file_name)
    pdf = os.path.abspath(path_pdf)
    return pdf


def demand_calculation_to_df(df_input, search_string):
    search_string_list = search_string.split()
    print(search_string_list)
    if search_string_list:
        search_string_first = search_string_list[0]
    else:
        search_string_first = None
    df_all_cards = API_WB.get_all_cards_api_wb(textSearch=search_string_first)
    df_report, file_name = yandex_disk_handler.download_from_YandexDisk()
    # print(file_name)
    df_wb_stock = API_WB.df_wb_stock_api()
    # df_wb_stock.to_excel("df_wb_stock.xlsx")

    df = df_all_cards.merge(df_report, how='left', left_on='vendorCode', right_on='supplierArticle',
                            suffixes=("", "_drop"))
    df = df.merge(df_wb_stock, how='left',
                  left_on=['vendorCode', 'techSize'],
                  right_on=['supplierArticle', 'techSize'], suffixes=("", "_drop"))

    df.to_excel("df_all_actual_stock_and_art.xlsx")

    if not df_input.empty:
        df = df_input.merge(df, how='left', left_on='vendorCode', right_on='vendorCode')

    # df = pd.read_excel("df_output.xlsx")
    cols = ['vendorCode']
    # print(cols)

    df = df.drop_duplicates(subset=['vendorCode', 'techSize'])

    if search_string_list:
        m = pd.concat([df[cols].agg("".join, axis=1).str.contains(s) for s in search_string_list], axis=1).all(1)
        df = df[m].reset_index()
    else:
        df = df.reset_index()

    df = df_worker.qt_to_order(df)
    df['techSize'] = pd.to_numeric(df['techSize'], errors='coerce').fillna(0).astype(np.int64)
    df['quantityFull'] = pd.to_numeric(df['quantityFull'], errors='coerce').fillna(0).astype(np.int64)
    df = df.sort_values(by=['Прибыль_sum', 'vendorCode', 'techSize'], ascending=False)
    df = df.reset_index(drop=True)
    return df

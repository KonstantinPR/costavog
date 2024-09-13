import pandas as pd
from app.modules import API_WB, yandex_disk_handler, img_processor, pdf_processor, pandas_handler
import numpy as np
import os


def demand_calculation_df_to_pdf(df, file_name="output_file"):
    df_unique = pd.DataFrame(df['vendorCode'].unique(), columns=['vendorCode'])
    qt_sum = int(df['Кол-во'].sum())
    img_name_list_files = img_processor.download_images_from_yandex_to_folder(df_unique, art_col_name="vendorCode")
    path_pdf, no_photo_list = pdf_processor.images_into_pdf(df, art_col_name='vendorCode',
                                                            size_col_name='techSize', qt_sum=qt_sum,
                                                            file_name=file_name)
    pdf = os.path.abspath(path_pdf)
    return pdf


def _count_demand(qt, re_per_qt, re_per_qt_all, net):
    if re_per_qt >= net:
        return qt + 2
    if re_per_qt > net / 2:
        return qt + 1
    if re_per_qt < 0 and re_per_qt_all < 0:
        return qt
    if re_per_qt < -net / 2 and re_per_qt_all < 0:
        return qt-1

    return qt


def clear_demand(df, qt_correct=True):
    if qt_correct:
        df['Кол-во'] = [_count_demand(qt, re_per_qt, re_per_qt_all, net) for qt, re_per_qt, re_per_qt_all, net in
                        zip(df['Кол-во'], df['Маржа-себест./ шт.'], df['Маржа-себест./ шт._all'], df['net_cost'])]
    df = df[
        ['vendorCode', 'techSize', 'Кол-во', 'quantityFull', 'Маржа-себест./ шт.', 'Маржа-себест.', 'Маржа-себест._all',
         'Маржа-себест./ шт._all']]
    # df = df[df['Кол-во'] > 0]

    return df


def demand_calculation_to_df(df_input, search_string, min_stock=1, testing_mode=False, is_from_yadisk=False):
    search_string_list = search_string.split()
    print(search_string_list)
    if search_string_list:
        search_string_first = search_string_list[0]
    else:
        search_string_first = None

    df_all_cards = API_WB.get_all_cards_api_wb(testing_mode=testing_mode, textSearch=search_string_first,
                                               is_from_yadisk=is_from_yadisk)
    df_report, file_name = yandex_disk_handler.download_from_YandexDisk('REPORT_DETAILING_UPLOAD')
    df_report_all, file_name_all = yandex_disk_handler.download_from_YandexDisk('REPORT_DETAILING_UPLOAD_ALL')
    # df_report.to_excel("df_report.xlsx")
    # print(file_name)

    request = {'no_sizes': False, 'no_city': 'no_city'}

    df_wb_stock = API_WB.get_wb_stock_api(testing_mode=testing_mode, request=request, is_upload_yandex=False)

    # Assuming df is your original DataFrame
    grouping_columns = ['supplierArticle', 'techSize']
    aggregation_dict = {col: 'first' if col not in grouping_columns else 'sum' for col in df_wb_stock.columns}

    # Group by 'supplierArticle' and 'techSize', summing 'quantityFull' and selecting first for other columns
    df_wb_stock = df_wb_stock.groupby(grouping_columns, as_index=False).agg(aggregation_dict)

    # Save the resulting DataFrame to a new Excel file
    # df_wb_stock.to_excel("df_wb_stock.xlsx")

    df = df_all_cards.merge(df_report, how='left', left_on='vendorCode', right_on='Артикул поставщика',
                            suffixes=("", "_drop"))
    df_report_all = df_report_all[['Артикул поставщика', 'Маржа-себест./ шт.', 'Маржа-себест.']]
    df = df.merge(df_report_all, how='left', left_on='Артикул поставщика', right_on='Артикул поставщика',
                  suffixes=("", "_all"))

    df = df.merge(df_wb_stock, how='left',
                  right_on=['supplierArticle', 'techSize'], left_on=['vendorCode', 'techSize'],
                  suffixes=("_drop", ""))

    # df.to_excel("df_all_actual_stock_and_art.xlsx")

    if not df_input.empty:
        df = df_input.merge(df, how='left', left_on='vendorCode', right_on='vendorCode')

    # df = pd.read_excel("df_output.xlsx")
    cols = ['vendorCode']
    # print(cols)

    df = df.drop_duplicates(subset=['vendorCode', 'techSize'])
    # df.to_excel('qt_to_order.xlsx')

    if search_string_list:
        m = pd.concat([df[cols].agg("".join, axis=1).str.contains(s) for s in search_string_list], axis=1).all(1)
        df = df[m].reset_index()
    else:
        df = df.reset_index()

    df['quantityFull'] = pd.to_numeric(df['quantityFull'], errors='coerce').fillna(0).astype(np.int64)
    df = qt_to_order(df, min_stock=min_stock)
    df['techSize'] = pd.to_numeric(df['techSize'], errors='coerce').fillna(0).astype(np.int64)
    df = df.sort_values(by=['Маржа-себест.', 'vendorCode', 'techSize'], ascending=False)
    df = df.reset_index(drop=True)
    return df


def qt_to_order(df, min_stock=1):
    """
    stay only goods for order that 0 on wherehouse
    :param df:
    :return df:
    """
    false_list = pandas_handler.FALSE_LIST_2
    # df.to_excel('qt_to_order.xlsx')
    df['Кол-во'] = [min_stock if x in false_list else min_stock - x for x in df['quantityFull']]

    return df

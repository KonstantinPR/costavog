import logging
import pandas as pd
from app import app
from flask import render_template, request, send_file
from flask_login import login_required
from app.modules import io_output, request_handler, yandex_disk_handler, API_WB, pandas_handler
import requests
from random import randrange


def scheduled_get_rating(col_name='Артикул', testing_mode=False, is_update=True, batch_size=300):
    try:
        # Retrieve unique nmIDs from the API
        nmIDs = API_WB.get_all_cards_api_wb(testing_mode=testing_mode)['nmID'].unique()[:1500]

        # Split nmIDs into batches
        nmID_batches = [nmIDs[i:i + batch_size] for i in range(0, len(nmIDs), batch_size)]

        # Iterate through batches
        gotten_cards = batch_size
        for nmID_batch in nmID_batches:
            logging.info(f"got ratings on {gotten_cards} cards ...")

            # Create a new DataFrame to store ratings
            rating_df = pd.DataFrame(columns=[col_name, 'Rating', 'Feedbacks'])

            # Fill in the 'Артикул' column with the current batch of nmIDs
            rating_df[col_name] = nmID_batch

            # Download the current DataFrame from Yandex Disk
            current_df, _ = yandex_disk_handler.download_from_YandexDisk(path='RATING')

            # Update ratings in the current DataFrame
            rating_df = get_rating(rating_df, col_name=col_name, is_to_yadisk=False)

            # Set the 'key' column as index
            current_df.set_index(col_name, inplace=True)
            rating_df.set_index(col_name, inplace=True)

            # Update values in df1 using values from df2
            current_df.update(rating_df)

            # Reset index to make 'key' a column again
            current_df.reset_index(inplace=True)

            # merged_df = pandas_handler.df_col_merging(current_df, rating_df, col_name)

            # # Filter out columns with the '_DROP' suffix
            # merged_df = merged_df.filter(regex='^(?!.*_DROP)')

            # Save the updated DataFrame to Yandex Disk
            io_df = io_output.io_output(current_df)
            file_name = f'rating.xlsx'
            logging.info(f'Updated DataFrame uploaded to YandexDisk with name {file_name}')
            yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['RATING'])
            gotten_cards += len(nmID_batch)

        return current_df

    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        return None


def get_rating(df, col_name='Артикул', is_to_yadisk=True):
    """Extract rating and number of feedbacks for each product ID."""
    for index, row in df.iterrows():
        good_id = row[col_name]
        logging.info(f"Getting: {good_id} ...")
        try:
            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) YaBrowser/22.9.4.866 Yowser/2.5 Safari/537.36'
            }
            url = f'https://card.wb.ru/cards/detail?spp=26&curr=rub&nm={good_id}'
            r = requests.get(url=url, headers=headers)
            if r.status_code == 200:
                logging.info(f"Got it: {good_id}")
                data = r.json().get('data')
                if data and data.get('products'):
                    product = data['products'][0]
                    df.at[index, 'Rating'] = product.get('rating', '')
                    df.at[index, 'Feedbacks'] = product.get('feedbacks', '')
                    logging.info(f"Rating: {df.at[index, 'Rating']}, Feedbacks {df.at[index, 'Feedbacks']}")
                else:
                    logging.warning(f"No product data found for ID: {good_id}")
            else:
                logging.error(f"Failed to retrieve data for ID: {good_id}. Status code: {r.status_code}")
        except Exception as e:
            logging.exception(f"An error occurred while processing ID: {good_id}. Error: {e}")

    # if is_to_yadisk:
    #     io_df = io_output.io_output(df)
    #     file_name = f'rating.xlsx'
    #     logging.info(f'df as excel uploading to YandexDisk with name {file_name}')
    #     yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['RATING'])

    return df


@app.route('/parser_rating_wb', methods=['GET', 'POST'])
@login_required
def parser_rating_wb():
    """Parse rating and number of feedbacks using WB Article IDs."""
    if request.method == 'POST':

        col_name = 'Артикул'
        con_rating_name = 'Rating'
        con_feedbacks_name = 'Feedbacks'
        df = request_handler.to_df(request, input_column=col_name)
        print(f"print df {df}")

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame()

        if df.empty:
            df = scheduled_get_rating()
        else:
            df[col_name] = ''
            df[con_rating_name] = ''
            df[con_feedbacks_name] = ''
            df = get_rating(df)

        file = io_output.io_output(df)
        return send_file(file, download_name="rating.xlsx", as_attachment=True)

    return render_template("upload_parser_rating_wb.html", doc_string=parser_rating_wb.__doc__)

# import logging
# from app import app
# from flask import render_template, request, send_file
# from flask_login import login_required
# from app.modules import io_output, request_handler
# import requests
#
#
# """
# Парсер wildberries по ссылке на каталог (указывать без фильтров)
# Парсер не идеален, есть множество вариантов реализации, со своими идеями
# и предложениями обязательно пишите мне, либо в группу, ссылка ниже.
# Ссылка на статью ВКонтакте: https://vk.com/@happython-parser-wildberries
# По всем возникшим вопросам, можете писать в группу https://vk.com/happython
# парсер wildberries по каталогам 2022, обновлен 22.09.2022 - на данное число работает исправно
# """
#
#
# def get_rating(goods_id_list):
#     """извлечение значения рейтинга и количества отзывов"""
#
#     rating_list = []
#     for good_id in goods_id_list:
#         headers = {
#             'Accept': '*/*',
#             'Accept-Encoding': 'gzip, deflate, br',
#             'Accept-Language': 'ru,en;q=0.9',
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) YaBrowser/22.9.4.866 Yowser/2.5 Safari/537.36'
#         }
#
#         # print(good_id)
#         url = f'https://card.wb.ru/cards/detail?spp=26&curr=rub&nm={good_id}'
#         r = requests.get(url=url, headers=headers)
#         if r.json()['data']['products']:
#             # print(r.json()['data']['products'])
#             # good_id = r.json()['data']['products'][0]['id']
#             rating = r.json()['data']['products'][0]['rating']
#             feedbacks = r.json()['data']['products'][0]['feedbacks']
#         else:
#             rating = ''
#             feedbacks = ''
#
#         rating_list.append([rating, feedbacks])
#
#     return rating_list
#
#
#
#
# @app.route('/parser_rating_wb', methods=['GET', 'POST'])
# @login_required
# def parser_rating_wb():
#     """Парсинг рейтинга и количества отзывов через Артикулы WB, шапка в txt файле = Артикул"""
#
#     if request.method == 'POST':
#         col_name = 'Артикул'
#         rating = 'Рейтинг'
#         feedbacks = 'Кол-во отзывов'
#         # df_column = io_output.io_txt_request(request, name_html="upload_parser_rating_wb.html",
#         #                                      inp_name='file', col_name=col_name)
#         df_column = request_handler.to_df(request, input_column="Артикул")
#         art_list = [x for x in df_column[col_name]]
#         rating_list = get_rating(art_list)
#         df_column[rating] = [x[0] if x else '' for x in rating_list]
#         df_column[feedbacks] = [x[1] if x else '' for x in rating_list]
#         file = io_output.io_output(df_column)
#
#         return send_file(file, download_name="parser_rating_wb.xlsx", as_attachment=True)
#     return render_template("upload_parser_rating_wb.html", doc_string=parser_rating_wb.__doc__)

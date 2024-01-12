from app import app
from flask import render_template, request, send_file
from flask_login import login_required
from app.modules import io_output, request_handler
import requests


"""
Парсер wildberries по ссылке на каталог (указывать без фильтров)
Парсер не идеален, есть множество вариантов реализации, со своими идеями 
и предложениями обязательно пишите мне, либо в группу, ссылка ниже.
Ссылка на статью ВКонтакте: https://vk.com/@happython-parser-wildberries
По всем возникшим вопросам, можете писать в группу https://vk.com/happython
парсер wildberries по каталогам 2022, обновлен 22.09.2022 - на данное число работает исправно
"""


def get_rating(goods_id_list):
    """извлечение значения рейтинга и количества отзывов"""

    rating_list = []
    for good_id in goods_id_list:
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) YaBrowser/22.9.4.866 Yowser/2.5 Safari/537.36'
        }

        # print(good_id)
        url = f'https://card.wb.ru/cards/detail?spp=26&curr=rub&nm={good_id}'
        r = requests.get(url=url, headers=headers)
        if r.json()['data']['products']:
            # print(r.json()['data']['products'])
            # good_id = r.json()['data']['products'][0]['id']
            rating = r.json()['data']['products'][0]['rating']
            feedbacks = r.json()['data']['products'][0]['feedbacks']
        else:
            rating = ''
            feedbacks = ''

        rating_list.append([rating, feedbacks])

    return rating_list




@app.route('/parser_rating_wb', methods=['GET', 'POST'])
@login_required
def parser_rating_wb():
    """Парсинг рейтинга и количества отзывов через Артикулы WB, шапка в txt файле = Артикул"""

    if request.method == 'POST':
        col_name = 'Артикул'
        rating = 'Рейтинг'
        feedbacks = 'Кол-во отзывов'
        # df_column = io_output.io_txt_request(request, name_html="upload_parser_rating_wb.html",
        #                                      inp_name='file', col_name=col_name)
        df_column = request_handler.to_df(request, input_column="Артикул")
        art_list = [x for x in df_column[col_name]]
        rating_list = get_rating(art_list)
        df_column[rating] = [x[0] if x else '' for x in rating_list]
        df_column[feedbacks] = [x[1] if x else '' for x in rating_list]
        file = io_output.io_output(df_column)

        return send_file(file, download_name="parser_rating_wb.xlsx", as_attachment=True)
    return render_template("upload_parser_rating_wb.html", doc_string=parser_rating_wb.__doc__)

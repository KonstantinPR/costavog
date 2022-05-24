import flask
from app import app
from flask import flash, render_template, request, send_file
from flask_login import login_required
import pandas as pd
import requests
from app.modules import io_output



def get_rating(arts):
    rating = {}
    review_count = {}
    for i in arts:
        url = f"https://wbxcatalog-ru.wildberries.ru/nm-2-card/" \
              f"catalog?spp=0&pricemarginCoeff=1.0&reg=0&appType=1&emp=0&locale=ru&lang=ru&curr=rub&nm={str(i)}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:100.0) Gecko/20100101 Firefox/100.0"
        }
        data = requests.get(url, headers=headers).json()["data"]["products"][0]
        print(f"{data['name']}\n{data['rating']} stars from {data['feedbacks']} reviews.")
        rating[i] = data['rating']
        review_count[i] = data['feedbacks']

    return rating, review_count


@app.route('/parser-rating-wb', methods=['GET', 'POST'])
@login_required
def parser_rating_wb():
    """Обработка файла excel  - шапка нужна"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df = pd.read_excel(uploaded_files[0])
        arts = df["Номенклатура"].tolist()

        rating, review_count = get_rating(arts)

        d = {}
        good_value = []
        rating_value = []
        review_count_value = []

        for key, value in rating.items():

            good_value.append(key)
            try:
                rating_value.append(value.text)
            except BaseException:
                rating_value.append(value)

        d["Номенклатура"] = good_value
        d["Рейтинг"] = rating_value

        for key, value in review_count.items():
            # отсекаем слова от чисел с отзывами

            try:
                value = value.text.split()
                value = value[0]
                print(value)
                review_count_value.append(value.text)
            except BaseException:
                review_count_value.append(value)

        d["Кол-во отзывов"] = review_count_value

        d = pd.DataFrame(data=d)
        file = io_output.io_output(d)

        return send_file(file, attachment_filename="parser-rating-wb.xlsx", as_attachment=True)
    return render_template("upload_parser_rating_wb.html")

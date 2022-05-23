import flask
from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
from io import BytesIO
from app.modules import io_output

from werkzeug.security import generate_password_hash, check_password_hash


def get_rating(arts):
    time_wait = 2
    rating = {}
    review_count = {}
    for i in arts:
        url = f"https://www.wildberries.ru/catalog/{str(i)}/detail.aspx?targetUrl=IN"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        print(soup)
        print(i)
        rating[i] = soup.find('span', {'data-link': 'text{: product^star}'})
        print(rating[i])

        review_count[i] = soup.find('span', {'data-link': "{include tmpl='productCardCommentsCount'}"})
        print(review_count[i])
        time.sleep(time_wait)

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

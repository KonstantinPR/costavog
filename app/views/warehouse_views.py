from app import app
from flask import render_template, request, redirect, send_file, flash
from urllib.parse import urlencode
from app.modules import warehouse_module, io_output, data_transforming_module
import pandas as pd
import flask
import requests
import yadisk
import os
from random import randrange
import shutil
from PIL import Image
from flask_login import login_required, current_user
import datetime

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.tree import DecisionTreeRegressor
import pickle
from sklearn.metrics import mean_squared_error
from math import sqrt


@app.route('/ya_gpt_predict', methods=['POST', 'GET'])
@login_required
def ya_gpt_predict():
    """22/01/2022 chatGPT testing solving problems"""

    return render_template('upload_ya_gpt.html', doc_string=ya_gpt_train.__doc__)


@app.route('/ya_gpt_train', methods=['POST', 'GET'])
@login_required
def ya_gpt_train():
    """22/01/2022 chatGPT are teaching make predictions """

    # load data
    data = pd.read_excel("data.xlsx")

    # split data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(data.drop(columns=["money_spent"]),
                                                        data["money_spent"], test_size=0.2)
    # train linear regression model
    model = DecisionTreeRegressor()
    model.fit(X_train, y_train)

    # evaluate model on test set
    score = model.score(X_test, y_test)
    print(f"Model accuracy: {score}")

    # make predictions on test set
    y_pred = model.predict(X_test)

    # calculate MSE and RMSE
    mse = mean_squared_error(y_test, y_pred)
    rmse = sqrt(mse)
    print(f"MSE: {mse}, RMSE: {rmse}")

    # save the model in the root folder
    with open('model.pkl', 'wb') as f:
        pickle.dump(model, f)

    if request.method == 'POST':
        # Load the trained model from a file
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)

        def predict_money_spent(model, sex, buyer_name, day_of_week, time_of_day):
            data = [{'buyer_name': buyer_name, 'sex': sex, 'day_of_week': day_of_week, 'time_of_day': time_of_day}]
            data = pd.DataFrame(data)
            prediction = model.predict(data)[0]
            return prediction

        buyer_name = 1
        sex = 1
        day_of_week = 1
        time_of_day = 16
        print(f"prediction is {predict_money_spent(model, sex, buyer_name, day_of_week, time_of_day)}")

    return render_template('upload_ya_gpt.html', doc_string=ya_gpt_train.__doc__)


@app.route('/arrivals_of_products', methods=['POST', 'GET'])
@login_required
def arrivals_of_products():
    """
    Вытягивает все транзакции с указанных путей.
    На выбор доп. опции: с рекурсией - захватит все вложенные файлы
    Чекбокс - для вертикализирования размеров. Т.е из строки размеров, например 40, 42, 44, написанных в строку
    получим запись, где каждый размер с переносом строки.
    """

    if request.method == 'POST':
        # create an empty list to store the DataFrames of the Excel files
        path = app.config["FULL_PATH_ARRIVALS"]
        print(path)
        list_paths_files = warehouse_module.get_list_paths_files(path, file_names=["Приход"])
        df = warehouse_module.df_from_list_paths_excel_files(list_paths_files)

        if not df:
            flash("DataFrame пустой, возможно неверно настроены пути или папки не существуют")
            return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)

        df = pd.concat(df)

        if 'checkbox_is_vertical' in request.form:
            df = data_transforming_module.vertical_size(df)

        df_output = io_output.io_output(df)
        file_name = f"arrivals_of_products_on_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx"
        return send_file(df_output, as_attachment=True, attachment_filename=file_name)

    return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)

import pandas as pd
from app import app
from flask import render_template, request, send_file
from flask_login import login_required
from app.modules import models_module, io_output
import pickle

file_name_train = "wb_dynamic_revenue_report-2022-10-28-2023-01-28.xlsx"
file_name_test = "wb_dynamic_revenue_report-2022-10-28-2023-01-28-predict.xlsx"
included_col_names = ["brand_name", "Предмет", "Цвет", "net_cost", "price_disc", "Логистика шт", "Прибыль - ед."]
goal_col = "Прибыль - ед."
encoded_col_name = ["brand_name", "Предмет", "Цвет"]
file_name_output = "wb_dynamic_output.xlsx"


@app.route('/train_model', methods=['POST', 'GET'])
@login_required
def train_model():
    """22/01/2022 training model to predict data """
    if request.method == 'POST':
        df_predict = models_module.xgb()
        excel_io = io_output.io_output(df_predict)
        return send_file(excel_io, download_name=file_name_output, as_attachment=True)
    return render_template('upload_models.html', doc_string=train_model.__doc__)


@app.route('/train_model2', methods=['POST', 'GET'])
@login_required
def train_model2():
    """22/01/2022 training model to predict data """
    if request.method == 'POST':
        data_train = pd.read_excel(file_name_train)
        data_train = data_train[included_col_names]
        # data_train = data_train[data_train[drop_null_rol_col] != 0]
        print(data_train)
        model = models_module.train(data_train, encoded_col_name, goal_col)
        with open('model.pkl', 'wb') as f:
            pickle.dump(model, f)
    return render_template('upload_models.html', doc_string=train_model.__doc__)


@app.route('/predict_by_model', methods=['POST', 'GET'])
@login_required
def predict_by_model():
    """22/01/2022 use model to predict data """

    if request.method == 'POST':
        with open('model.pkl', 'rb') as f:
            model = pickle.load(f)

        data_train = pd.read_excel(file_name_train)
        data_train = data_train[included_col_names].drop(columns=goal_col)

        print(data_train)
        data_test = pd.read_excel(file_name_test)
        data_test = data_test[included_col_names].drop(columns=goal_col)
        data_test = models_module.nan_correction(data_test)
        print(f"data_test {data_test}")
        data_train = models_module.nan_correction(data_train)
        data_train, default_label_encoder = models_module.mapping_categorical(data_train, encoded_col_name)
        data_test = models_module.redundant_col_name(data_train, data_test)
        print(f"data_test {data_test}")
        data_predict = models_module.predict(model, data_test, encoded_col_name, goal_col, default_label_encoder)
        excel_io = io_output.io_output(data_predict)
        return send_file(excel_io, download_name=file_name_output, as_attachment=True)

    return render_template('upload_models.html', doc_string=predict_by_model.__doc__)

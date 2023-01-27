import pandas as pd

from app import app
from flask import render_template, request, send_file
from flask_login import login_required
from app.modules import models_module, io_output
import pickle


@app.route('/train_model', methods=['POST', 'GET'])
@login_required
def train_model():
    """22/01/2022 training model to predict data """
    if request.method == 'POST':
        goal_col = "species"
        encoded_col_name = [goal_col]
        model = models_module.train("IRIS - IRIS.xlsx", encoded_col_name, goal_col)
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
        data = pd.read_excel('IRIS - IRIS-predict.xlsx')
        data_set = pd.read_excel('IRIS - IRIS.xlsx')
        goal_col = "species"
        encoded_col_name = [goal_col]
        data_set, default_label_encoder = models_module.mapping_categorical(data_set, encoded_col_name)
        data = models_module.redundant_col_name(data_set, data)
        data_predict = models_module.predict(model, data, encoded_col_name, goal_col, default_label_encoder)
        excel_io = io_output.io_output(data_predict)
        return send_file(excel_io, download_name="IRIS - IRIS-outcome.xlsx", as_attachment=True)

    return render_template('upload_models.html', doc_string=predict_by_model.__doc__)

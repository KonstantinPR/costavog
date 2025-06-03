import logging
import pandas as pd
import app.modules.request_handler
from app import app
from flask import render_template, request, redirect, send_file, flash, url_for
from flask_login import login_required, current_user
import datetime
import io
from app.modules import API_WB, API_OZON, OZON_module
from app.modules import io_output, yandex_disk_handler, request_handler, pandas_handler, OZON_actions_module
from datetime import date, timedelta


@app.route('/get_actions', methods=['POST', 'GET'])
@login_required
def get_actions():
    """
    To get current actions OZON and fix prices accordingly if the fit for margin
    """

    if not request.method == 'POST':
        return render_template('upload_actions_ozon.html', doc_string=get_actions.__doc__)

    testing_mode = 'testing_mode' in request.form
    is_info_actions = 'is_info_actions' in request.form

    headers = {
        'Client-Id': app.config['OZON_CLIENT_ID'],
        'Api-Key': app.config['OZON_API_TOKEN'],
        'Content-Type': 'application/json'
    }

    df_actions = OZON_actions_module.api_get_actions(testing_mode=testing_mode)
    df_actions_candidates = OZON_actions_module.get_candidates_for_action(df_actions=df_actions,
                                                                          testing_mode=testing_mode)
    df_updated_prices, _ = yandex_disk_handler.download_from_YandexDisk(app.config['YANDEX_UPDATED_PRICES'])
    df_merged = df_updated_prices.merge(df_actions_candidates, left_on='product_id', right_on='id', how='left',
                                        suffixes=('', '_y'))
    df_merged = OZON_actions_module.analyze_availability_actions(df_merged)
    df_go_out_action = OZON_actions_module.go_out_action(df_merged, headers, testing_mode=testing_mode)
    df_go_in_action = OZON_actions_module.go_in_action(df_go_out_action, headers, testing_mode=testing_mode)

    file_name = "df_OZON_actions.xlsx"

    return send_file(io_output.io_output(df_go_in_action), download_name=file_name, as_attachment=True)

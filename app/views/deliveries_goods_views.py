from app import app
import logging
from datetime import datetime, timedelta
from flask import render_template, request, send_file
from flask_login import login_required
from app.modules import request_handler, decorators, pandas_handler, delivery_module

DATE_FORMAT = '%Y-%m-%d'
FOLDER_DATE_FORMAT = '%d.%m.%Y'
FOLDER_PATH = app.config['DELIVERY_PRODUCTS']


@app.route('/get_deliveries_goods', methods=['POST', 'GET'])
@login_required
@decorators.timing_decorator
def get_deliveries_goods():
    if not request.method == 'POST':
        return render_template('concatenate_deliveries_wb.html', doc_string=get_deliveries_goods.__doc__)

    # Define the path and date range
    folder_path = FOLDER_PATH
    period_days = 0
    if request.form.get('period_days'):
        try:
            period_days = int(request.form.get('period_days'))
            date_from = datetime.now() - timedelta(days=period_days)
            date_end = datetime.now()
        except ValueError:
            logging.error(f"Invalid period_months value: {request.form.get('period_days')}")
            return "Error: Invalid period_months value"
    else:
        date_from = request_handler.request_date_from(request)
        date_end = request_handler.request_date_end(request)
        try:
            date_from = datetime.strptime(date_from, DATE_FORMAT)
            date_end = datetime.strptime(date_end, DATE_FORMAT)
        except ValueError as e:
            logging.error(f"Error transforming dates: {e}")
            return "Error: Invalid date format"

    dfs_dict = delivery_module.process_delivering(folder_path=folder_path, period=period_days, date_from=date_from,
                                                  date_end=date_end)

    # Filter out the empty DataFrames and their names
    filtered_dfs_list, filtered_dfs_names_list = pandas_handler.keys_values_in_list_from_dict(dfs_dict, ext='.xlsx')

    print(f"ready to zip {filtered_dfs_names_list}")

    # Add the timestamp to the zip_name
    zip_file, zip_name = pandas_handler.files_to_zip(filtered_dfs_list, filtered_dfs_names_list)

    return send_file(zip_file, download_name=zip_name, as_attachment=True)

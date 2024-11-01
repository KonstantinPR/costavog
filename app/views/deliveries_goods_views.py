import logging


from app import app
import logging
import os
from datetime import datetime, timedelta
import pandas as pd
from flask import render_template, request, send_file
from flask_login import login_required
from app.modules import io_output, request_handler, decorators
import multiprocessing

# Define constants
DATE_FORMAT = '%Y-%m-%d'
FOLDER_DATE_FORMAT = '%d.%m.%Y'
FOLDER_PATH = r'C:\YandexDisk\СЕТЕВЫЕ МАГАЗИНЫ\WILDBERRIES\ОТПРАВКИ'
SKIP_ROWS_NUM = 2
FOLDER_PARTS = 2
COLUMNS_AS_STR = ['Штрихкод', 'Куда', 'Артикул', 'Размер']
REQUIRED_COLUMNS = ['Штрихкод', 'Куда', 'Артикул', 'Размер', 'Кол-во']
EXCEL_FILE_NAMES = ['ИМПОРТ1C.xlsm', 'DB.xlsm', 'DB1.xlsm']
SHEET_NAME = 'ОПРЕДЛАРТ'
DATE_COL_NAME = 'Дата'
NUMBER_COL_NAME = 'Номер'
DESCRIPTION_COL_NAME = 'Описание'

# Function to process a single folder
def process_folder(folder_info):
    folder_name, folder_path, date_from, date_end = folder_info
    parts = folder_name.split(' - ')
    if len(parts) < FOLDER_PARTS:
        logging.warning(f"Unexpected folder name format: {folder_name}")
        return None

    folder_number_str = parts[0]
    folder_date_str = parts[1]
    folder_description = parts[2] if len(parts) > FOLDER_PARTS else ""

    try:
        folder_date = datetime.strptime(folder_date_str, FOLDER_DATE_FORMAT)
    except ValueError:
        logging.error(f"Error transforming folder date: {folder_date_str}")
        return None

    if not (date_from <= folder_date <= date_end):
        return None

    # Check for both possible Excel file names
    file_path = None
    for excel_file_name in EXCEL_FILE_NAMES:
        potential_file_path = os.path.join(folder_path, folder_name, excel_file_name)
        if os.path.exists(potential_file_path):
            file_path = potential_file_path
            break

    if not file_path:
        logging.warning(f"No valid file found in folder: {folder_name}")
        return None

    try:
        # Read the specific sheet and required columns
        col_to_str = {column: str for column in COLUMNS_AS_STR}
        df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=SKIP_ROWS_NUM, dtype=col_to_str)
        logging.info(f"Columns found in file {file_path}: {df.columns.tolist()}")

        if all(column in df.columns for column in REQUIRED_COLUMNS):
            df[DATE_COL_NAME] = folder_date.strftime(FOLDER_DATE_FORMAT)
            df[NUMBER_COL_NAME] = folder_number_str
            df[DESCRIPTION_COL_NAME] = folder_description
            return df[REQUIRED_COLUMNS + [DATE_COL_NAME, NUMBER_COL_NAME, DESCRIPTION_COL_NAME]]
        else:
            logging.warning(f"Required columns not found in file: {file_path}")
            return None
    except Exception as e:
        logging.error(f"Error processing file in folder {folder_name}: {e}")
        return None

@app.route('/get_deliveries_goods', methods=['POST', 'GET'])
@login_required
@decorators.timing_decorator
def get_deliveries_goods():
    if not request.method == 'POST':
        return render_template('concatenate_deliveries_wb.html', doc_string=get_deliveries_goods.__doc__)

    # Define the path and date range
    folder_path = FOLDER_PATH
    now = datetime.now()

    if request.form.get('period_months'):
        try:
            period_months = int(request.form.get('period_months'))
            date_from = now - timedelta(days=30 * period_months)
            date_end = now
        except ValueError:
            logging.error(f"Invalid period_months value: {request.form.get('period_months')}")
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

    if not os.path.exists(folder_path):
        logging.error(f"The specified path does not exist: {folder_path}")
        return f"Error: The specified path does not exist: {folder_path}"

    # Collect folder names and pass them to multiprocessing
    folder_infos = [
        (folder_name, folder_path, date_from, date_end) for folder_name in os.listdir(folder_path)
    ]

    # Use multiprocessing to process folders in parallel
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.map(process_folder, folder_infos)

    # Filter out None results and concatenate
    df_list = [df for df in results if df is not None]
    if df_list:
        result_df = pd.concat(df_list, ignore_index=True)
        file_name = "concatenated_data.xlsx"
        return send_file(io_output.io_output(result_df), download_name=file_name, as_attachment=True)
    else:
        logging.info("No data found for the specified date range.")
        return "No data found for the specified date range."

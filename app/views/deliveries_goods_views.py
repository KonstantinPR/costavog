import logging

import app.modules.request_handler
from app import app
from flask import render_template, request, send_file
from flask_login import login_required, current_user
from app.modules import io_output, request_handler
import os
import pandas as pd
from datetime import datetime, timedelta

# Define date format constants
DATE_FORMAT = '%Y-%m-%d'
FOLDER_DATE_FORMAT = '%d.%m.%Y'

# Define constant for required columns
FOLDER_PATH = r'C:\YandexDisk\СЕТЕВЫЕ МАГАЗИНЫ\WILDBERRIES\ОТПРАВКИ'
SKIP_ROWS_NUM = 2
FOLDER_PARTS = 2
COLUMNS_AS_STR = ['Штрихкод', 'Куда', 'Артикул', 'Размер']
REQUIRED_COLUMNS = ['Штрихкод', 'Куда', 'Артикул', 'Размер', 'Кол-во']

# Define constants for possible Excel file names
EXCEL_FILE_NAMES = ['ИМПОРТ1C.xlsm', 'DB.xlsm']
# Define constant for sheet name
SHEET_NAME = 'ОПРЕДЛАРТ'
DATE_COL_NAME = 'Дата'
NUMBER_COL_NAME = 'Номер'
DESCRIPTION_COL_NAME = 'Описание'


@app.route('/get_deliveries_goods', methods=['POST', 'GET'])
@login_required
def get_deliveries_goods():
    """
    Concatenate the delivering data from folders in Yandex Disk.
    The period can be provided by date_from and date_end or just by number of integer number of months until now
    (considered first)
    """

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

        # Convert date_from and date_end to datetime objects
        try:
            date_from = datetime.strptime(date_from, DATE_FORMAT)
            date_end = datetime.strptime(date_end, DATE_FORMAT)
        except ValueError as e:
            logging.error(f"Error transforming dates: {e}")
            return "Error: Invalid date format"

    print(f"date_from: {date_from} and date_end: {date_end}")

    # Check if the folder path exists
    if not os.path.exists(folder_path):
        logging.error(f"The specified path does not exist: {folder_path}")
        return f"Error: The specified path does not exist: {folder_path}"

    # Initialize an empty list to store DataFrames
    df_list = []

    # Loop through each folder in the directory
    for folder_name in os.listdir(folder_path):
        try:
            parts = folder_name.split(' - ')
            if len(parts) < FOLDER_PARTS:
                logging.warning(f"Unexpected folder name format: {folder_name}")
                continue

            folder_number_str = parts[0]
            folder_date_str = parts[1]
            folder_description = ""
            if len(parts) > FOLDER_PARTS:
                folder_description = parts[2]

            try:
                folder_date = datetime.strptime(folder_date_str, FOLDER_DATE_FORMAT)
            except ValueError:
                logging.error(f"Error transforming folder date: {folder_date_str}")
                continue

            logging.info(f"Processing folder: {folder_name} with date: {folder_date}")

            if date_from <= folder_date <= date_end:
                # Check for both possible Excel file names
                file_path = None
                for excel_file_name in EXCEL_FILE_NAMES:
                    potential_file_path = os.path.join(folder_path, folder_name, excel_file_name)
                    if os.path.exists(potential_file_path):
                        file_path = potential_file_path
                        break

                if file_path:
                    # Read the specific sheet and required columns
                    col_to_str = {column: str for column in COLUMNS_AS_STR}
                    df = pd.read_excel(file_path, sheet_name=SHEET_NAME, skiprows=SKIP_ROWS_NUM, dtype=col_to_str)
                    logging.info(f"Columns found in file {file_path}: {df.columns.tolist()}")

                    if all(column in df.columns for column in REQUIRED_COLUMNS):
                        # Add the delivering date to the DataFrame
                        df[DATE_COL_NAME] = folder_date.strftime(FOLDER_DATE_FORMAT)
                        df[NUMBER_COL_NAME] = folder_number_str
                        df[DESCRIPTION_COL_NAME] = folder_description

                        # Add the current DataFrame to the list
                        df_list.append(
                            df[REQUIRED_COLUMNS + [DATE_COL_NAME, NUMBER_COL_NAME, DESCRIPTION_COL_NAME]])
                    else:
                        logging.warning(f"Required columns not found in file: {file_path}")
                else:
                    logging.warning(f"No valid file found in folder: {folder_name}")
        except Exception as e:
            logging.error(f"Error processing folder {folder_name}: {e}")

    if df_list:
        # Concatenate all DataFrames
        result_df = pd.concat(df_list, ignore_index=True)

        file_name = "concatenated_data.xlsx"
        return send_file(io_output.io_output(result_df), download_name=file_name, as_attachment=True)
    else:
        logging.info("No data found for the specified date range.")
        return "No data found for the specified date range."

from app import app
import logging
import os
from datetime import datetime, timedelta
import pandas as pd
import multiprocessing

# Define constants
DATE_FORMAT = '%Y-%m-%d'
FOLDER_DATE_FORMAT = '%d.%m.%Y'
FOLDER_PATH = app.config['DELIVERY_PRODUCTS']
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
def _process_folder(folder_info):
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


def process_delivering(folder_path='', period=0, date_from='', date_end=''):
    """Delivering goods from our store to WB count"""

    if not date_from:
        if not period:
            period = 365
        date_from = datetime.now() - timedelta(days=period)
        date_end = datetime.now()

    if not os.path.exists(folder_path):
        logging.error(f"The specified path does not exist: {folder_path}")
        return f"Error: The specified path does not exist: {folder_path}"

        # Collect folder names and pass them to multiprocessing
    folder_infos = [
        (folder_name, folder_path, date_from, date_end) for folder_name in os.listdir(folder_path)
    ]

    # Use multiprocessing to process folders in parallel
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.map(_process_folder, folder_infos)

    # Filter out None results and concatenate
    df_list = [df for df in results if df is not None]
    if df_list:
        df_delivery_concat = pd.concat(df_list, ignore_index=True)
    else:
        logging.info("No data found for the specified date range.")
        return "No data found for the specified date range."

    df_delivery_pivot = _df_pivot_process(df_delivery_concat)
    dfs_dict = {'df_delivery_concat': df_delivery_concat, 'df_delivery_pivot': df_delivery_pivot}

    return dfs_dict


def _df_pivot_process(df, is_pivot=True):
    if not is_pivot:
        return None

    today = datetime.now()

    # Ensure 'Дата' is in datetime format
    df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y', errors='coerce')
    result = (
        df.groupby('Артикул')
        .agg(
            Total_delivery_qt=('Кол-во', 'sum'),
            Number_delivers=('Дата', 'nunique'),  # Count of unique delivery dates
            First_date_delivery=('Дата', 'min'),  # First delivery date
            Last_date_delivery=('Дата', 'max'),  # Last delivery date
            Days_between_First_Now=('Дата', lambda x: (today - x.min()).days + 1),
            First_delivery_qt=('Кол-во', lambda x: x[df.loc[x.index, 'Дата'].idxmin()] if not x.empty else 0),
            # Safeguard against empty groups
            Last_delivery_qt=('Кол-во', lambda x: x[df.loc[x.index, 'Дата'].idxmax()] if not x.empty else 0)
            # Safeguard against empty groups
        )
        .reset_index()
    )
    return result

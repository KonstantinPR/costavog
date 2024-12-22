from pandas import DataFrame
from app import app
import zipfile
import pandas as pd
import io
from app.modules import pandas_handler, yandex_disk_handler, dfs_process_module
from types import SimpleNamespace
from typing import List, Type
from varname import nameof

from app.modules.decorators import timing_decorator
from app.modules.detailing_upload_dict_module import INITIAL_COLUMNS_DICT, \
    DINAMIC_COLUMNS
from app.modules.dfs_dynamic_module import abc_xyz
from app.modules.dfs_process_module import choose_df_in, dfs_forming, choose_dynamic_df_list_in
from app.modules.promofiling_module import check_discount

'''Analize detaling WB reports, take all zip files from detailing WB and make one file EXCEL'''


@timing_decorator
def process_uploaded_files(uploaded_files):
    zip_buffer = io.BytesIO()

    if len(uploaded_files) == 1 and uploaded_files[0].filename.endswith('.zip'):
        # If there is only one file and it's a zip file, proceed as usual
        file = uploaded_files[0]
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            zip_file.writestr(file.filename, file.read())
    else:
        # If there are multiple files or a single non-zip file, create a zip archive in memory
        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
            for file in uploaded_files:
                zip_file.writestr(file.filename, file.read())

    # Reset the in-memory buffer's position to the beginning
    zip_buffer.seek(0)
    # Set the uploaded_file to the in-memory zip buffer
    file = zip_buffer

    return file


@timing_decorator
def zips_to_list(zip_downloaded):
    print(f"type of zip_downloaded {type(zip_downloaded)}")
    dfs = []

    z = zipfile.ZipFile(zip_downloaded)
    for f in z.namelist():
        # get directory name from file
        content = io.BytesIO(z.read(f))
        zip_file = zipfile.ZipFile(content)
        for i in zip_file.namelist():
            excel_0 = zip_file.read(i)
            df = pd.read_excel(excel_0)
            dfs.append(df)
    return dfs


@timing_decorator
def promofiling(promo_file, df, allowed_delta_percent=5):
    if not promo_file:
        return pd.DataFrame

    # Read the promo file into a DataFrame

    df_promo = pd.read_excel(promo_file)

    df_promo = pandas_handler.df_merge_drop(df_promo, df, "Артикул WB", "nmId", how='outer')
    df_promo = check_discount(df_promo, allowed_delta_percent)

    return df_promo


@timing_decorator
def dfs_process(df_list, r: SimpleNamespace) -> tuple[pd.DataFrame, List]:
    """dfs_process..."""
    print("""dfs_process...""")

    # element 0 in list is always general df that was through all df_list
    incl_col = list(INITIAL_COLUMNS_DICT.values())

    # API and YANDEX_DISK getting data into namespace
    d = dfs_process_module.dfs_from_outside(r)

    # must be refactored into def that gets DF class that contains df (first or combined) and dfs_list for dynamics:

    df = choose_df_in(df_list, is_first_df=r.is_first_df)
    df = pandas_handler.df_merge_drop(left_df=df, right_df=d.df_delivery, left_on='Артикул поставщика',
                                      right_on='Артикул')

    df = dfs_forming(df=df, d=d, r=r, include_columns=incl_col)

    df_dynamic_list = choose_dynamic_df_list_in(df_list, is_dynamic=r.is_dynamic)
    is_dynamic_possible = r.is_dynamic and len(df_dynamic_list) > 1
    df_completed_dynamic_list = [dfs_forming(x, d, r, incl_col) for x in df_dynamic_list if is_dynamic_possible]

    return df, df_completed_dynamic_list


@timing_decorator
def dfs_dynamic(df_dynamic_list, is_dynamic=True, testing_mode=False, is_upload_yandex=True) -> Type[DataFrame]:
    """dfs_dynamic merges DataFrames on 'Артикул поставщика' and expands dynamic columns."""
    print("dfs_dynamic...")

    if not is_dynamic:
        return pd.DataFrame

    if not df_dynamic_list:
        return pd.DataFrame

    # List of dynamic columns to analyze
    columns_dynamic = ["Маржа-себест.", "Ч. Продажа шт.", "quantityFull", "Логистика", "Хранение"]

    # Start by initializing the first DataFrame
    merged_df = df_dynamic_list[1][['Артикул поставщика'] + columns_dynamic].copy()

    # Iterate over the remaining DataFrames
    for i, df in enumerate(df_dynamic_list[2:]):  # Start from 2 to correctly handle suffixes
        # Merge with the next DataFrame on 'Артикул поставщика'
        merged_df = pd.merge(
            merged_df,
            df[['Артикул поставщика'] + columns_dynamic],
            on='Артикул поставщика',
            how='outer',  # Use 'outer' to keep all articles
            suffixes=('', f'_{i}')
        )

        # Drop duplicate rows based on 'Артикул поставщика' after each merge
        merged_df = merged_df.drop_duplicates(subset='Артикул поставщика')

    # Drop duplicate columns if any exist after merging
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Extract the sorted column names, excluding 'Артикул поставщика'
    sorted_columns = ['Артикул поставщика'] + sorted(
        [col for col in merged_df.columns if col != 'Артикул поставщика']
    )

    # Reorder the DataFrame with the sorted column list
    merged_df = merged_df[sorted_columns]

    # Perform ABC and XYZ analysis
    df_merged_dynamic = abc_xyz(merged_df)

    if is_upload_yandex and not testing_mode:
        yandex_disk_handler.upload_to_YandexDisk(file=merged_df, file_name=nameof(df_merged_dynamic),
                                                 path=app.config['REPORT_DYNAMIC'])

    # Return the final DataFrame with ABC and XYZ categories
    return df_merged_dynamic


@timing_decorator
def influence_discount_by_dynamic(df, df_dynamic, default_margin=1000, k=1):
    if df_dynamic.empty:
        return df

    dynamic_columns_names = DINAMIC_COLUMNS

    # Select relevant columns from df_dynamic
    df_dynamic = df_dynamic[["Артикул поставщика"] + dynamic_columns_names]

    # Merge df_dynamic into df
    df = pandas_handler.df_merge_drop(df, df_dynamic, "Артикул поставщика", "Артикул поставщика")

    # Calculate the number of periods to adjust Total_Margin for periodic sales
    periods_count = len([x for x in df_dynamic.columns if "Ч. Продажа шт." in x])
    medium_total_margin = df["Total_Margin"] / periods_count if periods_count > 0 else df["Total_Margin"]

    # Calculate discounts based on Total_Margin and CV
    df["ABC_discount"] = medium_total_margin / default_margin  # Adjust this to scale as needed
    df["CV_discount"] = df["CV"].apply(pandas_handler.false_to_null)
    df['ABC_CV_discount'] = k * df["ABC_discount"] / df["CV_discount"].apply(abs)
    df['ABC_CV_discount'] = df['ABC_CV_discount'].apply(pandas_handler.false_to_null)
    df['ABC_CV_discount'] = df['ABC_CV_discount'].apply(pandas_handler.inf_to_null)
    df["new_discount"] = df["new_discount"] - df['ABC_CV_discount']

    return df


def in_positive_digit(df, decimal=0, col_names=None):
    if col_names is None:  # Default empty check
        return df
    if isinstance(col_names, str):  # Handle single column name
        col_names = [col_names]

    for col in col_names:
        if col not in df.columns:  # Ensure the column exists
            continue
        # Set negative values to 0
        df[col] = df[col].apply(lambda x: max(0, x))
        # Round the values
        df[col] = df[col].round(decimal)

    return df


@timing_decorator
def get_data_from(request) -> SimpleNamespace:
    r = SimpleNamespace()
    r.days_by = int(request.form.get('days_by', app.config['DAYS_PERIOD_DEFAULT']))
    r.uploaded_files = request.files.getlist("file")
    r.files_period_days = int(len(r.uploaded_files)) * 7
    r.testing_mode = request.form.get('is_testing_mode')
    r.promo_file = request.files.get("promo_file")
    r.is_just_concatenate = 'is_just_concatenate' in request.form
    r.is_discount_template = 'is_discount_template' in request.form
    r.is_dynamic = 'is_dynamic' in request.form
    r.is_chosen_columns = 'is_chosen_columns' in request.form
    r.is_net_cost = 'is_net_cost' in request.form
    r.is_get_storage = 'is_get_storage' in request.form
    r.is_shushary = request.form.get('is_shushary')
    r.is_get_price = request.form.get('is_get_price')
    r.is_get_stock = 'is_get_stock' in request.form
    r.is_funnel = request.form.get('is_funnel')
    r.k_delta = request.form.get('k_delta', 1)
    r.is_mix_discounts = 'is_mix_discounts' in request.form
    r.reset_if_null = request.form.get('reset_if_null')
    r.is_first_df = request.form.get('is_first_df')
    r.k_delta = int(r.k_delta)

    return r


def file_names() -> SimpleNamespace:
    n = SimpleNamespace()
    n.detailing_name = "report_detailing_upload.xlsx"
    n.promo_name = "promo.xlsx"
    n.template_name = "discount_template.xlsx"
    n.df_dynamic_name = "df_dynamic.xlsx"
    return n

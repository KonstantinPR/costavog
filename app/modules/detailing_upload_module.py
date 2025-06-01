from app import app
import zipfile
import pandas as pd
import io
from app.modules import pandas_handler, yandex_disk_handler
from types import SimpleNamespace
from typing import List
from varname import nameof

from app.modules.decorators import timing_decorator
from app.modules.detailing_upload_dict_module import INITIAL_COLUMNS_DICT, DINAMIC_COLUMNS
from app.modules.dfs_dynamic_module import abc_xyz
from app.modules.dfs_process_module import choose_df_in, dfs_forming, choose_dynamic_df_list_in, dfs_from_outside, \
    concatenate_dfs
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
def promofiling(promo_file, df, allowed_delta_percent=7):
    if not promo_file:
        return pd.DataFrame()

    # Read the promo file into a DataFrame

    df_promo = pd.read_excel(promo_file)

    df_promo = pandas_handler.df_merge_drop(df_promo, df, "Артикул WB", "nmId", how='outer')
    df_promo = check_discount(df_promo, allowed_delta_percent)

    return df_promo


@timing_decorator
def dfs_process(df_list, r: SimpleNamespace) -> tuple[pd.DataFrame, List, SimpleNamespace]:
    """dfs_process..."""
    print("""dfs_process...""")

    # element 0 in list is always general df that was through all df_list
    incl_col = list(INITIAL_COLUMNS_DICT.values())

    # API and YANDEX_DISK getting data into namespace
    d = dfs_from_outside(r)

    # must be refactored into def that gets DF class that contains df (first or combined) and dfs_list for dynamics:

    df = choose_df_in(df_list, is_first_df=r.is_first_df)
    df = pandas_handler.df_merge_drop(left_df=df, right_df=d.df_delivery, left_on='Артикул поставщика',
                                      right_on='Артикул')

    df = dfs_forming(df=df, d=d, r=r, include_columns=incl_col)

    df_dynamic_list_initial = choose_dynamic_df_list_in(df_list, is_dynamic=r.is_dynamic)
    df_dynamic_list = concatenate_dfs(df_dynamic_list_initial, per=2)
    is_dynamic_possible = r.is_dynamic and len(df_dynamic_list) > 1
    df_completed_dynamic_list = [dfs_forming(x, d, r, incl_col) for x in df_dynamic_list if is_dynamic_possible]

    return df, df_completed_dynamic_list, d


@timing_decorator
def dfs_dynamic(df_dynamic_list, r: SimpleNamespace = None, by_col="Артикул поставщика"):
    """dfs_dynamic merges DataFrames on by_col and expands dynamic columns."""
    print("dfs_dynamic...")

    if r is None:
        r = SimpleNamespace(is_dynamic=False, is_upload_yandex=False, testing_mode=False)

    if not r.is_dynamic:
        return pd.DataFrame()

    if not df_dynamic_list:
        return pd.DataFrame()

    # Some useful columns to add
    additional_columns = ['prefix', 'quantityFull']

    # List of dynamic columns to analyze
    columns_dynamic = ["Маржа-себест.", "Маржа", "Ч. Продажа шт.", "Логистика", "Хранение"]

    # Initialize the merged DataFrame with the first DataFrame in the list
    merged_df = df_dynamic_list[0][[by_col] + additional_columns + columns_dynamic].copy()

    # Iterate over remaining DataFrames and merge
    for i, df in enumerate(df_dynamic_list[1:]):
        # Determine suffix
        zero = '0' if len(str(i)) <= 1 else ''
        suffix = f'_{zero}{i}'

        # Merge on by_col with suffixes for columns
        merged_df = pd.merge(
            merged_df,
            df[[by_col] + columns_dynamic],
            on=by_col,
            how='outer',  # keep all articles
            suffixes=('', suffix)
        )

        # Drop duplicate rows based on by_col after each merge
        merged_df = merged_df.drop_duplicates(subset=by_col)

    # Remove duplicated columns if any
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Rename 'Маржа' to 'Маржа_0' if it exists
    if 'Маржа' in merged_df.columns:
        merged_df = merged_df.rename(columns={'Маржа': 'Маржа_0'})

    # Prepare sorted columns: by_col first, then others sorted
    sorted_columns = [by_col] + sorted([col for col in merged_df.columns if col != by_col])

    # Reorder columns
    merged_df = merged_df[sorted_columns]

    # Perform ABC and XYZ analysis
    df_merged_dynamic = abc_xyz(merged_df, by_col=by_col)

    # Upload to Yandex Disk if needed
    if hasattr(r, 'is_upload_yandex') and r.is_upload_yandex and not getattr(r, 'testing_mode', False):
        yandex_disk_handler.upload_to_YandexDisk(
            file=df_merged_dynamic,
            file_name=nameof(df_merged_dynamic) + ".xlsx",
            path=app.config[r.path_to_save] + "/DYNAMIC_PER_ART"
        )

    return df_merged_dynamic


def merge_dynamic_by(df_merged_dynamic, by_col='prefix', r: SimpleNamespace = None):
    # Ensure r is a SimpleNamespace with default attributes if not provided
    if r is None:
        r = SimpleNamespace(is_dynamic=False, is_upload_yandex=False, testing_mode=False)

    # Return empty DataFrame if not dynamic
    if not r.is_dynamic:
        return pd.DataFrame()

    # Return empty DataFrame if input DataFrame is empty
    if df_merged_dynamic.empty:
        return pd.DataFrame()

    # List of columns to average instead of sum
    columns_to_mean = ['CV']

    # Initialize aggregation dict: sum for all except specified columns
    agg_dict = {col: 'sum' for col in df_merged_dynamic.columns if
                col not in [by_col, "Артикул поставщика", "ABC_Category", "XYZ_Category"]}

    # Override specific columns to 'mean'
    for col in columns_to_mean:
        if col in agg_dict:
            agg_dict[col] = 'mean'

    # Perform groupby with aggregation
    df_merged_dynamic_by_col = df_merged_dynamic.groupby(by_col).agg(agg_dict).reset_index()

    # Upload to Yandex Disk if conditions are met
    if hasattr(r, 'is_upload_yandex') and r.is_upload_yandex and not getattr(r, 'testing_mode', False):
        yandex_disk_handler.upload_to_YandexDisk(
            file=df_merged_dynamic_by_col,
            file_name=nameof(df_merged_dynamic_by_col) + ".xlsx",
            path=app.config[r.path_to_save] + '/DYNAMIC_PER_PRE'
        )

    return df_merged_dynamic_by_col


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
    r.path_to_save = request.form.get("path_to_save")
    r.is_just_concatenate = 'is_just_concatenate' in request.form
    r.is_discount_template = 'is_discount_template' in request.form
    r.is_dynamic = 'is_dynamic' in request.form
    r.is_chosen_columns = 'is_chosen_columns' in request.form
    r.is_net_cost = 'is_net_cost' in request.form
    r.is_get_storage = 'is_get_storage' in request.form
    r.is_shushary = request.form.get('is_shushary')
    r.is_get_price = request.form.get('is_get_price')
    r.is_get_stock = 'is_get_stock' in request.form
    r.is_from_yadisk = 'is_from_yadisk' in request.form
    r.is_archive = 'is_archive' in request.form
    r.is_save_yadisk = 'is_save_yadisk' in request.form
    r.is_upload_yandex = 'is_upload_yandex' in request.form
    r.is_funnel = request.form.get('is_funnel')
    r.k_delta = request.form.get('k_delta', 1)
    r.k_action_diff = request.form.get('k_action_diff', 7)
    r.is_mix_discounts = 'is_mix_discounts' in request.form
    r.reset_if_null = request.form.get('reset_if_null')
    r.is_first_df = request.form.get('is_first_df')
    r.is_compare_detailing = request.form.get('is_compare_detailing')
    r.k_delta = int(r.k_delta)

    return r


def file_names() -> SimpleNamespace:
    n = SimpleNamespace()
    n.detailing_name = "report_detailing_upload.xlsx"
    n.promo_name = "promo.xlsx"
    n.template_name = "discount_template.xlsx"
    n.df_dynamic_name = "df_dynamic.xlsx"
    return n


@timing_decorator
def mix_detailings(df, is_compare_detailing=""):
    """mix_detailings..."""
    print("""mix_detailings...""")

    if not is_compare_detailing:
        return df

    # Fetch DataFrames from Yandex Disk
    try:
        df_ALL_LONG = yandex_disk_handler.get_excel_file_from_ydisk(app.config['REPORT_DETAILING_UPLOAD_ALL'])
        df_LONG = yandex_disk_handler.get_excel_file_from_ydisk(app.config['REPORT_DETAILING_UPLOAD_LONG'])

        # Merge the first DataFrame
        df = df.merge(df_ALL_LONG[['Артикул поставщика', 'Маржа-себест.']],
                      on='Артикул поставщика',
                      how='left',
                      suffixes=('', '_ALL_LONG'))

        # Merge the second DataFrame
        df = df.merge(df_LONG[['Артикул поставщика', 'Маржа-себест.']],
                      on='Артикул поставщика',
                      how='left',
                      suffixes=('', '_LONG'))


    except Exception as e:
        print(f"An error occurred: {e}")  # Log the error instead of printing

    return df  # Return the original df if an error occurs

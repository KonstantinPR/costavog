import numpy as np
from app import app
import zipfile
import pandas as pd
import io
from app.modules import pandas_handler, yandex_disk_handler, API_WB, promofiling_module
from types import SimpleNamespace
from typing import List
from varname import nameof

from app.modules.decorators import timing_decorator
from app.modules.detailing_upload_dict_module import INITIAL_COLUMNS_DICT, DELIVERY_COLUMNS, DINAMIC_COLUMNS
from app.modules.dfs_dynamic_module import abc_xyz
from app.modules.dfs_process_module import choose_df_in, dfs_forming, choose_dynamic_df_list_in, dfs_from_outside, \
    concatenate_dfs

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


def min_price(df, pow_k=0.5, k=80, col_min="Новая минимальная цена для применения скидки по автоакции",
              col_price="net_cost"):
    """Calculate and return a subset DataFrame with updated min prices, without modifying the original."""
    # List of desired columns
    columns = [
        "Бренд", "Категория", "Артикул WB", "Артикул продавца", "Последний баркод",
        "Остатки WB", "Остатки продавца", "Оборачиваемость", "Цена со скидкой",
        "Текущая минимальная цена для применения скидки по автоакции",
        "Новая минимальная цена для применения скидки по автоакции",
        "Текущая блокировка применения скидки по автоакции",
        "Новая блокировка применения скидки по автоакции"
    ]

    # Проверка существования колонок
    if col_price not in df.columns:
        raise KeyError(f"Column '{col_price}' not found in DataFrame.")

    if 'nmId' not in df.columns:
        raise KeyError("Column 'nmId' not found in DataFrame.")

    # Обрабатываем нулевые цены
    df[col_price] = df[col_price].replace(0, np.nan)

    # Расчет минимальной цены:
    # 2000 3578
    # 1000 2530
    # 500 1789
    # 100 800
    df[col_min] = (df[col_price] ** pow_k) * k
    df[col_min] = df[col_min].round(0)

    # Создаем DataFrame с Артикул WB
    df_temp_min = pd.DataFrame({"Артикул WB": df["nmId"]})

    # Мержим
    df_temp_min = df_temp_min.merge(df, left_on="Артикул WB", right_on="nmId", how='left')

    # Убедимся, что все нужные колонки есть, и заполняем отсутствующие пустыми строками
    for col in columns:
        if col not in df_temp_min.columns:
            df_temp_min[col] = ''

    # Выбираем только нужные колонки
    df_temp_min = df_temp_min[columns]
    df_temp_min.replace('', np.nan, inplace=True)
    df_temp_min = df_temp_min.dropna(how='all')
    df_temp_min.replace(np.nan, '', inplace=True)

    return df_temp_min


@timing_decorator
def promofiling(promo_file, df, allowed_delta_percent=7):
    if not promo_file:
        return pd.DataFrame()

    # Read the promo file into a DataFrame

    df_promo = pd.read_excel(promo_file)

    df_promo = pandas_handler.df_merge_drop(df_promo, df, "Артикул WB", "nmId", how='outer')
    df_promo = promofiling_module.check_discount(df_promo, allowed_delta_percent)

    return df_promo


@timing_decorator
def dfs_process(df_list, r: SimpleNamespace) -> tuple[pd.DataFrame, List, SimpleNamespace]:
    """dfs_process..."""
    print("""dfs_process...""")

    # element 0 in list is always general df that was through all df_list
    incl_col = list(INITIAL_COLUMNS_DICT.values()) + DELIVERY_COLUMNS

    # API and YANDEX_DISK getting data into namespace
    d = dfs_from_outside(r)

    # must be refactored into def that gets DF class that contains df (first or combined) and dfs_list for dynamics:

    df = choose_df_in(df_list, is_first_df=r.is_first_df)

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
    additional_columns = ['prefix', 'nmId', 'quantityFull']

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
    # merged_df.to_excel('merged_df.xlsx')
    # Perform ABC and XYZ analysis
    df_merged_dynamic = abc_xyz(merged_df, prefix='Маржа-себест.', trend_name='ABC')
    df_merged_dynamic = abc_xyz(df_merged_dynamic, prefix='Ч. Продажа шт.', trend_name='XYZ')
    df_merged_dynamic['ABC_XYZ'] = (df_merged_dynamic['ABC'] + df_merged_dynamic['XYZ']) / 2
    df_merged_dynamic['ABC_XYZ'] = df_merged_dynamic['ABC_XYZ'].round(4)
    # df_merged_dynamic.to_excel('df_merged_dynamic.xlsx')

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
        r = SimpleNamespace(is_dynamic=False, is_upload_yandex=False, testing_mode=False, path_to_save='YOUR_PATH')

    # Return empty DataFrame if not dynamic
    if not r.is_dynamic:
        return pd.DataFrame()

    # Return empty DataFrame if input DataFrame is empty
    if df_merged_dynamic.empty:
        return pd.DataFrame()

    # Calculate total columns based on prefixes
    sum_col = ['Маржа-себест.', 'Логистика', 'Маржа_', 'Ч. Продажа шт.', 'Хранение']
    columns_to_sum = [col for col in df_merged_dynamic.columns if any(col.startswith(prefix) for prefix in sum_col)]
    # print(f"columns_to_sum {columns_to_sum}")

    for total_col in sum_col:
        if columns_to_sum:
            df_merged_dynamic[f'{total_col}Total'] = df_merged_dynamic[
                [col for col in columns_to_sum if col.startswith(total_col)]].sum(axis=1)
        else:
            # Optionally, handle case where no columns found
            df_merged_dynamic[f'{total_col}Total'] = 0

    # Initialize aggregation dict
    agg_dict = {}
    for col in df_merged_dynamic.columns:
        if col in [by_col, 'Артикул поставщика', 'nmId', 'quantityFull']:
            continue
        # Default to 'mean'
        agg_dict[col] = 'mean'

    # Set total columns to sum
    add_total_col = [f'{total_col}Total' for total_col in sum_col]
    for col in columns_to_sum + add_total_col:
        if col in agg_dict:
            agg_dict[col] = 'sum'

    # Perform groupby with aggregation
    df_merged_dynamic_by_col = df_merged_dynamic.groupby(by_col).agg(agg_dict).reset_index()

    # Upload to Yandex Disk if conditions are met
    if hasattr(r, 'is_upload_yandex') and r.is_upload_yandex and not getattr(r, 'testing_mode', False):
        # Generate filename
        filename = "merged_dynamic_" + pd.Timestamp.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"
        # Compose path
        save_path = app.config[r.path_to_save] + '/DYNAMIC_PER_PRE'
        # Upload
        yandex_disk_handler.upload_to_YandexDisk(
            file=df_merged_dynamic_by_col,
            file_name=filename,
            path=save_path
        )

    return df_merged_dynamic_by_col


@timing_decorator
def influence_discount_by_dynamic(df, df_dynamic, k_influence=2):
    if df_dynamic.empty:
        return df

    dynamic_columns_names = DINAMIC_COLUMNS

    df_dynamic = df_dynamic[
        ["Артикул поставщика"] + dynamic_columns_names + [col for col in df_dynamic.columns if 'Total' in col]]

    # Merge df_dynamic into df
    df = pandas_handler.df_merge_drop(df, df_dynamic, "Артикул поставщика", "Артикул поставщика")

    # Calculate the number of periods to adjust Total_Margin for periodic sales
    # periods_count = len([x for x in df_dynamic.columns if "Ч. Продажа шт." in x])
    # medium_total_margin = df["Total_Margin"] / periods_count if periods_count > 0 else df["Total_Margin"]
    # df.to_excel("d_disc_no.xlsx")
    if not "d_disc" in df.columns: df['d_disc'] = round(df['discount'])
    df["ABC_XYZ_delta"] = df["d_disc"] * df['ABC_XYZ'] / k_influence
    df["ABC_XYZ_delta"] = df["ABC_XYZ_delta"].round(4)
    df["new_discount"] = df["new_discount"] - df["ABC_XYZ_delta"]
    df["new_discount"] = df["new_discount"].apply(lambda x: round(x, 0))
    df['new_price'] = round(df['price'] * (1 - df['new_discount'] / 100))

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
        df_2025 = yandex_disk_handler.get_excel_file_from_ydisk(app.config['REPORT_DETAILING_UPLOAD_2025'])
        df_2024 = yandex_disk_handler.get_excel_file_from_ydisk(app.config['REPORT_DETAILING_UPLOAD_2024'])
        df_2023 = yandex_disk_handler.get_excel_file_from_ydisk(app.config['REPORT_DETAILING_UPLOAD_2023'])

        # Merge the first DataFrame
        df = df.merge(df_ALL_LONG[['Артикул поставщика', 'Маржа-себест.']],
                      on='Артикул поставщика',
                      how='left',
                      suffixes=('', '_ALL_LONG'))
        # Merge the second DataFrame
        df = df.merge(df_2025[['Артикул поставщика', 'Маржа-себест.']],
                      on='Артикул поставщика',
                      how='left',
                      suffixes=('', '_2025'))
        df = df.merge(df_2024[['Артикул поставщика', 'Маржа-себест.']],
                      on='Артикул поставщика',
                      how='left',
                      suffixes=('', '_2024'))
        df = df.merge(df_2023[['Артикул поставщика', 'Маржа-себест.']],
                      on='Артикул поставщика',
                      how='left',
                      suffixes=('', '_2023'))


    except Exception as e:
        print(f"An error occurred: {e}")  # Log the error instead of printing

    return df  # Return the original df if an error occurs


def df_disc_template_create(df, df_promo, is_discount_template=False, default_discount=5, is_from_yadisk=True):
    if not is_discount_template:
        return pd.DataFrame

    # if df_promo.empty:
    #     return pd.DataFrame

    # Fetch all cards and extract unique nmID values
    df_all_cards = API_WB.get_all_cards_api_wb(is_from_yadisk=is_from_yadisk)
    unique_nmID_values = df_all_cards["nmID"].unique()

    # Define the columns for the discount template
    df_disc_template_columns = [
        "Бренд", "Категория", "Артикул WB", "Артикул продавца",
        "Последний баркод", "Остатки WB", "Остатки продавца",
        "Оборачиваемость", "Текущая цена", "Новая цена",
        "Текущая скидка", "Новая скидка",
    ]

    # Create an empty DataFrame with the specified columns
    df_disc_template = pd.DataFrame(columns=df_disc_template_columns)

    # Populate "Артикул WB" with unique nmID values
    df_disc_template["Артикул WB"] = unique_nmID_values

    # If promo DataFrame is provided, merge and update "Новая скидка" by "new_discount" from df_promo
    if df_promo.empty:
        df_disc_template = pandas_handler.df_merge_drop(df_disc_template, df, "Артикул WB", "nmId", how='outer')
    else:
        df_disc_template = pandas_handler.df_merge_drop(df_disc_template, df_promo, "Артикул WB", "Артикул WB",
                                                        how='outer')

    df_disc_template["Новая скидка"] = df_disc_template["new_discount"]

    # Ensure "Новая скидка" is filled with default_discount where NaN
    df_disc_template["Новая скидка"] = df_disc_template["Новая скидка"].fillna(default_discount)
    df_disc_template["Новая скидка"] = df_disc_template["Новая скидка"].replace([np.inf, -np.inf], 0)

    df_disc_template = df_disc_template.drop_duplicates(subset=["Артикул WB"])

    # Create a mask for values not in FALSE_LIST
    mask = ~df_disc_template["Артикул WB"].isin(pandas_handler.FALSE_LIST)

    # Update the column for the rows that match the mask
    df_disc_template.loc[mask, "Артикул WB"] = df_disc_template.loc[mask, "Артикул WB"]

    # After populating df_disc_template
    df_disc_template = df_disc_template.loc[df_disc_template["Артикул WB"].str.strip() != ""]
    df_disc_template = df_disc_template.dropna(subset=["Артикул WB"])

    # Return the template DataFrame with the correct columns
    return df_disc_template[df_disc_template_columns]


def create_excel_from_df_and_list(df, list_of_dfs, output_path="output.xlsx"):
    """
    Создает Excel-файл, где:
      - Sheet1 содержит DataFrame df
      - Остальные листы содержат DataFrame из list_of_dfs

    Args:
      df: pandas DataFrame, который будет записан на Sheet1.
      list_of_dfs: Список pandas DataFrame, каждый из которых будет записан на отдельный лист.
      output_path: Путь для сохранения Excel-файла. По умолчанию "output.xlsx".
    """

    with pd.ExcelWriter(output_path) as writer:
        # Записываем основной DataFrame на Sheet1
        df.to_excel(writer, sheet_name='Sheet1', index=False)  # index=False для исключения индекса из записи

        # Записываем каждый DataFrame из списка на отдельные листы
        for i, df_in_list in enumerate(list_of_dfs):
            sheet_name = f'Sheet{i + 2}'  # Называем листы Sheet2, Sheet3 и т.д.
            df_in_list.to_excel(writer, sheet_name=sheet_name, index=False)


def remain_only_columns(cols, dfs):
    # Convert cols to list if it's not already
    columns_to_keep = list(cols)
    dfs_out = []

    # Filter each DataFrame
    for df in dfs:
        # Keep only columns that are in the columns_to_keep list
        df_filtered = df.loc[:, df.columns.intersection(columns_to_keep)]
        dfs_out.append(df_filtered)

    return dfs_out

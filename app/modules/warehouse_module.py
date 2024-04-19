import logging
from app import app
import inspect
import glob
import pandas as pd



def get_list_paths_files(path_to_files_glob: str,
                         file_names: list[str] = [app.config['ARRIVAL_FILE_NAMES']],
                         file_extension: str = app.config['EXTENSION_EXCEL'],
                         ):
    print(f"{inspect.stack()[0].function} ... in processing")
    list_paths_files = []
    for file_name in file_names:
        path_files = f"{path_to_files_glob}{file_name}{file_extension}"
        list_paths_files.extend(glob.glob(path_files, recursive=True))
    return list_paths_files


def df_from_list_paths_excel_files(list_paths_files, col_name_from_path=True):
    print(f"{inspect.stack()[0].function} ... in processing")
    excel_dfs = []
    for file in list_paths_files:
        # check if the file name contains what you need
        if not file.startswith('.') and not file.startswith('~$'):
            df = pd.read_excel(file)
            if col_name_from_path:
                df = add_col_to_df_by_path(df, file)
            excel_dfs.append(df)
    return excel_dfs


def add_col_to_df_by_path(df, file_path):
    file_path_split = file_path.split("\\")
    len_file_path_list = len(file_path_split)
    df["file_path"] = file_path
    for i in range(len_file_path_list):
        df[f"Column {i}"] = file_path_split[i]

    return df

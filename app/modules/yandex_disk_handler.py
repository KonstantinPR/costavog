import pandas as pd
import yadisk
from app import app
from io import BytesIO
from random import randrange
import shutil
import os
from app.modules import io_output
import copy


def upload_to_yandex_disk(file: BytesIO, file_name: str):
    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
    path_full_to = f"{app.config['YANDEX_KEY_FILES_PATH']}/{file_name}"
    print(path_full_to)
    y.upload(file, path_full_to, overwrite=True)

    return None


def download_from_yandex_disk():
    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
    path_yandex_file = f"{list(y.listdir(app.config['YANDEX_KEY_FILES_PATH']))[-1]['path']}".replace('disk:', '')
    file_name = os.path.basename(os.path.normpath(path_yandex_file))
    bytes_io = BytesIO()
    y.download(path_yandex_file, bytes_io)
    file_content = pd.read_excel(bytes_io)
    print(type(file_content))

    return file_content, file_name


# def _get_dict_images(y, path, final_path: list = []):
#     for sub_path in path:
#         sub_path
#
#         exit()
#
#     # print(path)
#     # parent_path = path
#     # path = path['path'].replace('disk:', '')
#     # print(path)
#     # if y.is_dir(path):
#     #     return _get_dict_images(y, list(y.listdir(path)))
#     #
#     # for file in parent_path['path'].replace('disk:', ''):
#     #     images[file] = file.split("/")[-1]
#
#     return images


# def run_fast_scandir(y, dir, ext):  # dir: str, ext: list
#     subfolders, files = [], []
#
#     for f in os.scandir(dir):
#         if y.is_dir(f):
#             subfolders.append(f.path)
#         if y.is_file(f):
#             if os.path.splitext(f.name)[1].lower() in ext:
#                 files.append(f.path)
#
#     for dir in list(subfolders):
#         sf, f = run_fast_scandir(dir, ext)
#         subfolders.extend(sf)
#         files.extend(f)
#     return subfolders, files

list_path = []


def run_fast_scandir(y, dir, ext):  # dir: str, ext: list
    """
    https://stackoverflow.com/questions/18394147/how-to-do-a-recursive-sub-folder-search-and-return-files-in-a-list

    :param y:
    :param dir:
    :param ext:
    :return:
    """
    subfolders, files = [], []

    y_paths = [x['path'].replace('disk:', '') for x in list(y.listdir(dir))]
    print(y_paths)

    for f in y_paths:
        if y.is_dir(f):
            subfolders.append(f)
        if y.is_file(f):
            if os.path.splitext(f)[1].lower() in ext:
                files.append(f)

    for dir in list(subfolders):
        sf, f = run_fast_scandir(y, dir, ext)
        subfolders.extend(sf)
        files.extend(f)
    return subfolders, files


def download_images_from_yandex_disk():
    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])

    subfolders, files = run_fast_scandir(y, app.config['YANDEX_FOLDER_IMAGE_YANDISK'], '.jpg')

    print(subfolders)
    print(files)

    # images = _get_dict_images(y, full_path_yandex_files)
    # print(images)

    exit()

    path_yandex_file = f"{list(y.listdir(app.config['YANDEX_FOLDER_IMAGE_YANDISK']))[-1]['path']}".replace('disk:', '')
    file_name = os.path.basename(os.path.normpath(path_yandex_file))
    print(file_name)
    # bytes_io = BytesIO()
    # y.download(path_yandex_file, bytes_io)
    # file_content = pd.read_excel(bytes_io)
    # print(type(file_content))
    return None
    # return file_content, file_name

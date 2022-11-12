import pandas as pd
import yadisk
from app import app
from io import BytesIO
from random import randrange
import shutil
import os
from app.modules import io_output
import copy
from typing import Union


def get_excel_file_from_ydisk(path: str, to_str=None) -> pd.DataFrame:
    if to_str is None:
        to_str = []
    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
    path_yandex_file = f"{list(y.listdir(path))[-1]['path']}".replace('disk:', '')
    print(f'ya_path {path_yandex_file}')
    # file_name = os.path.basename(os.path.normpath(path_yandex_file))
    bytes_io = BytesIO()
    y.download(path_yandex_file, bytes_io)
    file_content = pd.read_excel(bytes_io, converters={x: str for x in to_str})
    return file_content


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


# def yadisk_get_files():
#     y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
#     all_files = y.listdir(path="/ФОТОГРАФИИ/НОВЫЕ/2/Часть 122")
#     img_names = 'SK-KR34-MUTON-1.JPG'
#     print(all_files)
#     for idx, file in enumerate(all_files):
#         print(file)
#         ya_name = file['path'].replace('disk:', '')
#         print(ya_name)
#         if img_names in ya_name:
#             print(f'img_name in ya_name {ya_name}')
#             bytes_io = BytesIO()
#             img_io = y.download(ya_name, bytes_io)
#             print(f'img_io {img_io}')
#             img = io_output.io_img_output(img_io)
#
#             return img


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

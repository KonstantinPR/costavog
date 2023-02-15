import zipfile

import pandas as pd
import requests
import yadisk
from flask_login import current_user
from app import app, Company
from io import BytesIO
import os


def get_excel_file_from_ydisk(path: str, to_str=None) -> pd.DataFrame:
    if to_str is None:
        to_str = []
    y = yadisk.YaDisk(token=Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token)
    path_yandex_file = f"{list(y.listdir(path))[-1]['path']}".replace('disk:', '')
    print(f'ya_path {path_yandex_file}')
    # file_name = os.path.basename(os.path.normpath(path_yandex_file))
    bytes_io = BytesIO()
    y.download(path_yandex_file, bytes_io)
    file_content = pd.read_excel(bytes_io, converters={x: str for x in to_str})
    return file_content


def upload_to_yandex_disk(file: BytesIO, file_name: str, app_config_path=app.config['YANDEX_KEY_FILES_PATH']):
    y = yadisk.YaDisk(token=Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token)
    path_full_to = f"{app_config_path}/{file_name}"
    print(path_full_to)
    y.upload(file, path_full_to, overwrite=True)

    return None


def download_from_yandex_disk():
    y = yadisk.YaDisk(token=Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token)
    path_yandex_file = f"{list(y.listdir(app.config['YANDEX_KEY_FILES_PATH']))[-1]['path']}".replace('disk:', '')
    file_name = os.path.basename(os.path.normpath(path_yandex_file))
    bytes_io = BytesIO()
    y.download(path_yandex_file, bytes_io)
    file_content = pd.read_excel(bytes_io)
    print(type(file_content))

    return file_content, file_name


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
    y = yadisk.YaDisk(token=Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token)

    subfolders, files = run_fast_scandir(y, app.config['YANDEX_FOLDER_IMAGE_YANDISK'], '.jpg')
    print(subfolders)
    print(files)
    exit()

    path_yandex_file = f"{list(y.listdir(app.config['YANDEX_FOLDER_IMAGE_YANDISK']))[-1]['path']}".replace('disk:', '')
    file_name = os.path.basename(os.path.normpath(path_yandex_file))
    print(file_name)

    return None


def get_urls(dir_path, files_path: list):
    # Get the URLs of the images
    file_urls = []
    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
    for idx, file_name in enumerate(files_path):
        file_path = os.path.join(dir_path, file_name).replace("\\", "/")
        try:
            img_url = y.get_download_link(file_path)
            file_urls.append(img_url)
        except yadisk.exceptions.NotFoundError:
            print(f"File {file_path} not found on Yandex.Disk, skipping")
    return file_urls


def zip_buffer_files(file_urls, file_name_list):
    # Download the images and save them to an in-memory buffer
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for file_url, file_name in zip(file_urls, file_name_list):
            response = requests.get(file_url)
            zip_file.writestr(file_name, response.content)
    return zip_buffer

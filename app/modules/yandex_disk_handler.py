import zipfile
import flask
import pandas as pd
import requests
import yadisk
from flask_login import current_user
from app import app, Company
from io import BytesIO
import os
from app.modules import io_output
import datetime
from app.modules.decorators import timing_decorator
from typing import Union


def copy_file_to_archive_folder(request=None, path_or_config=None, archive_folder_name='ARCHIVE',
                                is_archive=True, testing_mode=False):
    if testing_mode:
        return None

    if not is_archive:
        return None

    if request:
        if 'is_archive' not in request.form:
            return None

    if not path_or_config:
        print("NO FILE path provided !!!")
        return None

    print(f"copy_file_to_archive_folder in {path_or_config}...")

    file_content, file_name = download_from_YandexDisk(path=path_or_config)

    if not file_name:
        print(f"NO FILE in {path_or_config} in yadisk !!!")
        return None

    file_name, file_extension = os.path.splitext(file_name)
    file_name = f"{file_name}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}{file_extension}"
    file_path_archive = f"{path_or_config}/{archive_folder_name}"
    print(f"file_path_archive {file_path_archive}")
    upload_to_YandexDisk(file=file_content, file_name=file_name, path=file_path_archive)
    print(f"File '{file_name}' copied to archive successfully.")

    return None


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


@timing_decorator
def upload_to_YandexDisk(request: flask.Request = None,
                         file: Union[pd.DataFrame, bytes] = pd.DataFrame,
                         file_name: str = 'filename',
                         path: str = app.config['YANDEX_KEY_FILES_PATH'], is_to_yadisk=True,
                         testing_mode=False,
                         is_add_timestamp=False,
                         is_upload=True
                         ):
    if not is_upload:
        return None

    if testing_mode:
        return None

    if request and 'is_to_yadisk' not in request.form:
        if not is_to_yadisk:
            return None

    if not is_to_yadisk:
        return None

    if not isinstance(file, BytesIO):
        file = io_output.io_output(file)

    if not file:
        print(f"file {file} can't be uploaded to yadisk by path {path}")
        return None

    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])

    # print(f"y.is_dir(path) {y.is_dir(path)}")

    if not y.is_dir(path):
        print(f"y.is_dir(path) is {y.is_dir(path)} here {path}")
        print(f"no {path} in yadisk. Creating dir ...")
        y.mkdir(path)

    if is_add_timestamp:
        # Get the current time as a formatted string, e.g., "20241028_153000"
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"zip_file_{timestamp}.zip"  # Assuming zip_name should end with .zip

    path_full_to = f"{path}/{file_name}"
    print(f"file upload to the yandex.disk in {path_full_to} ...")

    y.upload(file, path_full_to, overwrite=True)

    return None


def download_from_YandexDisk(path='YANDEX_KEY_FILES_PATH', is_from_yadisk=True, testing_mode=False):
    if is_from_yadisk or testing_mode:

        if not path.startswith("/"):
            path = app.config[path]

        print(f"file downloaded from {path} ...")
        y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
        path_yandex_file = f"{list(y.listdir(path))[-1]['path']}".replace('disk:', '')
        file_name = os.path.basename(os.path.normpath(path_yandex_file))
        if not y.is_file(f"{path}/{file_name}"):
            print(f"NO FILE in {path} on yadisk")
            return None, None
        bytes_io = BytesIO()
        y.download(path_yandex_file, bytes_io)
        file_content = pd.read_excel(bytes_io)
        # print(type(file_content))
        return file_content, file_name
    return None, None


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


def download_images_from_YandexDisk():
    y = yadisk.YaDisk(token=Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token)

    subfolders, files = run_fast_scandir(y, app.config['YANDEX_FOLDER_IMAGE_YANDISK'], '.jpg')
    # print(subfolders)
    # print(files)
    exit()

    path_yandex_file = f"{list(y.listdir(app.config['YANDEX_FOLDER_IMAGE_YANDISK']))[-1]['path']}".replace('disk:', '')
    file_name = os.path.basename(os.path.normpath(path_yandex_file))
    # print(file_name)

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
            print(f"File {file_path} not found on YandexDisk, skipping")
    return file_urls


def zip_buffer_files(file_urls, file_name_list) -> BytesIO:
    # Download the images and save them to an in-memory buffer
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        for file_url, file_name in zip(file_urls, file_name_list):
            response = requests.get(file_url)
            zip_file.writestr(file_name, response.content)
    return zip_buffer


def get_subfolders_names(dir_path):
    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
    subfolder_names = []
    for item in y.listdir(dir_path):
        if item.type == 'dir':
            subfolder_names.append(item.name)
    print(f"subfolder_names {subfolder_names}")
    return subfolder_names


def get_all_file_urls(subfolder_names, file_name_list, dir_path):
    all_file_urls = []
    found_files = set()  # to keep track of found files
    file_name_copy = file_name_list.copy()  # make a copy of the list
    for sub in reversed(subfolder_names):
        if not file_name_copy:  # all files have been found, break out of the loop
            break
        path = os.path.join(dir_path, sub).replace("\\", "/")
        file_urls = get_urls(path, file_name_copy)
        all_file_urls += file_urls
        for url, file_name in zip(file_urls, file_name_copy):
            if file_name not in found_files and url is not None:  # file not already found and exists
                found_files.add(file_name)
                file_name_copy.remove(file_name)  # remove the found file from the copy
    if len(found_files) != len(file_name_list):
        print(f"Warning: Some files were not found: {set(file_name_list) - found_files}")
    return all_file_urls

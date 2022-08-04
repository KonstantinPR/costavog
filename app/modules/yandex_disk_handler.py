import pandas as pd
import yadisk
from app import app
from io import BytesIO
from random import randrange
import shutil
import os
from app.modules import io_output


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

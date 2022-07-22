import yadisk
from app import app
from random import randrange
import os
import shutil


def upload_to_yandex_disk(file, file_name):
    y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
    # for creating unique folder
    id_folder = randrange(1000000)
    file_path = 'tmp_img_' + str(id_folder)

    if not os.path.exists(file_path):
        os.makedirs(file_path)
    path_full_from = f'{file_path}/{file_name}'
    path_full_to = f"TASKER/{app.config['KEY_FILES']}"
    y.upload(path_full_from, path_full_to)

    # deleting directory with file
    try:
        shutil.rmtree(file_path)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

    return "file was added"

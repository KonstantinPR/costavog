from app import app
from flask import render_template, request, redirect, send_file, flash, abort
from flask_login import login_required, current_user
from app.models import Product, db
import datetime
import pandas as pd
from app.modules import detailing, detailing_reports, yandex_disk_handler
from app.modules import io_output
import time
import numpy as np
import os
import shutil
import re
import string
import fileinput
from werkzeug.datastructures import FileStorage
import os
from flask import send_file
import io
from app.modules import yandex_disk_handler
from PIL import Image, ImageDraw, ImageFont

size_translate = {
    "m1": "150 x 100 см.",
    "m2": "150 x 200 см.",
    "m3": "150 x 300 см.",
}


def img_watermark(img_name, name):
    print(f"img_name {img_name}")
    print(f"name {name}")
    size = name[len(name) - 2:].lower()
    print(f"size {size}")
    size_text = size_translate[size]
    print(f"size_text {size_text}")
    base = Image.open(img_name).convert('RGBA')
    width, height = base.size

    # make a blank image for the text, initialized to transparent text color
    txt = Image.new('RGBA', base.size, (255, 255, 255, 0))

    fontsize = 1  # starting font size

    # portion of image width you want text width to be
    img_fraction = 0.90
    text = size_text
    font = ImageFont.truetype("arial.ttf", fontsize)
    while font.getsize(text)[0] < img_fraction * base.size[0]:
        # iterate until the text size is just larger than the criteria
        fontsize += 1
        font = ImageFont.truetype("arial.ttf", fontsize)

    # optionally de-increment to be sure it is less than criteria
    fontsize -= 10

    # get a font
    fnt = ImageFont.truetype('arial.ttf', fontsize)
    # get a drawing context
    d = ImageDraw.Draw(txt)

    x = width * (1 - img_fraction)
    y = height - fontsize * 2

    # draw text, half opacity
    d.text((x, y), text, font=fnt, fill=(255, 255, 255, 200))
    txt = txt.rotate(0)

    out = Image.alpha_composite(base, txt)
    out.convert('RGB').save(img_name, format="JPEG")


def img_foldering(df):
    print(f"file_txt {df}")

    images_folder = app.config['YANDEX_FOLDER_IMAGE']
    folder_folders = "folder_img"

    select = request.form.get('multiply_number')
    typeWB_OZON = select
    typeWB_OZON = 0 if typeWB_OZON == 'WB' else 1

    shutil.rmtree(folder_folders, ignore_errors=True)

    if not os.path.exists(folder_folders):
        os.makedirs(folder_folders)

    if typeWB_OZON == 0:

        img_name_list_files = {}

        for entry in os.scandir(images_folder):
            for subentry in os.scandir(entry.path):
                if subentry.is_dir():
                    for file in os.scandir(subentry.path):
                        if file.is_file():
                            img_name_list_files[file.name] = subentry.path

        # print(f"files {img_name_list_files}")

        for i in df['Article']:
            os.makedirs(f"{folder_folders}/{i}/photo")

        val = df['Article'].values[0]
        # print(f"Article_by_index {val}")

        for name, path in img_name_list_files.items():
            # name_clear = re.sub(r'(-9)?-\d.JPG', '', name)
            name_clear = re.sub(r'-(\d)?\d.JPG', '', name)
            for j in os.listdir(folder_folders):
                j_clear = j
                if j.startswith("EVS") or j.startswith("WLP") or j.endswith("new"):
                    j_clear = j[:(len(j) - 3)]
                    j_clear_end = j[(len(j) - 3):]
                    # print(f"j_clear_end {j_clear_end}")
                if name_clear == j_clear:
                    if typeWB_OZON == 0:
                        shutil.copyfile(f"{img_name_list_files[name]}/{name}", f"{folder_folders}/{j}/photo/{name}")
                        if j.startswith("WLP") and j.endswith("-1.JPG"):
                            img_watermark(f"{folder_folders}/{j}/photo/{name}", j)
                    if typeWB_OZON == 1:
                        shutil.copyfile(f"{img_name_list_files[name]}/{name}", f"{folder_folders}/{name}")

        for j in os.listdir(folder_folders):
            for d in range(len(df.index)):
                if df['Article'][d] == j:
                    os.rename(f"{folder_folders}/{j}", f"{folder_folders}/{df['Article_WB'][d]}")

        shutil.make_archive(folder_folders, 'zip', f"{folder_folders}")
        shutil.move(f"{folder_folders}.zip", folder_folders)

        zip_file = os.path.abspath(f"{folder_folders}\{folder_folders}.zip")

        return_data = io.BytesIO()
        with open(zip_file, 'rb') as file:
            return_data.write(file.read())
        # (after writing, cursor will be at last byte, so move it to start)
        return_data.seek(0)

        shutil.rmtree(folder_folders, ignore_errors=True)

        print(f"zip_file_path {return_data}")

        return return_data

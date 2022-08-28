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


# Make folders for wb photo that way:
# we place in txt file 2 columns article our and article wb with sep = " "
# in folder img must be img with number 1...2...4 ect
# place it all in folder "folder_img"

# in txt for ozon is only art without size
# for ozon the name of photo must be from 0 to 9 on the end for example JBG-8954-AB-0.JPG, JBG-8954-AB-1.JPG...


@app.route('/path')
def dir_listing():
    BASE_DIR = 'C:\Yandex.Disk\ФОТОГРАФИИ\НОВЫЕ\\2\Часть 102 Сапоги'

    # Joining the base and the requested path
    abs_path = os.path.join(BASE_DIR)
    print(abs_path)
    #
    # # Return 404 if path doesn't exist
    # if not os.path.exists(abs_path):
    #     return abort(404)
    #
    # # Check if path is a file and serve
    # if os.path.isfile(abs_path):
    #     return send_file(abs_path)
    #
    # # Show directory contents
    # files = os.listdir(abs_path)
    # print(files)
    return abs_path


@app.route('/watermark')
def watermark():
    base = Image.open('NO8B9709.JPG').convert('RGBA')
    width, height = base.size

    # make a blank image for the text, initialized to transparent text color
    txt = Image.new('RGBA', base.size, (255, 255, 255, 0))

    fontsize = 1  # starting font size

    # portion of image width you want text width to be
    img_fraction = 0.50
    text = "150 x 200 см."
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

    x = width / 2
    y = height - fontsize * 2

    # draw text, half opacity
    d.text((x, y), text, font=fnt, fill=(255, 255, 255, 200))
    txt = txt.rotate(0)

    out = Image.alpha_composite(base, txt)
    out.show()


@app.route('/images_foldering_yandisk', methods=['POST', 'GET'])
@login_required
def images_foldering_yandisk():
    yandex_disk_handler.download_images_from_yandex_disk()
    return render_template('upload_images_foldering.html')


@app.route('/images_foldering', methods=['POST', 'GET'])
@login_required
def images_foldering():
    """Get images from local yandex disk, preparing and foldering it on wb and ozon demand, and send it in zip
    on 08.08.2022 work only on local comp with pointing dict where img placed
    header in txt no need
    """
    if request.method == 'POST':
        file_txt: FileStorage = request.files['file']
        df = pd.read_csv(file_txt, sep='	', names=['Article', 'Article_WB'])

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

            print(f"files {img_name_list_files}")

            for i in df['Article']:
                os.makedirs(f"{folder_folders}/{i}/photo")

            val = df['Article'].values[0]
            print(f"Article_by_index {val}")

            for name, path in img_name_list_files.items():
                name_clear = re.sub(r'(-9)?-\d.JPG', '', name)
                for j in os.listdir(folder_folders):
                    j_clear = j
                    if j.startswith("EVS") or j.startswith("WLP") or j.endswith("new"):
                        j_clear = j[:(len(j) - 3)]
                    if name_clear == j_clear:
                        if typeWB_OZON == 0:
                            shutil.copyfile(f"{img_name_list_files[name]}/{name}", f"{folder_folders}/{j}/photo/{name}")
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

            return send_file(return_data, as_attachment=True, attachment_filename='image_zip.zip')

        exit()
        # HERE I STAY ON 08.08.2022

        if typeWB_OZON == 1:
            # txt include only one column article without header name

            list_art = []
            for line in fileinput.input(files=[file_txt]):
                list_art.append(line.strip())
            print(list_art)

            for root, dirs, files in os.walk(images_folder):
                for j in files:
                    j_name = re.sub(r'(-9)?-\d.JPG', '', j)
                    if j_name in list_art:
                        if "-1.JPG" in j:
                            j_re = re.sub(r'(-9)?-\d.JPG', '.JPG', j)
                        else:
                            j_re_start = re.sub(r'(-9)?-\d.JPG', '', j)
                            j_re_end = j[len(j) - 6: len(j)].replace("-", "_")
                            j_re = j_re_start + j_re_end

                        shutil.copyfile(f"{images_folder}/{j}", f"{folder_folders}/{j_re}")
                exit()

    return render_template('upload_images_foldering.html', doc_string=images_foldering.__doc__)

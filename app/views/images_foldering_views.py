from app import app
from flask import render_template, request, flash
from flask_login import login_required
import pandas as pd
from app.modules import img_processor
import shutil
import re
import fileinput
from werkzeug.datastructures import FileStorage
import os
from flask import send_file
from app.modules import yandex_disk_handler



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
    """test watermark placing on image for example"""
    img_processor.img_watermark("NO8B9709.JPG", "NO8B9717.JPG")


@app.route('/images_foldering_yandisk', methods=['POST', 'GET'])
@login_required
def images_foldering_yandisk():
    yandex_disk_handler.download_images_from_yandex_disk()
    return render_template('upload_images_foldering.html')


@app.route('/images_foldering', methods=['POST', 'GET'])
@login_required
def images_foldering():
    """
    on 21.01.2022:
    Text is expected in first place!
    if via txt then it with one columns no name
    for wb is our article
    for ozon is article without -size (-38)
    Get images from local yandex disk, preparing and foldering it on wb and ozon demand, and send it in zip
    on 08.08.2022 work only on local comp with pointing dict where img placed
    header in txt no need
    if good is wool then watermark will be placed like 150 x 300 см.
    if Article_WB is in columns (in second column) then - folder will be named by it, if not - then our art

    """
    if request.method == 'POST':
        if request.form['text_input']:
            input_text = request.form['text_input']
            input_text = input_text.split()
            print(input_text)
            df = pd.DataFrame(input_text, columns=['Article'])
            print(df)
        elif request.files['file']:
            file_txt: FileStorage = request.files['file']
            df = pd.read_csv(file_txt, sep='	', names=['Article', 'Article_WB'])
        else:
            flash(f"No one file of text is passed")
            return render_template('upload_images_foldering.html', doc_string=images_foldering.__doc__)

        return_data = img_processor.img_foldering(df)

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

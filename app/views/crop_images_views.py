from flask_login import login_required

import logging
from app import app
from flask import render_template, request, send_file
from app.modules import img_cropper
import flask
from app.modules.decorators import local_only


@app.route('/upload_img_crop', methods=['POST', 'GET'])
@login_required
@local_only
def upload_img_crop():
    if request.method == "POST":
        logging.warning(flask.request.files.getlist("images"))
        upload_images = flask.request.files.getlist("images")
        type_clothes_to_crop = request.form['type_clothes_to_crop']
        logging.warning('type_clothes_to_crop: ' + type_clothes_to_crop)
        images_zipped = img_cropper.crop_images(upload_images, type_clothes_to_crop)
        return send_file(images_zipped, download_name='zip.zip', as_attachment=True)

    return render_template('upload_img_crop.html')

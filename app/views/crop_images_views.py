from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required
from app.models import db
from urllib.parse import urlencode
from app.modules import img_cropper, io_output
import pandas as pd
import flask
import requests



@app.route('/upload_img_crop', methods=['POST', 'GET'])
def upload_img_crop():
    if request.method == "POST":
        upload_images = flask.request.files.getlist("images")
        print('upload images: ' + str(upload_images))
        images_zipped = img_cropper.crop_images(upload_images)
        return send_file(images_zipped, attachment_filename='zip.zip', as_attachment=True)

    return render_template('upload_img_crop.html')




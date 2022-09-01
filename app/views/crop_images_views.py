from app import app
from flask import render_template, request, send_file
from app.modules import img_cropper
import flask



@app.route('/upload_img_crop', methods=['POST', 'GET'])
def upload_img_crop():
    if request.method == "POST":
        print(flask.request.files.getlist("images"))
        upload_images = flask.request.files.getlist("images")
        # print('upload images: ' + str(upload_images))
        images_zipped = img_cropper.crop_images(upload_images)
        return send_file(images_zipped, attachment_filename='zip.zip', as_attachment=True)

    return render_template('upload_img_crop.html')

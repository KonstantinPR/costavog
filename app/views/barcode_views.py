from werkzeug.datastructures import FileStorage
import logging
from app import app
from flask import flash, render_template, request, send_file
import pandas as pd
from flask_login import login_required
from PIL import Image
from PIL import ImageDraw, ImageFont
from app.modules import io_output, zip_handler
import treepoem
import os
from barcode.writer import ImageWriter
from io import BytesIO
from barcode import Code128

import io
import zipfile
from PIL import Image
from flask import make_response


@app.route('/testbarcodegpt', methods=['GET', 'POST'])
@login_required
def testbarcodegpt():
    import io
    import zipfile
    import treepoem
    from flask import Flask, make_response

    # Your list of strings
    data = ["string1", "string2", "string3"]

    # Create an in-memory buffer to store the zip file
    zip_buffer = io.BytesIO()

    # Create a ZipFile object to write the images to
    zip_file = zipfile.ZipFile(zip_buffer, mode='w')

    # Generate a DataMatrix barcode for each string and save it as a PNG image in memory
    for i, d in enumerate(data):
        barcode = treepoem.generate_barcode(
            barcode_type='datamatrix',  # specify the barcode type
            data=d,
            output="png",  # generate PNG files instead of EPS files
            options={"eclevel": "m"}  # specify the error correction level
        )
        # Create an in-memory image object from the barcode data
        image = Image.open(io.BytesIO(barcode))
        # Save the image as PNG to an in-memory buffer
        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        # Add the image to the zip file
        zip_file.writestr(f'image_{i}.png', img_bytes.getvalue())

    # Close the zip file
    zip_file.close()

    # Create a Flask response object with the zip file data
    response = make_response(zip_buffer.getvalue())
    response.headers['Content-Type'] = 'application/zip'
    response.headers['Content-Disposition'] = 'attachment; filename=barcodes.zip'

    # Return the Flask response object
    return response


@app.route('/barcode', methods=['GET', 'POST'])
@login_required
def barcode():
    """png datamatrix from txt in zip"""
    if request.method == 'POST':

        if request.form['text_input']:
            input_text = request.form['text_input']
            input_text = input_text.split(" ")
            df = pd.DataFrame(input_text)
            # print(df)
        elif request.files['file']:
            file: FileStorage = request.files['file']
            df = pd.read_table(file, delim_whitespace=False, header=None)
            # print(df)
        else:
            flash("Не приложен файл")
            return render_template('upload_barcode.html')

        if not request.form['type-barcode']:
            flash("Тип баркода который будем печатать")
            return render_template('upload_barcode.html')

        type_barcode = request.form['type-barcode']
        print(f'type_barcode {type_barcode}')

        # col_name = "Датаматрикс"
        # df_column = df.T.reset_index().set_axis([col_name]).T.reset_index(drop=True)
        # print(df)
        lines = df[0].to_list()
        lines = [str(x) for x in lines]
        print(lines)

        # N = 3
        # list_merge_every_third = [''.join(map(str, lines[i:i + N])) for i in range(0, len(lines), N)]

        i = 0
        images_zipped = []
        images_set = []

        if len(lines[0]) > 100:
            type_barcode = 'Datamatrix'
            # flash("Code128 can't contain more then 100 simbol so type_barcode is chenged on Datamatrix")

        if type_barcode == 'code128':
            for line in lines:
                count_i = '{0:0>4}'.format(i)
                line_name = f'bar_{count_i}.png'

                rv = BytesIO()
                Code128(str(line), writer=ImageWriter()).write(rv)
                print(f'img {rv}')

                images_set.append((line_name, rv))
                images_zipped = zip_handler.put_in_zip(images_set)
                i += 1

            return send_file(images_zipped, download_name='zip.zip', as_attachment=True)

        if 'DYNO' in os.environ:
            # run on HEROKU cause the one don't understand pylibdmtx library
            for line in lines:
                count_i = '{0:0>4}'.format(i)
                line_name = f'bar_{count_i}.png'
                img = treepoem.generate_barcode(
                    barcode_type='datamatrix',  # One of the supported codes.
                    data=line,
                )
                img = io_output.io_img_output(img)
                images_set.append((line_name, img))
                images_zipped = zip_handler.put_in_zip(images_set)
                i += 1
        else:
            for i, line in enumerate(lines):
                print(line)
                count_i = '{0:0>4}'.format(i + 1)  # Change i to i + 1 to start counting from 1
                line_name = f'bar_{count_i}.png'
                datamatrix = treepoem.generate_barcode(
                    barcode_type='datamatrix',  # One of the supported codes.
                    data=line,
                )
                padded_image = Image.new(mode="RGB", size=(datamatrix.width + 10, datamatrix.height + 100),
                                         color=(255, 255, 255, 0))
                padded_image.paste(datamatrix, (5, 30))  # Offset the pasting by 20 pixels to add padding
                draw = ImageDraw.Draw(padded_image)
                draw.text((5, 0), "ЧЕСТНЫЙ ЗНАК", font=ImageFont.truetype("arial.ttf", 18),
                          fill=(0, 0, 0))  # add count_i to top left corner of image
                draw = ImageDraw.Draw(padded_image)
                draw.text((5, datamatrix.height + 40), line[0:18], font=ImageFont.truetype("arial.ttf", 14),
                          fill=(0, 0, 0))  # add count_i to top left corner of image
                draw = ImageDraw.Draw(padded_image)
                draw.text((5, datamatrix.height + 55), line[18:31], font=ImageFont.truetype("arial.ttf", 14),
                          fill=(0, 0, 0))  # add count_i to top left corner of image
                draw = ImageDraw.Draw(padded_image)
                draw.text((5, datamatrix.height + 70), line[32:38], font=ImageFont.truetype("arial.ttf", 14),
                          fill=(0, 0, 0))  # add count_i to top left corner of image
                img = io_output.io_img_output(padded_image)
                images_set.append((line_name, img))
            images_zipped = zip_handler.put_in_zip(images_set)

        return send_file(images_zipped, download_name='zip.zip', as_attachment=True)

    return render_template('upload_barcode.html', doc_string=barcode.__doc__)

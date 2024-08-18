from werkzeug.datastructures import FileStorage
from app import app
from flask import flash, render_template, request, send_file
import pandas as pd
from flask_login import login_required
from PIL import ImageDraw, ImageFont
from app.modules import io_output, zip_handler
import os
from barcode.writer import ImageWriter
from io import BytesIO
from barcode import Code128
import treepoem
import io
import zipfile
from PIL import Image
from flask import make_response
from pylibdmtx.pylibdmtx import encode


@app.route('/barcode', methods=['GET', 'POST'])
@login_required
def barcode():
    """png datamatrix from txt in zip"""
    if not request.method == 'POST':
        return render_template('upload_barcode.html', doc_string=barcode.__doc__)

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

    if not request.form['format']:
        flash("Формат сохранения не задан")
        return render_template('upload_barcode.html')

    type_barcode = request.form['type-barcode']
    print(f'type_barcode {type_barcode}')

    # col_name = "Датаматрикс"
    # df_column = df.T.reset_index().set_axis([col_name]).T.reset_index(drop=True)
    # print(df)
    lines = df[0].to_list()
    lines = [str(x) for x in lines]
    # print(lines)

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
            line_name = f'bar_{count_i}.jpg'
            img = treepoem.generate_barcode(barcode_type='datamatrix', data=line, )
            img = io_output.io_img_output(img)
            images_set.append((line_name, img))
            images_zipped = zip_handler.put_in_zip(images_set)
            i += 1
    else:
        for i, line in enumerate(lines):
            # print(line)
            count_i = '{0:0>4}'.format(i + 1)  # Change i to i + 1 to start counting from 1
            file_format = request.form['format']
            line_name = f"bar_{count_i}.{file_format}"
            scale_factor = 1  # Scale factor for making the image bigger

            # Generate the DataMatrix barcode as an SVG
            # datamatrix_svg = treepoem.generate_barcode(
            #     barcode_type='datamatrix',
            #     data=line,
            #     options={"format": file_format, "scale": scale_factor}
            # )
            encoded = encode(line.encode('utf8'))
            # print(encoded.width, encoded.height)
            datamatrix_svg = Image.frombytes('RGB', size=(encoded.width, encoded.height), data=encoded.pixels)

            # print(type(datamatrix_svg))
            # print(datamatrix_svg.decode('utf-8'))  # If it's a byte object

            # Convert the SVG to a high-resolution PNG using Pillow

            width, height = datamatrix_svg.width, datamatrix_svg.height
            # print("Original Size:", width, height)

            new_width = width * scale_factor
            new_height = height * scale_factor

            # Create an image with a white background
            padded_image = Image.new(mode="RGB",
                                     size=(new_width + 10 * scale_factor, new_height + 100 * scale_factor),
                                     color=(255, 255, 255))

            # Paste the resized SVG image into the padded image
            datamatrix_svg = datamatrix_svg.resize((new_width, new_height), Image.LANCZOS)
            padded_image.paste(datamatrix_svg, (5 * scale_factor, 40 * scale_factor))

            # Draw text
            draw = ImageDraw.Draw(padded_image)
            draw.text((14 * scale_factor, 6 * scale_factor), "ЧЕСТНЫЙ ЗНАК",
                      font=ImageFont.truetype("arial.ttf", 22 * scale_factor), fill=(0, 0, 0))
            draw.text((14 * scale_factor, new_height + 42 * scale_factor), line[0:18],
                      font=ImageFont.truetype("arial.ttf", 16 * scale_factor), fill=(0, 0, 0))
            draw.text((14 * scale_factor, new_height + 56 * scale_factor), line[18:31],
                      font=ImageFont.truetype("arial.ttf", 16 * scale_factor), fill=(0, 0, 0))
            draw.text((14 * scale_factor, new_height + 72 * scale_factor), line[32:38],
                      font=ImageFont.truetype("arial.ttf", 16 * scale_factor), fill=(0, 0, 0))

            img = io_output.io_img_output(padded_image, dpi=(300, 300))

            images_set.append((line_name, img))

        images_zipped = zip_handler.put_in_zip(images_set)

    return send_file(images_zipped, download_name='zip.zip', as_attachment=True)

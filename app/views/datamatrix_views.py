from werkzeug.datastructures import FileStorage
from app import app
from flask import flash, render_template, request, send_file
import pandas as pd
from flask_login import login_required
from PIL import Image
from app.modules import io_output, zip_handler
import treepoem
import os
import barcode

from barcode.writer import ImageWriter
from io import BytesIO

from barcode import EAN13
from barcode import Code128
from barcode.writer import SVGWriter
from barcode import generate


@app.route('/datamatrix', methods=['GET', 'POST'])
@login_required
def datamatrix():
    """png datamatrix from txt in zip"""
    if request.method == 'POST':
        file_txt: FileStorage = request.files['file']

        if not request.files['file']:
            flash("Не приложен файл")
            return render_template('upload_datamatrix.html')

        if not request.form['type-barcode']:
            flash("Тип баркода который будем печатать")
            return render_template('upload_datamatrix.html')

        type_barcode = request.form['type-barcode']
        print(f'type_barcode {type_barcode}')

        df = pd.read_fwf(file_txt)
        col_name = "Датаматрикс"
        df_column = df.T.reset_index().set_axis([col_name]).T.reset_index(drop=True)
        lines = df_column[col_name].to_list()
        print(lines)

        # N = 3
        # list_merge_every_third = [''.join(map(str, lines[i:i + N])) for i in range(0, len(lines), N)]

        i = 0
        images_zipped = []
        images_set = []

        # Write to a file-like object:
        if type_barcode == 'code128':
            from pylibdmtx.pylibdmtx import encode
            for line in lines:
                count_i = '{0:0>4}'.format(i)
                line_name = f'bar_{count_i}.png'

                rv = BytesIO()
                Code128(str(line), writer=ImageWriter()).write(rv)
                print(f'img {rv}')

                images_set.append((line_name, rv))
                images_zipped = zip_handler.put_in_zip(images_set)
                i += 1

            return send_file(images_zipped, attachment_filename='zip.zip', as_attachment=True)

        if 'DYNO' in os.environ:
            # run on HEROKU
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
            # run LOCAL
            from pylibdmtx.pylibdmtx import encode
            for line in lines:
                count_i = '{0:0>4}'.format(i)
                line_name = f'bar_{count_i}.png'
                encoded = encode(line.encode('utf8'))
                img = Image.frombytes('RGB', (encoded.width, encoded.height), encoded.pixels)
                img = io_output.io_img_output(img)
                images_set.append((line_name, img))
                images_zipped = zip_handler.put_in_zip(images_set)
                i += 1

        return send_file(images_zipped, attachment_filename='zip.zip', as_attachment=True)

    return render_template('upload_datamatrix.html', doc_string=datamatrix.__doc__)

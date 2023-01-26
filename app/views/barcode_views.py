from werkzeug.datastructures import FileStorage
from app import app
from flask import flash, render_template, request, send_file
import pandas as pd
from flask_login import login_required
from PIL import Image
from app.modules import io_output, zip_handler
import treepoem
import os
from barcode.writer import ImageWriter
from io import BytesIO
from barcode import Code128




@app.route('/barcode', methods=['GET', 'POST'])
@login_required
def barcode():
    """png datamatrix from txt in zip"""
    if request.method == 'POST':

        if request.form['text_input']:
            input_text = request.form['text_input']
            input_text = input_text.split(" ")
            df = pd.DataFrame(input_text)
            print(df)
        elif request.files['file']:
            file: FileStorage = request.files['file']
            df = pd.read_table(file, delim_whitespace=False, header=None)
            print(df)
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
        print(df)
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

            return send_file(images_zipped, attachment_filename='zip.zip', as_attachment=True)

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
            # run LOCAL cause it much faster than pylibdmtx library
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

    return render_template('upload_barcode.html', doc_string=barcode.__doc__)

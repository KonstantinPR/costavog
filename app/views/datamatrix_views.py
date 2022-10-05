from werkzeug.datastructures import FileStorage
from app import app
from flask import flash, render_template, request, send_file
import pandas as pd
from flask_login import login_required
from pylibdmtx.pylibdmtx import encode
from PIL import Image
from app.modules import io_output, zip_handler


@app.route('/datamatrix', methods=['GET', 'POST'])
@login_required
def datamatrix():
    """png datamatrix from txt in zip"""
    if request.method == 'POST':
        file_txt: FileStorage = request.files['file']

        if not request.files['file']:
            flash("Не приложен файл")
            return render_template('upload_datamatrix.html')

        # if not request.form['multiply_number']:
        #     flash("Сколько фото делать то будем? Поле пустое")
        #     return render_template('upload_datamatrix.html')

        # multiply = int(request.form['multiply_number'])

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

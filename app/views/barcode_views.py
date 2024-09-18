from werkzeug.datastructures import FileStorage
from app import app
from flask import flash, render_template, request, send_file
import pandas as pd
from flask_login import login_required
from app.modules import zip_handler, barcode_module


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

    elif request.files['file']:
        file: FileStorage = request.files['file']
        df = pd.read_table(file, delim_whitespace=False, header=None)

    else:
        flash("Не приложен файл")
        return render_template('upload_barcode.html')

    if not request.form['type-barcode']:
        flash("Тип баркода который будем печатать")
        return render_template('upload_barcode.html')


    type_barcode = request.form['type-barcode']
    print(f'type_barcode {type_barcode}')

    images_set = barcode_module.create_barcodes(df, type_barcode=type_barcode)
    images_zipped = zip_handler.put_in_zip(images_set)


    return send_file(images_zipped, download_name='zip.zip', as_attachment=True)

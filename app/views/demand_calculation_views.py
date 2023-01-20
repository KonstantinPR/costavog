import datetime

from flask_login import login_required
from werkzeug.datastructures import FileStorage
from app import app
from flask import render_template, request, send_file, flash
from app.modules import io_output, demand_calculation_module


@app.route('/demand_calculation_excel', methods=['POST', 'GET'])
@login_required
def demand_calculation_excel():
    """
    Excel с потребностями:
    Присылает excel с потребностями.
    Напечатает те артикулы, которые передали в txt с шапкой vendorCode.
    В строке пишем через пробел подстроки, которые ищем в артикуле, которые хотим вывести в excel.
    Если передали файл - то приоритет у файла, строка работать не будет. Если файла нет - работает строка.
    """

    if request.method == 'POST':
        input_txt: FileStorage = request.files['file']
        search_string = str(request.form['search_string'])
        df = demand_calculation_module.demand_calculation_to_df(input_txt, search_string)
        df = io_output.io_output(df)
        file_name = f"demand_calculation_{str(datetime.date.today())}.xlsx"
        # df.to_excel("df_output.xlsx")

        return send_file(df, attachment_filename=file_name, as_attachment=False)
    return render_template('upload_demand_calculation_with_image_catalog.html',
                           doc_string=demand_calculation_excel.__doc__)


@app.route('/demand_calculation_with_image_catalog', methods=['POST', 'GET'])
@login_required
def demand_calculation_with_image_catalog():
    """
    Каталог PDF с потребностями:
    Делает PDF каталог с потребностями. С фото артикула и другой информацией.
    Работает на локальном яндекс.диске.
    Напечатает те артикулы, которые передали в txt с шапкой vendorCode.
    В строке пишем через пробел подстроки, которые ищем в артикуле, которые хотим вывести в каталог.
    Если передали файл - то приоритет у файла, строка работать не будет. Если файла нет - работает строка.
    """

    if request.method == 'POST':
        input_txt: FileStorage = request.files['file']

        search_string = str(request.form['search_string'])
        search_string_list = search_string.split()
        print(search_string_list)
        if not search_string_list and not input_txt:
            flash("ОШИБКА. Прикрепите файл или заполните инпут строку !")
            return render_template('upload_demand_calculation_with_image_catalog.html',
                                   doc_string=demand_calculation_with_image_catalog.__doc__ + demand_calculation_excel.__doc__)
        df = demand_calculation_module.demand_calculation_to_df(input_txt, search_string)
        pdf = demand_calculation_module.demand_calculation_df_to_pdf(df)

        return send_file(pdf, as_attachment=True)

    return render_template('upload_demand_calculation_with_image_catalog.html',
                           doc_string=demand_calculation_with_image_catalog.__doc__)

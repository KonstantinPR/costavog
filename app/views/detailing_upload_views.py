from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import io_output, yandex_disk_handler, pandas_handler, detailing_upload_module, price_module, API_WB
from app.modules import implementation_report, request_handler
import numpy as np

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'pdf', 'xlsx'}


@app.route('/extract_financial_data_from_pdf', methods=['POST', 'GET'])
@login_required
def extract_financial_data_from_pdf():
    """Extract financial data from uploaded PDF files weekly_implementation_report.pdf. Income - pdf files, outcome -
    zip file with 3 files. First - outcome by each week, second  - by each quarter, third - total"""

    if request.method != 'POST':
        return render_template('upload_weekly_implementation_report.html',
                               doc_string=extract_financial_data_from_pdf.__doc__)

    pdf_files = request_handler.get_files(request)
    all_data = implementation_report.pdf_files_process(pdf_files)

    # Convert the collected data into a DataFrame
    df = pd.DataFrame(all_data)

    # Calculate totals for each quarter
    df_totals = implementation_report.totals_calculate(df)

    # Calculate grand totals
    df_grand = implementation_report.grand_calculate(df)

    # Create a DataFrame for totals by quarter and grand totals
    zip_name = "upload_weekly_implementation_report.zip"
    name = "detailing_for_tax_purpose.xlsx"
    name_quarter = "detailing_for_tax_purpose_by_quarter.xlsx"
    name_total = "detailing_for_tax_purpose_total.xlsx"
    file, zip_name = pandas_handler.files_to_zip([df, df_totals, df_grand], [name, name_quarter, name_total], zip_name)

    return send_file(file, download_name=zip_name, as_attachment=True)


@app.route('/upload_detailing', methods=['POST', 'GET'])
@login_required
def upload_detailing():
    """Analize detailing of excel that can be downloaded in wb portal in zips, you can put any number zips."""

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if not request.method == 'POST':
        return render_template('upload_detailing.html', doc_string=upload_detailing.__doc__)

    days_by = int(request.form.get('days_by'))
    if not days_by: days_by = int(app.config['DAYS_PERIOD_DEFAULT'])
    # print(f"days_by {days_by}")
    uploaded_files = request.files.getlist("file")
    testing_mode = request.form.get('is_testing_mode')
    promo_file = request.files.get("promofile")
    is_just_concatenate = request.form.get('is_just_concatenate')
    is_discount_template = request.form.get('is_discount_template')
    is_dynamic = request.form.get('is_dynamic')
    is_chosen_columns = request.form.get('is_chosen_columns')
    # print(f"is_discount_template {is_discount_template}")

    yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                    path_or_config=app.config['REPORT_DETAILING_UPLOAD'],
                                                    testing_mode=testing_mode)

    if not uploaded_files:
        flash("Вы ничего не выбрали. Необходим zip архив с zip архивами, скаченными с сайта wb раздела детализаций")
        return render_template('upload_detailing.html')

    uploaded_file = detailing_upload_module.process_uploaded_files(uploaded_files)
    df_list = detailing_upload_module.zips_to_list(uploaded_file)
    df_list = pandas_handler.upper_case(df_list, 'Артикул поставщика')

    if is_just_concatenate == 'is_just_concatenate':
        return send_file(io_output.io_output(pd.concat(df_list)), download_name=f'concat.xlsx', as_attachment=True)

    df_list, dfs_names = detailing_upload_module.dfs_process(df_list, request, testing_mode=testing_mode,
                                                             is_dynamic=is_dynamic)
    # main concatenated df
    df = df_list[0]
    df_dynamic = detailing_upload_module.dfs_dynamic(df_list, is_dynamic=is_dynamic)
    df = detailing_upload_module.influence_discount_by_dynamic(df, df_dynamic)
    df = detailing_upload_module.in_positive_digit(df, decimal=0, col_names='new_discount')

    # file, name = pandas_handler.files_to_zip(dfs, dfs_names)
    # return send_file(file, download_name=name, as_attachment=True)

    df_promo = detailing_upload_module.promofiling(promo_file, df[['nmId', 'new_discount']])

    detailing_name = "report_detailing_upload.xlsx"
    file = io_output.io_output(df)
    yandex_disk_handler.upload_to_YandexDisk(file, file_name=detailing_name, path=app.config['REPORT_DETAILING_UPLOAD'],
                                             testing_mode=testing_mode)

    if is_chosen_columns:
        df = df[[col for col in detailing_upload_module.CHOSEN_COLUMNS if col in df]]

    promo_name = "promo.xlsx"
    template_name = "discount_template.xlsx"
    df_dynamic_name = "df_dynamic.xlsx"

    df_template = pandas_handler.df_disc_template_create(df, df_promo, is_discount_template)
    dfs = [df, df_promo, df_template, df_dynamic]
    dfs_names = [detailing_name, promo_name, template_name, df_dynamic_name]
    file, name = pandas_handler.files_to_zip(dfs, dfs_names)

    # Flash message and return the zip file for download
    flash("Отчет успешно создан")
    return send_file(file, download_name=name, as_attachment=True)

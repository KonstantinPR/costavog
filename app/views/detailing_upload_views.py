from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import io_output, yandex_disk_handler, pandas_handler, detailing_upload_module
from app.modules import implementation_report, request_handler
from types import SimpleNamespace

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

    if not request.method == 'POST':
        return render_template('upload_detailing.html', doc_string=upload_detailing.__doc__)

    r = detailing_upload_module.get_data_from(request)

    if not r.uploaded_files:
        flash("Вы ничего не выбрали. Необходим zip архив с zip архивами, скаченными с сайта wb раздела детализаций")
        return render_template('upload_detailing.html')

    yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                    path_or_config=app.config['REPORT_DETAILING_UPLOAD'],
                                                    testing_mode=r.testing_mode)

    uploaded_file = detailing_upload_module.process_uploaded_files(r.uploaded_files)
    df_list = detailing_upload_module.zips_to_list(uploaded_file)
    df_list = pandas_handler.upper_case(df_list, 'Артикул поставщика')

    if r.is_just_concatenate:
        return send_file(io_output.io_output(pd.concat(df_list)), download_name=f'concat.xlsx', as_attachment=True)

    df, df_dynamic_list = detailing_upload_module.dfs_process(df_list, r=r)
    df_merged_dynamic = detailing_upload_module.dfs_dynamic(df_dynamic_list, is_dynamic=r.is_dynamic)
    df = detailing_upload_module.influence_discount_by_dynamic(df, df_merged_dynamic)
    df = detailing_upload_module.in_positive_digit(df, decimal=0, col_names='new_discount')

    # file, name = pandas_handler.files_to_zip(dfs, dfs_names)
    # return send_file(file, download_name=name, as_attachment=True)

    df_promo = detailing_upload_module.promofiling(r.promo_file, df[['nmId', 'new_discount']])

    n = detailing_upload_module.file_names()

    yandex_disk_handler.upload_to_YandexDisk(df, file_name=n.detailing_name, path=app.config['REPORT_DETAILING_UPLOAD'],
                                             testing_mode=r.testing_mode)

    if r.is_chosen_columns:
        df = df[[col for col in detailing_upload_module.CHOSEN_COLUMNS if col in df]]

    df_template = pandas_handler.df_disc_template_create(df, df_promo, r.is_discount_template)

    # Assuming you have your DataFrames and names
    dfs_list = [df, df_promo, df_template, df_merged_dynamic]
    dfs_names_list = [n.detailing_name, n.promo_name, n.template_name, n.df_dynamic_name]

    # Use a list comprehension to filter out empty DataFrames and their names
    filtered_dfs = [df for df in dfs_list if not df.empty]
    filtered_dfs_names = [name for df, name in zip(dfs_list, dfs_names_list) if not df.empty]

    # Now you can call files_to_zip with the filtered lists
    file, name = pandas_handler.files_to_zip(filtered_dfs, filtered_dfs_names)

    # Flash message and return the zip file for download
    flash("Отчет успешно создан")
    return send_file(file, download_name=name, as_attachment=True)

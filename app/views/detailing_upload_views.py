import app.modules.detailing_upload_dict_module
from app import app
from flask import flash, render_template, request, send_file
from flask_login import login_required
import pandas as pd
from app.modules import io_output, yandex_disk_handler, pandas_handler, detailing_upload_module
from app.modules import implementation_report, request_handler, detailing_upload_dict_module
from app.modules.decorators import timing_decorator

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'pdf', 'xlsx'}


@app.route('/extract_financial_data_from_pdf', methods=['POST', 'GET'])
@login_required
def extract_financial_data_from_pdf():
    """24.03.2025 is ok in folder for 2024 result. Extract financial data from uploaded PDF files weekly_implementation_
    report.pdf or Отчет...pdf. Income - pdf files, outcome -
    zip file with 3 files. First - outcome by each week, second  - by each quarter, third - total
    C:\YandexDisk\СЕТЕВЫЕ МАГАЗИНЫ\WILDBERRIES\ВАЖНОЕ\ДЕТАЛИЗАЦИЯ С WB с 11.2018\УПД PDF ДЕТАЛИЗАЦИИ\РАСПАКОВАННЫЕ"""

    if request.method != 'POST':
        return render_template('upload_weekly_implementation_report.html',
                               doc_string=extract_financial_data_from_pdf.__doc__)

    pdf_files = request_handler.get_files(request)
    all_data = implementation_report.pdf_files_process(pdf_files)

    # Convert the collected data into a DataFrame
    df = pd.DataFrame(all_data)

    # Calculate totals for each quarter
    # df.to_excel("df_middle.xlsx")
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
@timing_decorator
def upload_detailing():
    """Analize detailing of excel that can be downloaded in wb portal in zips, you can put any number zips."""

    if not request.method == 'POST':
        return render_template('upload_detailing.html', doc_string=upload_detailing.__doc__)

    r = detailing_upload_module.get_data_from(request)

    if not r.uploaded_files:
        flash("Вы ничего не выбрали. Необходим zip архив с zip архивами, скаченными с сайта wb раздела детализаций")
        return render_template('upload_detailing.html')

    yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                    path_or_config=app.config[r.path_to_save],
                                                    testing_mode=r.testing_mode)

    uploaded_file = detailing_upload_module.process_uploaded_files(r.uploaded_files)
    df_list = detailing_upload_module.zips_to_list(uploaded_file)
    df_list = pandas_handler.upper_case(df_list, 'Артикул поставщика')

    if r.is_just_concatenate:
        return send_file(io_output.io_output(pd.concat(df_list)), download_name=f'concat.xlsx', as_attachment=True)

    df, df_dynamic_list, d = detailing_upload_module.dfs_process(df_list, r=r)

    df_merged_dynamic = detailing_upload_module.dfs_dynamic(df_dynamic_list, r=r, by_col="Артикул поставщика")

    df_merged_dynamic_by_prefix = detailing_upload_module.merge_dynamic_by(df_merged_dynamic, by_col='prefix', r=r)

    df = detailing_upload_module.influence_discount_by_dynamic(df, df_merged_dynamic)
    df = detailing_upload_module.mix_detailings(df, is_compare_detailing=r.is_compare_detailing)

    df = detailing_upload_module.in_positive_digit(df, decimal=0, col_names='new_discount')

    df_promo = detailing_upload_module.promofiling(r.promo_file, df[['nmId', 'new_discount']], r.k_action_diff)

    n = detailing_upload_module.file_names()

    if r.is_chosen_columns:
        columns = detailing_upload_dict_module.CHOSEN_COLUMNS + detailing_upload_dict_module.DELIVERY_COLUMNS
        df = df[[col for col in columns if col in df]]

    df = pandas_handler.fill_val_by_df(df_left=df, df_right=d.df_all_cards[['vendorCode', 'brand']],
                                       left_on='Артикул поставщика', right_on='vendorCode', left_col='Бренд',
                                       right_col='brand'
                                       )

    df = df.drop_duplicates(subset='Артикул поставщика')

    yandex_disk_handler.upload_to_YandexDisk(file=df, file_name=n.detailing_name,
                                             path=app.config[r.path_to_save],
                                             testing_mode=r.testing_mode, is_upload=r.is_save_yadisk)

    df_template = pandas_handler.df_disc_template_create(df, df_promo, r.is_discount_template)

    dfs_dict = {'df': df, 'df_promo': df_promo, 'df_template': df_template, 'df_merged_dynamic': df_merged_dynamic,
                'df_merged_dynamic_by_prefix': df_merged_dynamic_by_prefix}

    # Filter out the empty DataFrames and their names
    filtered_dfs_list, filtered_dfs_names_list = pandas_handler.keys_values_in_list_from_dict(dfs_dict, ext='.xlsx')

    print(f"ready to zip {filtered_dfs_names_list}")

    # Add the timestamp to the zip_name
    zip_file, zip_name = pandas_handler.files_to_zip(filtered_dfs_list, filtered_dfs_names_list)

    yandex_disk_handler.upload_to_YandexDisk(request=request, file=zip_file,
                                             path=app.config['REPORT_WB_ZIPS'], testing_mode=r.testing_mode,
                                             is_add_timestamp=True, is_upload=False)

    # Flash message and return the zip file for download
    flash("Отчет успешно создан")
    return send_file(zip_file, download_name=zip_name, as_attachment=True)

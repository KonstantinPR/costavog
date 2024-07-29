import logging
from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import io_output, yandex_disk_handler, pandas_handler, detailing_upload_module, price_module, API_WB
from app.modules import sales_funnel_module
from app.modules import detailing_api_module
import numpy as np
import re
import pdfplumber
import io
from datetime import datetime

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'pdf', 'xlsx'}


@app.route('/extract_financial_data_from_pdf', methods=['POST', 'GET'])
@login_required
def extract_financial_data_from_pdf():
    """Extract financial data from uploaded PDF files."""

    if request.method != 'POST':
        return render_template('upload_weekly_implementation_report.html',
                               doc_string=extract_financial_data_from_pdf.__doc__)

    # Check if the post request has the file part
    if 'file' not in request.files:
        return 'No file part', 400

    pdf_files = request.files.getlist("file")

    # Ensure at least one file is uploaded
    if len(pdf_files) == 0:
        return 'No files uploaded', 400

    all_data = []

    for pdf_file in pdf_files:
        try:
            # Use pdfplumber to open the PDF file
            with pdfplumber.open(pdf_file) as pdf:
                text = ''
                for page in pdf.pages:
                    # Extract text from each page
                    page_text = page.extract_text() or ''
                    text += page_text

        except Exception as e:
            return f"Failed to process PDF file: {str(e)}", 500

        print("Extracted Text:\n", text)  # Debug: Print the extracted text

        # Define patterns for extracting financial data
        patterns = {
            "total_product_value": r"Всего стоимость реализованного товара.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "total_deducted_value": r"Итого зачтено из стоимости реализованного товара.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "wildberries_reward": r"Сумма вознаграждения Вайлдберриз.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "vat": r"НДС с вознаграждения Вайлдберриз.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "international_shipping_cost": r"Стоимость услуг Вайлдберриз по организации международной перевозки.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "acquiring_costs": r"Возмещение издержек по эквайрингу.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "agent_expenses": r"Возмещение расходов поверенного.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "penalties": r"Штрафы.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "other_deductions": r"Прочие удержания.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "compensation_damage": r"Компенсация ущерба.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "other_payments": r"Прочие выплаты.*?(\d{1,9}(?:\.\d{2})*,\d{2})",
            "final_amount": r"Итого к перечислению Продавцу за текущий период.*?(\d{1,9}(?:\.\d{2})*,\d{2})"
        }

        # Define the desired column names
        column_names = {
            "total_product_value": "Всего стоимость реализованного товара",
            "total_deducted_value": "Итого зачтено из стоимости реализованного товара",
            "wildberries_reward": "Сумма вознаграждения Вайлдберриз",
            "vat": "НДС с вознаграждения Вайлдберриз",
            "international_shipping_cost": "Стоимость услуг Вайлдберриз по организации международной перевозки",
            "acquiring_costs": "Возмещение издержек по эквайрингу",
            "agent_expenses": "Возмещение расходов поверенного",
            "penalties": "Штрафы",
            "other_deductions": "Прочие удержания",
            "compensation_damage": "Компенсация ущерба",
            "other_payments": "Прочие выплаты",
            "final_amount": "Итого к перечислению Продавцу за текущий период"
        }

        # Extract values using regex
        extracted_data = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.DOTALL)
            if match:
                # Replace commas with periods for float conversion
                value = float(match.group(1).replace(',', '.'))
                extracted_data[column_names[key]] = value
                print(f"{column_names[key]}: {extracted_data[column_names[key]]}")  # Debug: Print extracted value
            else:
                extracted_data[column_names[key]] = None
                print(f"{column_names[key]}: No match found")  # Debug: Indicate no match

        # Calculate new columns
        # total_product_value = extracted_data.get("Всего стоимость реализованного товара", 0) or 0
        # final_amount = extracted_data.get("Итого к перечислению Продавцу за текущий период", 0) or 0
        # final_amount = extracted_data.get("Итого к перечислению Продавцу за текущий период", 0) or 0
        extracted_data["Добавить доход в бухгалтерию"] = extracted_data[column_names["total_product_value"]] - \
                                                         extracted_data[column_names["final_amount"]] + \
                                                         extracted_data[column_names["compensation_damage"]] + \
                                                         extracted_data[column_names["other_payments"]]

        total_deducted_value = extracted_data.get("Итого зачтено из стоимости реализованного товара", 0) or 0
        penalties = extracted_data.get("Штрафы", 0) or 0
        other_deductions = extracted_data.get("Прочие удержания", 0) or 0
        compensation_damage = extracted_data.get("Компенсация ущерба", 0) or 0
        extracted_data["Добавить расход в бухгалтерию"] = total_deducted_value - penalties

        # Extract the period and determine the quarter
        date_pattern = r"за период с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})"
        date_match = re.search(date_pattern, text)
        if date_match:
            start_date_str = date_match.group(1)
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            quarter = (start_date.month - 1) // 3 + 1
            extracted_data['Квартал'] = f'Q{quarter}'
            print(f"Quarter: Q{quarter}")  # Debug: Print the quarter
        else:
            extracted_data['Квартал'] = None
            print("Quarter: No match found")  # Debug: Indicate no match

        # Collect data from each PDF
        all_data.append(extracted_data)

    # Convert the collected data into a DataFrame
    df = pd.DataFrame(all_data)

    # Calculate totals for each quarter
    df.loc['Total'] = df.sum(numeric_only=True)
    df_totals = df.groupby('Квартал').sum(numeric_only=True).reset_index()
    df_totals['Квартал'] = df_totals['Квартал'].astype(str)
    # df_totals = df_totals.rename(columns=lambda x: f"total_by_quarter_{x}" if x != 'Квартал' else 'Квартал')

    # Calculate grand totals
    grand_totals = df.sum(numeric_only=True).to_frame().T
    grand_totals['Квартал'] = 'Total'
    # grand_totals = grand_totals.rename(columns=lambda x: f"total_by_quarter_{x}" if x != 'Квартал' else 'Квартал')

    # Create a DataFrame for totals by quarter and grand totals
    # totals_df = pd.concat([df_totals, grand_totals], ignore_index=True)
    zip_name = "upload_weekly_implementation_report"
    name = "detailing_for_tax_purpose.xlsx"
    name_quarter = "detailing_for_tax_purpose_by_quarter.xlsx"
    name_total = "detailing_for_tax_purpose_total.xlsx"
    file, zip_name = pandas_handler.files_to_zip([df, df_totals, grand_totals], [name, name_quarter, name_total])

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
    print(f"days_by {days_by}")
    uploaded_files = request.files.getlist("file")
    promo_file = request.files.get("promofile")
    testing_mode = request.form.get('is_testing_mode')
    is_net_cost = request.form.get('is_net_cost')
    is_get_storage = request.form.get('is_get_storage')
    change_discount = request.form.get('change_discount')
    is_just_concatenate = request.form.get('is_just_concatenate')
    is_delete_shushary = request.form.get('is_delete_shushary')
    is_get_price = request.form.get('is_get_price')
    is_get_stock = request.form.get('is_get_stock')
    is_funnel = request.form.get('is_funnel')
    k_delta = request.form.get('k_delta')
    is_mix_discounts = request.form.get('is_mix_discounts')
    is_discount_template = request.form.get('is_discount_template')
    print(f"is_discount_template {is_discount_template}")
    if not k_delta: k_delta = 1
    k_delta = int(k_delta)

    yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                    path_or_config=app.config['REPORT_DETAILING_UPLOAD'],
                                                    testing_mode=testing_mode)

    INCLUDE_COLUMNS = list(detailing_upload_module.INITIAL_COLUMNS_DICT.values())

    if not uploaded_files:
        flash("Вы ничего не выбрали. Необходим zip архив с zip архивами, скаченными с сайта wb раздела детализаций")
        return render_template('upload_detailing.html')

    uploaded_file = detailing_upload_module.process_uploaded_files(uploaded_files)

    # print(df_net_cost)
    df_list = detailing_upload_module.zips_to_list(uploaded_file)
    concatenated_dfs = pd.concat(df_list)

    concatenated_dfs = pandas_handler.upper_case(concatenated_dfs, 'Артикул поставщика')

    # Check if concatenate parameter is passed
    if is_just_concatenate == 'is_just_concatenate':
        return send_file(io_output.io_output(concatenated_dfs), download_name=f'concat.xlsx', as_attachment=True)

    concatenated_dfs = detailing_upload_module.replace_incorrect_date(concatenated_dfs)

    date_min = concatenated_dfs["Дата продажи"].min()
    print(f"date_min {date_min}")
    # concatenated_dfs.to_excel('ex.xlsx')
    date_max = concatenated_dfs["Дата продажи"].max()

    dfs_sales, INCLUDE_COLUMNS = detailing_upload_module.get_dynamic_sales(concatenated_dfs, days_by, INCLUDE_COLUMNS)

    # concatenated_dfs = detailing.get_dynamic_sales(concatenated_dfs)

    storage_cost = detailing_upload_module.get_storage_cost(concatenated_dfs)

    df = detailing_upload_module.zip_detail_V2(concatenated_dfs, drop_duplicates_in="Артикул поставщика")

    df = detailing_upload_module.merge_stock(df, testing_mode=testing_mode, is_get_stock=is_get_stock,
                                             is_delete_shushary=is_delete_shushary)

    if not 'quantityFull' in df.columns: df['quantityFull'] = 0
    df['quantityFull'].replace(np.NaN, 0, inplace=True)
    df['quantityFull + Продажа, шт.'] = df['quantityFull'] + df['Продажа, шт.']

    df = detailing_upload_module.merge_storage(df, storage_cost, testing_mode, is_get_storage=is_get_storage,
                                               is_delete_shushary=is_delete_shushary)
    df = detailing_upload_module.merge_net_cost(df, is_net_cost)
    df = detailing_upload_module.merge_price(df, testing_mode, is_get_price).drop_duplicates(subset='nmId')
    df = detailing_upload_module.profit_count(df)
    df_rating = yandex_disk_handler.get_excel_file_from_ydisk(app.config['RATING'])
    # df = df.merge(df_rating, how='outer', left_on='nmId', right_on="Артикул")
    df = pandas_handler.df_merge_drop(df, df_rating, 'nmId', 'Артикул', how="outer")
    df.to_excel("rating_merged.xlsx")

    df = pandas_handler.fill_empty_val_by(['article', 'vendorCode', 'supplierArticle'], df, 'Артикул поставщика')
    df = pandas_handler.fill_empty_val_by(['brand'], df, 'Бренд')
    df = df.rename(columns={'Предмет_x': 'Предмет'})
    df = pandas_handler.fill_empty_val_by(['category'], df, 'Предмет')

    if dfs_sales:
        print(f"merging dfs_sales ...")
        for d in dfs_sales:
            df = pandas_handler.df_merge_drop(df, d, 'nmId', 'Код номенклатуры', how="outer")
            # df = df.merge(d, how='left', left_on='nmId', right_on='Код номенклатуры')

    # df.to_excel("sales_merged.xlsx")

    # --- DICOUNT ---

    df = detailing_upload_module.get_period_sales(df, date_min, date_max)
    k_norma_revenue = price_module.count_norma_revenue(df)
    df = price_module.discount(df, k_delta=k_delta, k_norma_revenue=k_norma_revenue)
    discount_columns = sales_funnel_module.DISCOUNT_COLUMNS
    # discount_columns['buyoutsCount'] = 'Ч. Продажа шт.'

    # df = price_module.k_dynamic(df, days_by=days_by)
    # 27/04/2024 - not yet prepared
    # df[discount_columns['func_discount']] *= df['k_dynamic']

    # Reorder the columns

    #  --- PATTERN SPLITTING ---
    df = df[~df['nmId'].isin(pandas_handler.FALSE_LIST)]
    df['prefix'] = df['Артикул поставщика'].astype(str).apply(lambda x: x.split("-")[0])
    prefixes_dict = detailing_upload_module.PREFIXES_ART_DICT
    prefixes = list(prefixes_dict.keys())
    df['prefix'] = df['prefix'].apply(lambda x: starts_with_prefix(x, prefixes))
    df['prefix'] = df['prefix'].apply(lambda x: prefixes_dict.get(x, x))
    df['pattern'] = df['Артикул поставщика'].apply(get_second_part)
    df['material'] = df['Артикул поставщика'].apply(get_third_part)
    MATERIAL_DICT = detailing_upload_module.MATERIAL_DICT
    df['material'] = [MATERIAL_DICT[x] if x in MATERIAL_DICT else y for x, y in zip(df['pattern'], df['material'])]

    if is_funnel:
        df_funnel, file_name = API_WB.get_wb_sales_funnel_api(request, testing_mode=testing_mode)
        # df = df.merge(df_funnel, how='outer', left_on='nmId', right_on="nmID")
        df = pandas_handler.df_merge_drop(df, df_funnel, "nmId", "nmID")
        df = sales_funnel_module.calculate_discount(df, discount_columns=discount_columns)
        df = price_module.mix_discounts(df, is_mix_discounts)

    df = detailing_upload_module.add_k(df)

    # print(INCLUDE_COLUMNS)
    include_column = [col for col in INCLUDE_COLUMNS if col in df.columns]
    df = df[include_column + [col for col in df.columns if col not in INCLUDE_COLUMNS]]
    df = pandas_handler.round_df_if(df, half=10)
    if 'new_discount' not in df.columns: df['new_discount'] = df['n_discount']
    df_promo = detailing_upload_module.promofiling(promo_file, df[['nmId', 'new_discount']])

    detailing_name = "report_detailing_upload.xlsx"
    file = io_output.io_output(df)
    yandex_disk_handler.upload_to_YandexDisk(file, file_name=detailing_name, path=app.config['REPORT_DETAILING_UPLOAD'],
                                             testing_mode=testing_mode)

    promo_name = "promo.xlsx"
    template_name = "discount_template.xlsx"

    # Define the filenames for the zip file
    df_template = pandas_handler.df_disc_template_create(df, df_promo, is_discount_template)
    # Create a zip file containing the DataFrames
    file, name = pandas_handler.files_to_zip([df, df_promo, df_template], [detailing_name, promo_name, template_name])

    # Flash message and return the zip file for download
    flash("Отчет успешно создан")
    return send_file(file, download_name=name, as_attachment=True)


def starts_with_prefix(string, prefixes):
    for prefix in prefixes:
        if string.startswith(prefix):
            if len(string) > 10:
                return ''
            return prefix  # Return the prefix itself, not prefixes[prefix]
    return string


def get_second_part(x):
    try:
        return str(x).split("-")[1]
    except IndexError:
        # If the string doesn't contain the delimiter '-', return None or any other value as needed
        return ''


def get_third_part(x):
    try:
        return str(x).split("-")[2]
    except IndexError:
        # If the string doesn't contain the delimiter '-', return None or any other value as needed
        return ''

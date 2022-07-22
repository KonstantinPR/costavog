from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
from app.models import Product, db
import datetime
import pandas as pd
from app.modules import detailing, detailing_reports
from app.modules import io_output
import time


@app.route('/revenue_processing', methods=['POST', 'GET'])
@login_required
def revenue_processing():
    """
    correcting existing discount via analise revenue dynamics and stocks
    """

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        sf_df, file_name = detailing_reports.revenue_processing_module(request)
        print(file_name)
        file_content = io_output.io_output_styleframe(sf_df)
        return send_file(file_content, download_name=file_name, as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    df = detailing_reports.get_wb_price_api()
    file_content = io_output.io_output(df)
    return send_file(file_content, attachment_filename='price.xlsx', as_attachment=True)


@app.route('/get_wb_pivot_sells_api', methods=['POST', 'GET'])
@login_required
def get_wb_pivot_sells_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':

        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(days=app.config['DAYS_STEP_DEFAULT'])
            date_from = date_from.strftime("%Y-%m-%d")

        print(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']

        df = detailing_reports.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales = detailing_reports.get_wb_sales_realization_pivot(df)
        df_stock = detailing_reports.get_wb_stock_api(date_from)
        df_net_cost = pd.read_sql(
            db.session.query(Product).filter_by(company_id=app.config['CURRENT_COMPANY_ID']).statement, db.session.bind)
        df = df_sales.merge(df_stock, how='outer', on='nm_id')
        df = df.merge(df_net_cost, how='outer', left_on='supplierArticle', right_on='article')
        df = detailing_reports.get_revenue_column(df)
        df = detailing_reports.get_important_columns(df)
        file = io_output.io_output(df)

        return send_file(file,
                         attachment_filename=f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_sales_realization_api', methods=['POST', 'GET'])
@login_required
def get_wb_sales_realization_api():
    """To get speed of sales for all products in period"""
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(days=app.config['DAYS_STEP_DEFAULT'])
            date_from = date_from.strftime("%Y-%m-%d")

        print(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']
        t = time.process_time()
        print(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = detailing_reports.get_wb_sales_realization_api(date_from, date_end, days_step)
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         attachment_filename=f"wb_sales_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_stock', methods=['POST', 'GET'])
@login_required
def get_wb_stock():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    df = detailing.get_wb_stock()
    file = io_output.io_output(df)

    return send_file(file, attachment_filename='report' + str(datetime.date.today()) + ".xlsx", as_attachment=True)


@app.route('/get_wb_stock_api', methods=['POST', 'GET'])
@login_required
def get_wb_stock_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(days=app.config['DAYS_STEP_DEFAULT'])
            date_from = date_from.strftime("%Y-%m-%d")

        print(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']
        t = time.process_time()
        print(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = detailing_reports.get_wb_stock_api()
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         attachment_filename='report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')

from app import app
from flask import render_template, request
from flask_login import login_required
import pandas as pd
from app.modules import img_processor, API_WB
from flask import send_file
from app.modules import yandex_disk_handler, request_handler
from app.modules.decorators import local_only


# Make folders for wb photo that way:
# we place in txt file 2 columns article our and article wb with sep = " "
# in folder img must be img with number 1...2...4 ect
# place it all in folder "folder_img"

# in txt for ozon is only art without size
# for ozon the name of photo must be from 0 to 9 on the end for example JBG-8954-AB-0.JPG, JBG-8954-AB-1.JPG...


@app.route('/watermark')
@login_required
@local_only
def watermark():
    """test watermark placing on image for example"""
    img_processor.img_watermark("NO8B9709.JPG", "NO8B9717.JPG")


@app.route('/images_foldering_yandisk', methods=['POST', 'GET'])
@login_required
@local_only
def images_foldering_yandisk():
    yandex_disk_handler.download_images_from_YandexDisk()
    return render_template('upload_images_foldering.html')


@app.route('/images_foldering', methods=['POST', 'GET'])
@login_required
@local_only
def images_foldering():
    """
    on 21.01.2022:
    Text is expected in first place!
    if via txt then it with one columns no name
    for wb is our article
    for ozon is article without -size (-38)
    Get images from local YandexDisk, preparing and foldering it on wb and ozon demand, and send it in zip
    on 08.08.2022 work only on local comp with pointing dict where img placed
    header in txt no need
    if good is wool then watermark will be placed like 150 x 300 см.
    For excel table there are first column is Article, second is Article_WB (in second column)
    then - folder will be named by it, if not - then our art.
    ALL - take all photo of articles in all folders
    ONLY_NEW - take photo only in one folder that will be seen
    ASCENDING - take photo from oldest (first) folders with that articles
    DESCENDING - take photo from last folders with that articles

    """
    if request.method == 'POST':
        df = request_handler.to_df(request, input_column="Article")
        df.columns = [col.strip() for col in df.columns]
        marketplace = request.form["multiply_number"]
        is_replace = request.form["is_replace"]
        order_is = request.form["order_is"]
        is_cards_from_yadisk = request.form.get("is_cards_from_yadisk")

        # print(df)

        # Use get_all_cards_api_wb to retrieve nmID values based on vendorCode
        if marketplace == "WB":
            if 'Article_WB' not in df.columns:
                print(f"Article_WB not in df.columns")

                df_nm_wb = API_WB.get_all_cards_api_wb(is_from_yadisk=is_cards_from_yadisk)
                df_nm_wb = df_nm_wb[['vendorCode', 'nmID']].drop_duplicates(subset=['nmID'])

                # Merge df and df_nm_wb on Article and vendorCode columns
                df = pd.merge(df, df_nm_wb, left_on="Article", right_on="vendorCode", how="left")
                # print (df)
                # Duplicate and rename nmID column to 'Article_WB'
                df['Article_WB'] = df['nmID'].copy()
                df['Article_WB'] = df['nmID'].apply(int).apply(str)
        if marketplace == "OZON":
            df['Article_WB'] = df['Article'].copy()

        # Remove duplicate rows based on the 'Article' column
        # df.drop_duplicates(subset='Article', keep='first', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Now merged_df contains nmID values along with other columns from df and df_nm_wb

        return_data = img_processor.img_foldering(df, marketplace, is_replace, order_is)
        return send_file(return_data, as_attachment=True, download_name='image_zip.zip')
    return render_template('upload_images_foldering.html', doc_string=images_foldering.__doc__)

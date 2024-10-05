import pandas as pd
from app import app
from flask import render_template, request, send_file
from flask_login import login_required
from app.modules import io_output, request_handler, parser_rating_module


@app.route('/parser_rating_wb', methods=['GET', 'POST'])
@login_required
def parser_rating_wb():
    """Parse rating and number of feedbacks using WB Article IDs."""
    if request.method == 'POST':

        col_name = 'Артикул'
        con_rating_name = 'Rating'
        con_feedbacks_name = 'Feedbacks'
        df = request_handler.to_df(request, input_column=col_name)
        print(df)

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame()

        if df.empty:
            df = parser_rating_module.batched_get_rating()
        else:
            # df[col_name] = ''
            df[con_rating_name] = ''
            df[con_feedbacks_name] = ''
            df = parser_rating_module.get_rating(df)

        file = io_output.io_output(df)
        return send_file(file, download_name="rating.xlsx", as_attachment=True)

    return render_template("upload_parser_rating_wb.html", doc_string=parser_rating_wb.__doc__)

import pandas as pd
from werkzeug.datastructures import FileStorage


def to_df(request, html_text_input_name='text_input', html_file_input_name='file', col_art_name='vendorCode'):
    """To get request and take from it text_input and make from it df, or take file and make df"""
    df = pd.DataFrame
    if request.form[html_text_input_name]:
        print(html_file_input_name)
        input_text = request.form[html_text_input_name]
        input_text = input_text.split(" ")
        df = pd.DataFrame(input_text, columns=[col_art_name])
        return df
    elif request.files[html_file_input_name]:
        print('HERE')
        input_txt = request.files[html_file_input_name]
        df = pd.read_csv(input_txt, sep='	', names=[col_art_name])
        if df[col_art_name][0] == col_art_name: df = df.drop([0, 0]).reset_index(drop=True)
        print(df)
        return df
    print('NONE')
    return df

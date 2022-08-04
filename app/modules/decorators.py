from functools import wraps
import flask
import pandas as pd


def flask_request_to_df(function):
    @wraps(function)
    def wrapper(flask_request, *args, **kwargs):
        files = flask_request.files.getlist("file")
        df = pd.read_excel(files[0])
        result = function(df, *args, **kwargs)
        return result

    return wrapper

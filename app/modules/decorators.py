from functools import wraps
import flask
import pandas as pd
from flask import redirect, flash
from flask_login import current_user


def flask_request_to_df(function):
    @wraps(function)
    def wrapper(flask_request, *args, **kwargs):
        files = flask_request.files.getlist("file")
        df = pd.read_excel(files[0])
        result = function(df, *args, **kwargs)
        return result

    return wrapper


def administrator_required(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        if current_user.role != 'administrator':
            flash(f'Для входа в раздел необходимы права администратора. Текущий статус {current_user.role}. '
                  f'Для изменения прав обратитесь к вашему администратору приложения. ')
            return redirect('/company_register')
        return function()

    return wrapper
from app import app
from functools import wraps
import pandas as pd
from flask import redirect, flash
from flask_login import current_user


def flask_request_to_df(function):
    # Не актуально на 09.02.2022 - заменено на request_handler.py
    @wraps(function)
    def wrapper(flask_request, *args, **kwargs):
        files = flask_request.files.getlist("file")
        dfs = []
        for idx, file in enumerate(files):
            dfs.append(pd.read_excel(files[idx]))
        result = function(dfs, *args, **kwargs)
        return result

    return wrapper


def administrator_required(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        print(f"decorator {current_user}")
        print(f"decorator {current_user.role}")
        if current_user:
            if current_user.role != app.config['ADMINISTRATOR_ROLE']:
                flash(f'Для входа в раздел необходимы права администратора. Текущий статус {current_user.role}. '
                      f'Для изменения прав обратитесь к вашему администратору приложения. ')
                return redirect('/profile')
        return function()

    return wrapper

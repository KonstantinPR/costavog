import logging
from app import app
import pandas as pd
from flask import redirect, flash
from flask_login import current_user
from threading import Thread
import psycopg2 
from os import environ
from flask import abort
import os
from functools import wraps
from flask import Response
import time
from datetime import datetime as dt



def local_only(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        environment = os.getenv('ENVIRONMENT', 'production')
        if environment != 'local':
            return abort(403)  # Forbidden
        return f(*args, **kwargs)

    return decorated_function


def keep_alive_decorator():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Function to keep PostgreSQL connection alive
            def keep_connection_alive():
                while True:
                    try:
                        # Establishing a connection to the PostgreSQL server
                        conn = psycopg2.connect(environ.get('DATABASE_URL'))
                        # Send a keep-alive query every minute
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT 1")
                            conn.commit()
                        # Sleep for 1 minute
                        time.sleep(10)
                        # Close the connection
                        conn.close()
                    except Exception as e:
                        logging.warning(f"Error: {e}")

            # Start a thread to keep the connection alive
            keep_alive_thread = Thread(target=keep_connection_alive)
            keep_alive_thread.start()

            # Execute the decorated function
            result = func(*args, **kwargs)

            # Wait for the thread to finish
            keep_alive_thread.join()

            return result

        return wrapper

    return decorator


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


def timing_decorator(original_function):
    @wraps(original_function)
    def wrapper_function(*args, **kwargs):
        print(f"Function '{original_function.__name__}' started at {dt.now().strftime('%d.%m.%Y %H:%M')}.")
        start_time = time.time()
        result = original_function(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Function '{original_function.__name__}' finished at {dt.now().strftime('%d.%m.%Y %H:%M')}.")
        print(f"It took {execution_time:.2f} seconds to execute.")
        return result
    return wrapper_function



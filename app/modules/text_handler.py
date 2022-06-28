import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
import pandas as pd
from app.modules.io_output import io_output
import numpy as np


def names_multiply(df, multiply_number):
    multiply_number = multiply_number + 1
    list_of_multy = []
    for art in df['Артикул']:
        for n in range(1, multiply_number):
            art_multy = f'{art}-{n}'
            list_of_multy.append(art_multy)
    df = pd.DataFrame(list_of_multy)
    return df

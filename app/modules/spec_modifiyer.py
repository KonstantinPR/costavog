from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
from app.models import Product, db
import datetime
import pandas as pd
from app.modules import detailing, detailing_reports, yandex_disk_handler, decorators
from app.modules import io_output
import time
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
from app.modules import text_handler, io_output
import numpy as np
from flask_login import login_required, current_user, login_user, logout_user



@decorators.flask_request_to_df
def data_transcript(flask_request) -> pd.DataFrame:
    df = flask_request
    return df

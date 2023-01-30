import pandas as pd
from app.modules import decorators


"""Module for function that most base for all project and cant't be put in any other module"""


@decorators.flask_request_to_df
def request_excel_to_df(flask_request) -> pd.DataFrame:
    df = flask_request
    return df

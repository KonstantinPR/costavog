from app import app
from functools import reduce
import math
import requests
from app.models import Product, db
import datetime
import pandas as pd
import numpy as np
from app.modules import yandex_disk_handler
from app.modules import io_output


def df_take_off_boxes(df, sep_boxes: str = 'end'):
    print(df)
    art_size_col = 'Артикул товара'
    art_col = 'Артикул'
    qt_col = 'Кол-во'
    sep_col = 'Разметка'
    box_number = 'Номер коробки'
    answer_col = 'Решение'
    already = 'Уже'
    can_be = 'Можно'

    num_box = 1
    for idx, val in enumerate(df[art_size_col]):
        df.at[idx, box_number] = num_box
        if df.at[idx, sep_col] == sep_boxes:
            num_box += 1

    for art_set in set(df[art_size_col]):
        qt = 0
        for idx, art_val in enumerate(df[art_size_col]):
            if df.at[idx, art_size_col] == art_set:
                qt = qt + 1
                df.at[idx, already] = qt

    for idx, art_val in enumerate(df[art_size_col]):
        box_num = df.at[idx, box_number]
        ar_val = df.at[idx, art_col]
        if df.at[idx, already] > df.at[idx, can_be]:
            dfx = df.loc[df[box_number] == box_num]
            print(art_val)
            print(dfx[art_size_col])
            print(art_val in dfx[art_size_col].values)
            if ar_val in dfx[art_col].values:
                if not len(set(dfx[art_col])) > 1:
                    print(len(set(dfx[art_col])))
                    print(f'box_num {box_num}')
                    df.at[idx, answer_col] = 'No'

    dc = dict(zip(df[box_number], df[answer_col]))
    df[answer_col] = df[box_number].map(dc)

    print(dc)

    print(df)

    return df

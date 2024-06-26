import pandas as pd
from app.modules import pandas_handler


def qt_to_order(df, min_stock=1):
    """
    stay only goods for order that 0 on wherehouse
    :param df:
    :return df:
    """
    false_list = pandas_handler.FALSE_LIST_2
    # df.to_excel('qt_to_order.xlsx')
    df['Кол-во'] = [min_stock if x in false_list else min_stock - x for x in df['quantityFull']]
    # df['Кол-во'] = [0 if x < 0 else x for x in df['Кол-во']]

    return df


def df_take_off_boxes(df, sep_boxes: str = 'end'):
    # print(df)
    art_size_col = 'Артикул товара'
    art_col = 'Артикул'
    if not art_col in df.columns: art_col = art_size_col
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

    # print(dc)

    # print(df)

    return df

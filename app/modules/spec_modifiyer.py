import pandas as pd
import numpy as np
from random import randrange

COL_ART_NAME = "Артикул товара"

PRICE_MULTIPLIER = lambda x: 40 / x ** 0.3
"""40 / 10000**0.3 = 2.52"""
"""40 / 1000**0.3 = 5.03"""
"""40 / 100**0.3 = 10.04"""

SPEC_TYPE = {
    'OAJCAPRON': 'APRON',
    'SK': 'ECO_FURS_WOMEN',
    'SH': 'ECO_FURS_WOMEN',
    'SHK': 'ECO_FURS_WOMEN',
    'SF': 'ECO_FURS_WOMEN',
    'MHOJ': 'ECO_FURS_WOMEN',
    'MHOC': 'ECO_FURS_WOMEN',
    'MHOF': 'ECO_FURS_WOMEN',
    'IAOC': 'ECO_FURS_WOMEN',
    'TIE': 'TIE',
    'MIT': 'MIT',
    'MHSU': 'SHOES',
    'MHBB': 'SHOES',
    'MHSD': 'SHOES',
    'MHSB': 'SHOES',
    'MHSORT': 'ORTOSHOES',
    'J': 'JEANS',
    'DEFAULT': 'DEFAULT',
}

JEANS_SIZES = {
    '23': '36',
    '24': '38',
    '25': '38-40',
    '26': '40',
    '27': '40-42',
    '28': '42',
    '29': '42-44',
    '30': '44',
    '31': '44-46',
    '32': '46',
    '33': '46-48',
    '34': '48',
    '35': '48-50',
    '36': '50',
    '37': '50-52',
    '38': '52',
    '39': '52-54',
    '40': '54-56',
    '41': '56'
}


def wrap_prefix_by_dash(prefix, i):
    if not prefix:
        return None
    if prefix == 'SK':
        return f"-{i}-"
    if prefix == 'SH':
        return f"{i}-"
    if prefix == 'SHK':
        return f"{i}-"
    if prefix == 'SF':
        return f"{i}-"
    return i


BEST_SIZES = [44, 42, 46, 40, 48, 50, 52, 54, 56, 58]


def spec_definition(df, col_name="Артикул товара"):
    print("spec_definition ...")
    # print(df)
    # print(df['Артикул товара'])
    # print(df['Артикул товара'][0].split('-')[0])
    if str(df[col_name][0]).startswith("SHK"):
        prefix = "SHK"
    elif str(df[col_name][0]).startswith("SH"):
        prefix = "SH"
    elif str(df[col_name][0]).startswith("J"):
        prefix = "J"
    elif str(df[col_name][0]).startswith("SK"):
        prefix = "SK"
    elif str(df[col_name][0]).startswith("SF"):
        prefix = "SF"
    else:
        prefix = df[col_name][0].split('-')[0]

    try:
        spec_type = SPEC_TYPE[prefix]
    except KeyError:
        spec_type = SPEC_TYPE['DEFAULT']

    return spec_type


def merge_spec(df1, df2, left_on=COL_ART_NAME, right_on=COL_ART_NAME, how='outer') -> pd.DataFrame:
    print("merge_spec ...")
    # print(df1)
    # print(df2)
    random_suffix = f'_col_on_drop_{randrange(10)}'
    df = df1.merge(df2, how=how, left_on=left_on, right_on=right_on, suffixes=('', random_suffix), sort=False)
    # print(df)
    for idx, col in enumerate(df.columns):
        if f'{col}{random_suffix}' in df.columns:
            for idj, val in enumerate(df[f'{col}{random_suffix}']):
                if not pd.isna(val):
                    df[col][idj] = val

    df = df.drop(columns=[x for x in df.columns if random_suffix in x])
    # df = df[df[on].notna()]

    return df


def picking_prefixes(df, df_art_prefixes, col_name="Артикул товара"):
    print("picking_prefixes ...")
    """to fill df on coincidence startwith and in"""
    # print(df_art_prefixes)
    # print(df)
    df['Префикс'] = ''
    df['Лекало'] = ''
    for idx, art in enumerate(df[col_name]):
        try:
            for idy, pattern in enumerate(df_art_prefixes["Лекало"]):
                for i in pattern.split():
                    art_prefix = df_art_prefixes['Префикс'][idy]
                    # print(art_prefix)
                    dash_prefix = wrap_prefix_by_dash(art_prefix, i)
                    # print(dash_prefix)
                    if dash_prefix in art and art.startswith(art_prefix):
                        df.at[idx, 'Лекало'] = pattern
                        df.at[idx, 'Префикс'] = df_art_prefixes.at[idy, 'Префикс']
                        break
        except KeyError:
            break

    return df


def picking_colors(df, df_colors,
                   df_col_name='Артикул товара',
                   df_colors_col_eng_name='Цвет английский',
                   df_colors_col_rus_name='Цвет русский'):
    """colors picking from english"""
    print("picking_colors ...")
    # print(df[df_col_name])
    # print(df_colors[df_colors_col_eng_name])
    for idx, art in enumerate(df[df_col_name]):
        for jdx, color in enumerate(df_colors[df_colors_col_eng_name]):
            # print(f'art {art}')
            # print(color)
            try:
                # if f'{color.upper()}' in art:
                if art.endswith(f'-{color.upper()}'):
                    # df['Цвет'][idx] = df_colors['Цвет русский'][jdx]
                    df.loc[idx, 'Цвет'] = df_colors.loc[jdx, df_colors_col_rus_name]
                    continue
            except:
                ValueError(f"color {color} can't be translated")
    return df


def df_clear(df_income, col_name="Артикул товара") -> pd.DataFrame:
    print("df_clear ...")
    df_income[col_name].replace('', np.nan, inplace=True)
    df_income.dropna(subset=[col_name], inplace=True)
    df_income = df_income.reset_index(drop=True)
    return df_income


def col_adding(df_income, col_name="Артикул товара"):
    print("col_adding ...")

    # Подбираем российские размеры, в большинстве случаев просто копируем родные размеры
    df_income['Рос. размер'] = ''
    print("sizes_pick ...")
    for idx, art in enumerate(df_income[col_name]):
        # print(idx)
        if not art.startswith('J'):
            df_income['Рос. размер'][idx] = df_income['Размер'][idx]

    # Наценку на закупочные цены с учетом малости цены себестоимости. Округляем результат красиво например 1990 или 790
    print("price_pick ...")
    if 'Цена' in df_income.columns:
        df_income['Цена'] = [round(x * PRICE_MULTIPLIER(x), -(int(len(str(int(x)))) - 2)) - 10 for x in
                             df_income['Цена']]

    # дополняем описание для светлых изделий - как возможно подходящие к свадебному наряду
    print("desc_white ...")
    random_wedding_desc = [
        f' Дополнительный аксессуар к свадебному образу.',
        f' Теплый аксессуар к свадебному платью в прохладный сезон.',
        f' Незаменимый атрибут к свадебному наряду в холодный сезон.',
        f' Отличный аксессуар к образу невесты и незаменимый атрибут к свадебному платью в прохладный сезон.'
    ]
    wedding_desc = random_wedding_desc[randrange(len(random_wedding_desc))]

    print("color ...")
    if 'Цвет' not in df_income.columns:
        # add 'Цвет' column to df
        df_income['Цвет'] = ''

    for idx, color in enumerate(df_income['Цвет']):
        if color in ['белый', 'молочный', 'светло-бежевый', 'бежевый'] and df_income['Префикс'][idx] == 'SK':
            df_income['Описание'][idx] += wedding_desc

    print("number_card_pattern ...")
    # нумеруем карточки на основе лекал, если нет лекал - на основе одинаковых артикулей
    set_patterns = set(df_income['Лекало'])
    set_art = set(df_income[col_name])
    # print(f'set_art {set_art}')
    # print(f'len_patterns {len(set_patterns)}')
    dict_patterns = {k: v for v, k in enumerate(set_patterns, 1)}
    # print(f'dict_arts {dict_patterns}')
    dict_arts = {k: v for v, k in enumerate(set_art, len(set_art) + len(set_patterns) + 1)}
    # print(f'dict_arts {dict_arts}')

    number_card_col_name = 'Номер карточки'
    if not number_card_col_name in df_income.columns:
        df_income[number_card_col_name] = ''
    for idx, pattern in enumerate(df_income['Лекало']):
        # print(f'idx {idx} and pattern {pattern} and dict_patterns[patterns] {dict_patterns[pattern]}')
        # if dict_patterns[pattern] and not df_income[number_card_col_name][idx]:
        if dict_patterns[pattern] and not df_income[number_card_col_name][idx]:
            df_income[number_card_col_name][idx] = dict_patterns[pattern]

    print("number_card_art ...")
    for idx, art in enumerate(df_income[col_name]):
        if not df_income[number_card_col_name][idx]:
            df_income[number_card_col_name][idx] = dict_arts[art]

    # создаем дополнительный столбец равный артикулу поствщика
    print("art_duplicate ...")
    df_income[col_name] = df_income[col_name]

    # создаем дополнительный столбец равный категории
    print("equil_category ...")
    if not 'Категория' in df_income.columns and 'Предмет' in df_income.columns:
        df_income['Категория'] = df_income['Предмет']

    # df_income.to_excel('df_income.xlsx')
    return df_income


def col_str(df, lst: list):
    # print(df)
    print("col_str ...")
    for col in lst:
        if col in df.columns:
            df[col] = [str(x) if not pd.isna(x) else x for x in df[col]]
    return df


def sizes_translate(df, spec_type):
    print("sizes_translate ...")
    if spec_type.startswith("J"):
        df['Рос. размер'] = df['Размер'].map(JEANS_SIZES)
    return df

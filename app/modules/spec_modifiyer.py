import pandas as pd
from app.modules import decorators
import numpy as np
from random import randrange

PRICE_MULTIPLIER = lambda x: 40 / x ** 0.3
"""40 / 10000**0.3 = 2.52"""
"""40 / 1000**0.3 = 5.03"""
"""40 / 100**0.3 = 10.04"""

SPEC_TYPE = {
    'OAJCAPRON': 'APRON',
    'SK': 'ECO_FURS_WOMEN',
    'MIT': 'MIT',
    'MHSU': 'SHOES',
}

BEST_SIZES = [44, 42, 46, 40, 48, 50, 52, 54, 56, 58]


@decorators.flask_request_to_df
def request_to_df(flask_request) -> pd.DataFrame:
    df = flask_request
    return df


def spec_definition(df):
    print(df)
    print(df['Артикул товара'])
    print(df['Артикул товара'][0].split('-')[0])
    prefix = df['Артикул товара'][0].split('-')[0]
    if SPEC_TYPE[prefix]:
        spec_type = SPEC_TYPE[prefix]
    else:
        spec_type = SPEC_TYPE['DEFAULT']
    print(spec_type)
    return spec_type


def vertical_size(df, col: str = 'Размеры', col_re='Размер'):
    if col in df.columns:
        re_size = [str(x).split(' ') if ' ' in str(x) else str(x) for x in df[col]]
        print(re_size)
        df = df.assign(temp_col=re_size).explode('temp_col', ignore_index=True)
        df = df.drop(columns=col, axis=1)
        df = df.rename({'temp_col': col_re}, axis='columns')
        print(df)

    # OLD RIGHT
    # lst_art = []
    # lst_sizes = [x.split() for x in df['Размеры']]
    # for i in range(len(lst_sizes)):
    #     for j in range(len(lst_sizes[i])):
    #         lst_art.append(df['Артикул полный'][i])
    #
    # lst_sizes = sum(lst_sizes, [])
    # df = pd.DataFrame({'Артикул полный': lst_art, 'Размеры': lst_sizes})

    return df


def to_keep_for_photo(df, size_col_name='Размер', art_col_name='Артикул товара', is_photo_col_name='На фото'):
    f"""На вход подается Data Frame с вертикальными размерами и столбцами 
    {art_col_name}, {size_col_name}, {is_photo_col_name}.
    На выходе получаем отмеченные в столбце {is_photo_col_name} как 1 размеры для фотосессии"""

    qt_col_name = 'Кол-во'
    arts = list(set(df.loc[df[is_photo_col_name] == 1, art_col_name]))

    for size in BEST_SIZES:
        for idx, value in enumerate(df[is_photo_col_name]):
            if value and df[art_col_name][idx] in arts and str(df[size_col_name][idx]) == str(size):
                arts.remove(df[art_col_name][idx])
                df[is_photo_col_name][idx] = 2

    df[is_photo_col_name] = ['' if x == 1 else x for x in df[is_photo_col_name]]
    df[is_photo_col_name] = df[is_photo_col_name].replace(2, 1)
    df[qt_col_name] = 1
    re_cols = [art_col_name, size_col_name, qt_col_name, is_photo_col_name]
    df = df[re_cols]

    return df


def merge_spec(df1, df2, left_on='Артикул товара', right_on='Артикул товара', how='outer') -> pd.DataFrame:
    print(df1)
    print(df2)
    random_suffix = f'_col_on_drop_{randrange(10)}'
    df = df1.merge(df2, how=how, left_on=left_on, right_on=right_on, suffixes=('', random_suffix), sort=False)
    print(df)
    for idx, col in enumerate(df.columns):
        if f'{col}{random_suffix}' in df.columns:
            for idj, val in enumerate(df[f'{col}{random_suffix}']):
                if not pd.isna(val):
                    df[col][idj] = val

    df = df.drop(columns=[x for x in df.columns if random_suffix in x])
    # df = df[df[on].notna()]

    return df


def picking_prefixes(df, df_art_prefixes):
    """to fill df on coincidence startwith and in"""
    print(df_art_prefixes)
    print(df)
    df['Префикс'] = ''
    df['Лекало'] = ''
    for idx, art in enumerate(df['Артикул товара']):
        for idy, pattern in enumerate(df_art_prefixes["Лекало"]):
            for i in pattern.split():
                print(f"i {i} pattern {pattern}")
                if f'-{i}-' in art and art.startswith(df_art_prefixes['Префикс'][idy]):
                    print(f"idx {idx} idy {idy} i {i} art {art} pre {df_art_prefixes['Префикс'][idy]} patt {pattern}")
                    # df['Лекало'][idx] = pattern
                    df.at[idx, 'Лекало'] = pattern
                    # df['Префикс'][idx] = df_art_prefixes['Префикс'][idy]
                    df.at[idx, 'Префикс'] = df_art_prefixes.at[idy, 'Префикс']
                    print(
                        f"art {art} pattern {pattern} df.at[idx, 'Лекало'] {df.at[idx, 'Лекало']} df.at[idx, 'Префикс'] {df.at[idx, 'Префикс']} ")
                    break
    return df


def picking_colors(df, df_colors):
    """colors picking from english"""
    for idx, art in enumerate(df['Артикул товара']):
        for jdx, color in enumerate(df_colors['Цвет английский']):
            print(f'art {art}')
            if f'-{color.upper()}' in art:
                # df['Цвет'][idx] = df_colors['Цвет русский'][jdx]
                df.loc[idx, 'Цвет'] = df_colors.loc[jdx, 'Цвет русский']
    return df


def df_clear(df_income) -> pd.DataFrame:
    df_income['Артикул товара'].replace('', np.nan, inplace=True)
    df_income.dropna(subset=['Артикул товара'], inplace=True)
    return df_income


def col_adding(df_income):
    # Подбираем российские размеры, в большинстве случаев просто копируем родные размеры
    df_income['Рос. размер'] = ''
    df_income['Номер карточки'] = ''
    for idx, art in enumerate(df_income['Артикул товара']):
        if not art.startswith('J'):
            df_income['Рос. размер'][idx] = df_income['Размер'][idx]

    # Наценку на закупочные цены с учетом малости цены себестоимости. Округляем результат с маркетинговым приемом
    df_income['Цена'] = [round(x * PRICE_MULTIPLIER(x), -(int(len(str(int(x)))) - 2)) - 10 for x in df_income['Цена']]

    # дополняем описание для светлых изделий - как возможно подходящие к свадебному наряду
    for idx, color in enumerate(df_income['Цвет']):
        if color in ['белый', 'молочный', 'светло-бежевый', 'бежевый'] and df_income['Префикс'][idx] == 'SK':
            df_income['Описание'][idx] += f' Дополнительный аксессуар к свадебному гардеробу'

    # нумеруем карточки на основе лекал, если нет лекал - на основе одинаковых артикулей
    set_patterns = set(df_income['Лекало'])
    set_art = set(df_income['Артикул товара'])
    print(f'set_art {set_art}')
    print(f'len_patterns {len(set_patterns)}')
    dict_patterns = {k: v for v, k in enumerate(set_patterns, 1)}
    print(f'dict_arts {dict_patterns}')
    dict_arts = {k: v for v, k in enumerate(set_art, len(set_art) + len(set_patterns) + 1)}
    print(f'dict_arts {dict_arts}')

    for idx, pattern in enumerate(df_income['Лекало']):
        print(f'idx {idx} and pattern {pattern} and dict_patterns[patterns] {dict_patterns[pattern]}')
        if dict_patterns[pattern]:
            df_income['Номер карточки'][idx] = dict_patterns[pattern]

    for idx, art in enumerate(df_income['Артикул товара']):
        if not df_income['Номер карточки'][idx]:
            df_income['Номер карточки'][idx] = dict_arts[art]

    return df_income


def col_str(df, lst: list):
    print(df)
    for col in lst:
        if col in df.columns:
            df[col] = [str(x) if not pd.isna(x) else x for x in df[col]]
    return df

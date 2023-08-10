import requests
from bs4 import BeautifulSoup
import pandas as pd
import math
import numpy as np
import random
from typing import Union

NORMAL_TURNOVER = 20
NORMAL_REWARD = 1000
NORMAL_PROFITABILITY = 1
NORMAL_STOCK = 20
MIN_STEP_DISCOUNT = 1

# Шаг для скидки в %
STEP_DISCOUNT = 10


def pow(normal_reward: float, current_reward: float) -> float:
    if current_reward > normal_reward:
        return 0.5
    elif current_reward > normal_reward / 2:
        return 0.75
    else:
        return 1


def analize_discount(profitability: float, turnover: float, turnover_all: float, profit, day_on_site,
                     stock: int) -> Union[float, None]:
    if turnover_all == 499.5:
        if stock > 100:
            return MIN_STEP_DISCOUNT * 6
        if stock > 50:
            return MIN_STEP_DISCOUNT * 4
        if stock > 25:
            return MIN_STEP_DISCOUNT * 2
        return MIN_STEP_DISCOUNT
    elif turnover_all == 999:
        if profit > 0 and day_on_site > 28:
            if stock > 100:
                return MIN_STEP_DISCOUNT * 8
            if stock > 50:
                return MIN_STEP_DISCOUNT * 6
            if stock > 25:
                return MIN_STEP_DISCOUNT * 4
            return MIN_STEP_DISCOUNT * 2
        return MIN_STEP_DISCOUNT
    elif profitability > 0 and turnover > 0:
        disc = -(profitability + turnover) / 2
    elif profitability > 0 and turnover < 0:
        disc = -(turnover)
    elif profitability < 0 and turnover > 0:
        disc = -(turnover)
    elif profitability < 0 and turnover < 0:
        if profit > 0:
            disc = -max([profitability, turnover])
        else:
            disc = max([profitability, turnover])
    else:
        return None

    return round(STEP_DISCOUNT * disc, 0)


def nice_price(plan_delta_discount: float, current_price: float) -> float:
    """формирование цены в виде 995"""
    new_price = int(round(current_price * (1 - plan_delta_discount / 100), 0))
    nice_new_price = round(new_price, -2) - random.randint(1, 2) * 5
    return nice_new_price


def sigmoid(x: float = 1, k: float = 1, a: float = 0.3, b: float = -1, ) -> float:
    """
    Сигмоида - функция для сглаживания величины при минимальных и максимальных значениях

    """
    return (k / (0.5 + math.exp(a + b * x))) - 1


def merge_dataframes(excel1_path: str, excel2_path: str, left_on: str, right_on: str) -> pd.DataFrame:
    excel_data_df1 = pd.read_excel(excel1_path)
    excel_data_df2 = pd.read_excel(excel2_path)
    df = excel_data_df1.merge(excel_data_df2, left_on=left_on, right_on=right_on, how='outer')
    return df


def is_img(art: str, r: float) -> None:
    url_site = "https://www.wildberries.ru/catalog/" + str(art) + "/detail.aspx?targetUrl=IN"

    if r == 999 or r == 499.5:
        if BeautifulSoup(requests.get(url_site).content).find('img', {'class': 'preview-photo j-zoom-preview'}):
            image = BeautifulSoup(requests.get(url_site).content).find('img', {'class': 'preview-photo j-zoom-preview'})
            if image:
                img_src = image['src']
                print(img_src)
                return img_src
    return None


def is_discount_possible(x: int, y: int) -> int:
    if x <= 0:
        if y >= 3:
            return 3
        else:
            return ""
    elif 0 <= x <= 3:
        if x < y:
            return 3
        else:
            return ""
    else:
        return x


def discount(file_turnover: pd.DataFrame, file_net_cost: pd.DataFrame) -> pd.DataFrame:
    """
        get the information from excel file from WB that contains price, discount, quantity etc
        and work with it
        :return: name of saving file
    """
    df = file_turnover.merge(file_net_cost, left_on='Артикул продавца', right_on='article', how='outer')
    df.replace("Товар на сайте менее 30 дн.", int(round(999 / 2)), inplace=True)
    df.replace(np.NaN, 0, inplace=True)
    df = df[(df['Остаток товара (шт.)'] > 0) & (df['Оборачиваемость'] is not None)]
    df['Оборачиваемость'] = [int(x) for x in df['Оборачиваемость']]
    # ///   БЛОК КОЭФФИЦИЕНТОВ   ///

    # Кол-во дней в расчетном периоде
    D = 7

    X = "Текущая розн. цена (до скидки)"
    A = "Текущая скидка на сайте, %"
    B = "Текущая скидка по промокоду, %"
    # Скидка покупателя % - брать для каждой торговой марки или каждой Api WB
    C = 0.05
    # Коэффициент гарантированного вознаграждения WB % - брать для каждого товара из Api WB
    K = 0.15
    # Хранение WB % - высчитывать из Api
    H = 0.05
    # Логистика WB % - высчитывать из Api
    L = 0.1

    # ///   БЛОК ЛОГИКИ - РАССЧЕТЫ СКИДОК   ///
    # Цена, которая отображается на сайте WB с учетом всех скидок
    df["Текущая конечная цена"] = df[X] * (1 - df[A] / 100) * (1 - df[B] / 100)
    # Получаем наше вознаграждение с учетом всех вычетов и скидок
    df["Вознаграждение с единицы"] = df[X] * (1 - df[A] / 100) * (1 - df[B] / 100) * (1 - C) * (1 - K) * (1 - L)
    # Прибыльность артикула с учетом всех вычетов, скидок и себестоиости товара
    df["Прибыльность с единицы"] = df["Вознаграждение с единицы"] - df["net_cost"]

    # df["Наличие фотографии"] = [is_img(b, c) for b, c in zip(df["Номенклатура (код 1С)"], df["Оборачиваемость"])]

    # Используем поправочный коэффициент для товаров у которых якобы очень низкая оборачиваемость - по факту может и нет
    df["Оборачиваемость ед."] = [x / (x * 5 / x ** 1.25) for x in df["Оборачиваемость"] / df["Остаток товара (шт.)"]]

    df["Рек. оборачиваемость"] = [NORMAL_TURNOVER * (i / NORMAL_REWARD) ** pow(NORMAL_REWARD, i)
                                  for i in df["Вознаграждение с единицы"]]

    df["Рек. оборачиваемость поправка Остаток"] = df["Рек. оборачиваемость"] * (1 + 1 / df["Остаток товара (шт.)"])

    df["Отклонение в % Оборачиваемость"] = df["Рек. оборачиваемость поправка Остаток"] / df["Оборачиваемость ед."]

    df["Рентабельность"] = df["Прибыльность с единицы"] / df["net_cost"]

    df["Рек. рентабельность"] = [NORMAL_PROFITABILITY / (i / NORMAL_REWARD) ** pow(NORMAL_REWARD, i)
                                 for i in df["Вознаграждение с единицы"]]

    df["Отклонение в % от Рентабельности"] = df["Рентабельность"] / df["Рек. рентабельность"]

    df["Рек. поправка к скидке - Рентабельность"] = [sigmoid(x) for x in df["Отклонение в % от Рентабельности"]]
    df["Рек. поправка к скидке - Оборачиваемость"] = [sigmoid(x) for x in df["Отклонение в % Оборачиваемость"]]

    df["Рек. поправка к скидке"] = [analize_discount(b, c, d, e, f, g) for b, c, d, e, f, g in
                                    zip(df["Рек. поправка к скидке - Рентабельность"],
                                        df["Рек. поправка к скидке - Оборачиваемость"],
                                        df["Оборачиваемость"],
                                        df["Прибыльность с единицы"],
                                        df["Количество дней на сайте"],
                                        df['Остаток товара (шт.)'],
                                        )]

    df["Скидочная маркетинговая цена"] = [nice_price(c, d) for c, d in
                                          zip(df["Рек. поправка к скидке"],
                                              df["Текущая конечная цена"],
                                              )]

    df["Согласованная скидка, %"] = df["Текущая скидка на сайте, %"] + df["Рек. поправка к скидке"]

    df["Согласованная скидка, %"] = [is_discount_possible(x, y) for x, y in
                                     zip(df["Согласованная скидка, %"],
                                         df["Текущая скидка на сайте, %"],
                                         )]

    df.reset_index()

    return df

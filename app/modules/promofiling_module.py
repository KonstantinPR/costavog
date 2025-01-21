import numpy as np
from app.modules import pandas_handler



def check_discount(df, allowed_delta_percent=5):
    # plan_price_name = "Плановая цена для акции"
    current_price_name = "Текущая розничная цена"
    new_discount_col = "new_discount"
    promo_discount_name = "Загружаемая скидка для участия в акции"

    promo_discount_name_actual = "Загружаемая скидка для участия в акции исправленная"
    # to fix the wildberries count price percentage discount promo bug
    # df[promo_discount_name_actual] = (1 - df[plan_price_name] / df[current_price_name]) * 100
    # df[promo_discount_name_actual] = df[promo_discount_name_actual].apply(pandas_handler.false_to_null)
    # df[promo_discount_name_actual] = df[promo_discount_name_actual].apply(lambda x: math.ceil(x))

    # Calculate the discount price
    df["discount_price"] = df[current_price_name] * (1 - df[new_discount_col] / 100)

    # Calculate the price difference
    # df["price_difference"] = df[plan_price_name] / df["discount_price"]
    # df.to_excel("df_promo.xlsx")
    false_list = pandas_handler.NAN_LIST

    df[promo_discount_name] = np.where(df[promo_discount_name].isin(false_list), df[new_discount_col],
                                       df[promo_discount_name])

    df["action_price"] = df[current_price_name] * (1 - df[promo_discount_name] / 100)
    df["price_difference"] = df["action_price"] / df["discount_price"]

    # Apply the discount condition
    allowed_ratio = 1 - allowed_delta_percent / 100

    # Store original promo discounts
    df["Загружаемая скидка для участия в акции_old"] = df[promo_discount_name]
    df["Allowed"] = "Yes"

    # Update promo discounts based on allowed delta percent
    df.loc[df["price_difference"] >= allowed_ratio, new_discount_col] = df[promo_discount_name]
    df.loc[df["price_difference"] < allowed_ratio, new_discount_col] = df[new_discount_col]
    df.loc[df["price_difference"] < allowed_ratio, "Allowed"] = "No"
    # df.to_excel("df_promo2.xlsx")

    return df

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from collections import defaultdict
import pickle
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import f1_score
from math import sqrt
from sklearn.impute import SimpleImputer
import numpy as np


def train(file_name, encoded_col_name, goal_col):
    # load data
    data = pd.read_excel(file_name)
    data, d = mapping_categorical(data, encoded_col_name)
    data.to_excel('data_converted.xlsx')

    # split data into training and test sets
    X = data.drop(columns=[goal_col])
    y = data[goal_col]
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)

    # create an instance of the SimpleImputer class
    imputer = SimpleImputer(missing_values=np.nan, copy=False, strategy="mean")

    # fit and transform the SimpleImputer on the training set
    X_train = imputer.fit_transform(X_train)
    X_test = imputer.transform(X_test)

    model = train_model_forest(X_train, X_test, y_train, y_test)

    # evaluate model on test set
    score = model.score(X_test, y_test)
    print(f"Model accuracy: {score}")

    # make predictions on test set
    y_pred = model.predict(X_test)

    # calculate MSE and RMSE
    mse = mean_squared_error(y_test, y_pred)
    rmse = sqrt(mse)

    print(f"MSE: {mse}, RMSE: {rmse}")

    return model


def predict(model, data, encoded_col_name, goal_col, default_encoder):
    data = nan_correction(data)
    # Encode the categorical variables

    if set(encoded_col_name).issubset(data.columns):
        data, default_encoder = mapping_categorical(data, encoded_col_name, default_encoder=default_encoder)

    # Make predictions
    predictions = model.predict(data)

    # Add predictions as a new column in the dataframe
    data[goal_col] = [int(x) for x in predictions]
    print(f"data['goal_col] = {data[goal_col]}")
    # Decode the categorical variables
    data = mapping_categorical(data, encoded_col_name, inverse=True, default_encoder=default_encoder)

    return data


def redundant_col_name(df_train, df_predict):
    col_to_be = [x for x in df_predict if x in df_train]
    df_predict = df_predict[col_to_be]
    print(f"col_to_be {col_to_be}")
    return df_predict


def mapping_categorical(data: pd.DataFrame, col_to_mapping: list = None, inverse=False, default_encoder=None):
    # Encoding the variable
    if not default_encoder:
        default_encoder = defaultdict(LabelEncoder)
    if inverse:
        data[col_to_mapping] = data[col_to_mapping].apply(lambda x: default_encoder[x.name].inverse_transform(x))
        return data
    data[col_to_mapping] = data[col_to_mapping].apply(lambda x: default_encoder[x.name].fit_transform(x))
    print(f"d = {default_encoder}")
    return data, default_encoder


def nan_correction(data):
    for col in data.columns:
        data[col] = data[col].fillna(data[col].mode().iat[0])
        print(data[col])
    return data


def get_mae(model, max_leaf_nodes, X_test, y_test):
    preds_val = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds_val)
    print(f"Max leaf nodes: {max_leaf_nodes}, Mean Absolute Error: {mae}")
    return mae


def train_model_tree(X_train, X_test, y_train, y_test, depth_leaf_list=[5, 50, 500, 5000], random_state=0):
    # compare MAE with different values of max_leaf_nodes
    best_mae, best_model = float('inf'), None
    for max_leaf_nodes in depth_leaf_list:
        model = RandomForestRegressor(random_state=random_state)
        model.fit(X_train, y_train)
        mae = get_mae(model, max_leaf_nodes, X_test, y_test)
        if mae < best_mae:
            best_mae = mae
            best_model = model
    return best_model


def train_model_forest(X_train, X_test, y_train, y_test, random_state=0):
    model = RandomForestRegressor(n_estimators=100, random_state=random_state)
    model.fit(X_train, y_train)
    preds_val = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds_val)
    print(f"Mean Absolute Error: {mae}")

    return model

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from collections import defaultdict
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error
from math import sqrt
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100


def process_date(df, date_col=None):
    if date_col in df:
        df[date_col] = [int(str(x).split()[0].replace('-', '')) for x in df[date_col]]
    return df


def xgb():
    # Load the data
    df = pd.read_excel("clothes_dataset_train.xlsx")
    # Convert produced_data to numerical representation
    df = process_date(df, 'produced_date')
    # Split the data into features and target
    X = df.drop("price", axis=1)
    y = df["price"]

    # Split the data into training and validation sets
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=0)

    # Define the pipeline
    pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OneHotEncoder(handle_unknown='ignore')),
        ("scaler", StandardScaler(with_mean=False)),
        ("model", XGBRegressor()),
    ])

    # Fit the pipeline on the training data
    pipeline.fit(X_train, y_train)

    # Predict on the validation data
    y_pred = pipeline.predict(X_val)

    # Calculate MAE
    mae = mean_absolute_error(y_val, y_pred)
    mape = mean_absolute_percentage_error(y_val, y_pred)
    print(f"MAE: {mae:.4f}")
    print(f"MAPE: {mape:.4f}%")

    df_test = pd.read_excel("clothes_dataset_test.xlsx")
    df_test = process_date(df_test, 'produced_date')
    X_2_val = df_test.drop("price", axis=1)
    y_test_predict = pipeline.predict(X_2_val)
    df_test['price_predict'] = [int(x) for x in y_test_predict]

    return df_test


def train(data, encoded_col_name, goal_col):
    # load data

    data = nan_correction(data)
    data, d = mapping_categorical(data, encoded_col_name)
    # data.to_excel('data_converted.xlsx')

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


def predict(model, data_test, encoded_col_name, goal_col, default_encoder):
    data_test = nan_correction(data_test)
    # Encode the categorical variables

    if set(encoded_col_name).issubset(data_test.columns):
        data_test, default_encoder = mapping_categorical(data_test, encoded_col_name, default_encoder=default_encoder)

    # Make predictions
    predictions = model.predict(data_test)

    # Add predictions as a new column in the dataframe
    data_test[goal_col] = [int(x) for x in predictions]
    print(f"data['goal_col] = {data_test[goal_col]}")
    # Decode the categorical variables
    data_test = mapping_categorical(data_test, encoded_col_name, inverse=True, default_encoder=default_encoder)

    return data_test


def redundant_col_name(df_train, df_test):
    col_to_be = [x for x in df_test if x in df_train]
    df_predict = df_test[col_to_be]
    print(f"col_to_be {col_to_be}")
    return df_predict


def mapping_categorical(data: pd.DataFrame, col_to_mapping: list = None, inverse=False, default_encoder=None):
    # Encoding the variable
    if not default_encoder:
        default_encoder = defaultdict(LabelEncoder)
    if inverse:
        data[col_to_mapping] = data[col_to_mapping].apply(lambda x: default_encoder[x.name].inverse_transform(x))
        return data
    print(f"data[col_to_mapping] {data[col_to_mapping]}")
    data[col_to_mapping] = data[col_to_mapping].apply(lambda x: default_encoder[x.name].fit_transform(x))
    print(f"d = {default_encoder}")
    return data, default_encoder


def nan_correction(df):
    for col in df.columns:
        df = df.dropna(subset=[col])
        # data[col] = data[col].fillna(data[col].mode().iat[0])
        # print(data[col])
    return df


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

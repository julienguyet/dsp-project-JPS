import os
import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler


def filling_na_train(df: pd.DataFrame, columns: list) -> pd.DataFrame:

    df.dropna(subset = ['Date'], inplace=True)
    df['MarkDown1'].fillna(value=0, inplace=True)
    df['MarkDown2'].fillna(value=0, inplace=True)
    df['MarkDown3'].fillna(value=0, inplace=True)
    df['MarkDown4'].fillna(value=0, inplace=True)
    df['MarkDown5'].fillna(value=0, inplace=True)
    df['CPI'].fillna(value=0, inplace=True)
    df['Unemployment'].fillna(value=0, inplace=True)
    df['IsHoliday'].fillna(value=False, inplace=True)
    df['Type'].fillna(value='A', inplace=True)
    return df

def create_time_feature(df: pd.DataFrame) -> pd.DataFrame:
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['dayofmonth'] = df['Date'].dt.day
    df['dayofweek'] = df['Date'].dt.dayofweek
    df['quarter'] = df['Date'].dt.quarter
    df['month'] = df['Date'].dt.month
    df['year'] = df['Date'].dt.year
    df['dayofyear'] = df['Date'].dt.dayofyear
    df['weekofyear'] = df['Date'].dt.isocalendar().week
    return df


def cpi_difference(df: pd.DataFrame) -> pd.DataFrame:
    differences = []

    for rows in range(1, len(df)):
        value_0 = float(df['CPI'].iloc[rows - 1])
        value_1 = float(df['CPI'].iloc[rows])
        result = value_1 - value_0
        differences.append(result)
    
    df['CPI_Difference'] = [0] + differences
    return df


def data_split(df: pd.DataFrame) -> pd.DataFrame:
    X_train, X_test, y_train, y_test = train_test_split(df.drop(columns=['Store','Weekly_Sales', 'Date', 'Fuel_Price',
                                                                         'CPI', 'Unemployment', 'Type']), df['Weekly_Sales'])

    return X_train, X_test, y_train, y_test


def train_data_encoder(df: pd.DataFrame, path: str) -> pd.DataFrame:

    #bool_cols = df.select_dtypes(include=bool).columns
    #df[bool_cols] = df[bool_cols].astype(str)

    encoder = OneHotEncoder(handle_unknown='ignore')
    encoder.fit(df)
    X_train_encoded = encoder.transform(df)
    encoder_path = os.path.join(path, 'one-hot-encoder.joblib')
    joblib.dump(encoder, encoder_path)
    return X_train_encoded


def test_data_encoder(df: pd.DataFrame, path: str) -> pd.DataFrame:

    encoder_path = os.path.join(os.path.dirname(__file__), '..', 'models', 'one-hot-encoder.joblib')
    encoder = joblib.load(encoder_path)
    X_test_encoded = encoder.transform(df)

    return X_test_encoded


def train_data_scaler(data: np.array, path: str) -> np.array:

    scaler = MinMaxScaler()
    scaler.fit(data)
    X_train_scaled = scaler.transform(data)
    scaler_path = os.path.join(path, 'min-max-scaler.joblib')
    joblib.dump(scaler, scaler_path)

    return X_train_scaled


def test_data_scaler(data: np.array, path: str) -> np.array:

    scaler = joblib.load(os.path.join(path, 'min-max-scaler.joblib'))
    X_test_scaled = scaler.transform(data)

    return X_test_scaled
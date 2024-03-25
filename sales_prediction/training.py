import os
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sales_prediction.preprocessing import *
from sales_prediction.__init__ import *
import sys
sys.path.append('./sales_prediction')


def build_model(data: pd.DataFrame) -> dict[str, str]:
    df = filling_na_train(data, data.columns)
    df = create_time_feature(df)
    df = cpi_difference(df)
    X_train, X_test, y_train, y_test = data_split(df)
    X_train_encoded = train_data_encoder(df=X_train, path=MODEL_BASE_PATH)
    X_test_encoded = test_data_encoder(df=X_test, path=MODEL_BASE_PATH)

    model = RandomForestRegressor(max_depth=10, random_state=0)
    model.fit(X_train_encoded, y_train)

    model_path = os.path.join(MODEL_BASE_PATH, 'random-forest.joblib')
    joblib.dump(model, model_path)

    y_pred_train = model.predict(X_train_encoded)
    y_pred_test = model.predict(X_test_encoded)

    train_accuracy = model.score(X_train_encoded, y_train)
    test_accuracy = model.score(X_test_encoded, y_test)

    accuracy = {"Train accuracy score": train_accuracy, "Test accuracy score": test_accuracy}

    return accuracy

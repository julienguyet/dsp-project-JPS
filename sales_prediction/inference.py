import joblib
import numpy as np
import pandas as pd
from sales_prediction.preprocessing import *
from sales_prediction.__init__ import *
import sys
sys.path.append('./sales_prediction')


'''def make_predictions(input_data: pd.DataFrame) -> np.ndarray:

    df = create_time_feature(input_data)
    df = cpi_difference(df)
    clean_data = filling_na(df, TEST_FEATURES)
    clean_data.drop(columns=FEATURES_TO_DROP, inplace=True)
    X_test_encoded = test_data_encoder(df=clean_data, path=MODEL_BASE_PATH)
    model = joblib.load('../models/random-forest.joblib')
    Y = model.predict(X_test_encoded)

    return {"Here are our predictions using Random Forest": Y}'''


def make_predictions(input_data: pd.DataFrame) -> dict:
    
    # Preprocess the data and make predictions
    df = filling_na_train(input_data, input_data.columns)
    df = create_time_feature(df)
    df = cpi_difference(df)
    
    df.drop(columns=FEATURES_TO_DROP, inplace=True)
    X_test_encoded = test_data_encoder(df=df, path=MODEL_BASE_PATH)
    
    # Load the model and make predictions
    model_path = os.path.join(os.path.dirname(__file__), '..', 'models', 'random-forest.joblib')
    model = joblib.load(model_path)
    Y = model.predict(X_test_encoded)

    # Return the predictions as a dictionary
    return {"Sales": Y}

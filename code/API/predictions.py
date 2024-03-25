from fastapi import FastAPI
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath('../..'))
from sales_prediction.inference import *


# Initialize FastAPI app
app = FastAPI()

# Create SQLAlchemy engine
SQLALCHEMY_DATABASE_URL = "postgresql://postgres:Lufiva@localhost:5432/postgres"
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Create SQLAlchemy session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Define SQLAlchemy Base
Base = declarative_base()

# Define Prediction model
class Prediction(Base):
    __tablename__ = "prediction"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    prediction_value = Column(Float)
    features_used = Column(String)

# Function to make predictions and save to database
def make_predictions_and_save(input_data: dict):

    predictions = make_predictions(input_data)

    # Extract prediction value
    prediction_value = predictions["Here are our predictions using Random Forest"]

    features_used = input_data.columns.tolist()

    # Save to database
    with SessionLocal() as db:
        prediction_record = Prediction(prediction_value=prediction_value, features_used=str(features_used))
        db.add(prediction_record)
        db.commit()
        db.refresh(prediction_record)

    return predictions

'''# API endpoint to make predictions and save to database
@app.post("/predict/")
async def predict(input_data: dict):
    df = pd.DataFrame(input_data)
    predictions = make_predictions_and_save(df)
    return predictions '''

# API endpoint to make predictions and save to database
# API endpoint to make predictions and save to database
@app.post("/predict/")
async def predict(input_data: dict):
    print("Received request body:", input_data)  # Print the received request body
    predictions = make_predictions_and_save(input_data)
    return predictions


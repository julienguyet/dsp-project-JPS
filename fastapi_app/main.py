import os
import sys
import json
import io
sys.path.append('../')
from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Body, Request
from fastapi.responses import JSONResponse
import joblib
import pandas as pd
from sqlalchemy import create_engine, Column, Integer,Float, Boolean, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel, Field
from sales_prediction.inference import make_predictions
from sales_prediction import FEATURES_TO_DROP, MODEL_BASE_PATH, TEST_FEATURES
from sales_prediction.preprocessing import cpi_difference, create_time_feature, test_data_encoder
from datetime import date, datetime, timezone
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware
from typing import Union
from fastapi import File , UploadFile
from pathlib import Path
from sqlalchemy import Column, Integer, Float, Boolean, String, BigInteger, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from pydantic import BaseModel
import time



app = FastAPI()

DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/postgres" #"postgresql://siva:siva@localhost/jsp"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class FeatureInput(Base):
    __tablename__ = "feature"
    id = Column(Integer, primary_key=True, index=True)
    Store = Column(Float)
    Dept = Column(Float)
    Date = Column(String)
    Temperature = Column(Float)
    Fuel_Price = Column(Float)
    MarkDown1 = Column(Float)
    MarkDown2 = Column(Float)
    MarkDown3 = Column(Float)
    MarkDown4 = Column(Float)
    MarkDown5 = Column(Float)
    CPI = Column(Float)
    Unemployment = Column(Float)
    IsHoliday = Column(Boolean)
    Type = Column(String)
    Size = Column(Float)
    Sales = Column(Float)
    pred_date = Column(DateTime, default=datetime.utcnow)


class FeatureInputRequest(BaseModel):
    Store: float
    Dept: float
    Date: str
    Temperature: float
    Fuel_Price: float
    MarkDown1: float
    MarkDown2: float
    MarkDown3: float
    MarkDown4: float
    MarkDown5: float
    CPI: float
    Unemployment: float
    IsHoliday: bool
    Type: str
    Size: float

@app.post("/predictval/")
async def predict_features(file: UploadFile = File(None)):
    db = SessionLocal()
    try:
        if file:
            content = await file.read()
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
            predictions = []

            for _, row in df.iterrows():
                input_data = row.to_dict()
                predictions.append(make_predictions(pd.DataFrame(input_data, index=[0]))['Sales'][0])

            for i, pred in enumerate(predictions):
                feature_input = FeatureInput(**df.iloc[i].to_dict())
                feature_input.Sales = pred
                db.add(feature_input)

            db.commit()
            return JSONResponse(content={"sales": predictions})
    
    except Exception as e:
        db.rollback()
        print(f"Error: {str(e)}") 
        raise HTTPException(status_code=500, detail=f"Internal Server Error {str(e)}")
    finally:
        db.close()

@app.post("/predict/")
async def predict_features(filepaths: list = Body(...)):
    db = SessionLocal()
    try:
        predictions = []
        for filepath in filepaths:
            # Validate and handle potential path issues
            file_path = Path(filepath)
            print(file_path)
            if not file_path.is_file():
                print(f"Error: File not found - {filepath}")
                continue  # Skip to next filepath

            # Read the file content from the path
            with open(file_path, 'r') as f:
                df = pd.read_csv(f)
                for _, row in df.iterrows():
                    input_data = row.to_dict()
                    predictions.append(make_predictions(pd.DataFrame(input_data, index=[0]))['Sales'][0])

            # Process predictions and database operations here
            for i, pred in enumerate(predictions):
                feature_input = FeatureInput(**df.iloc[i].to_dict())
                feature_input.Sales = pred
                db.add(feature_input)
            #time.sleep(5)

        db.commit()
        return JSONResponse(content={"sales": predictions})
    
    except Exception as e:
        db.rollback()
        print(f"Error: {str(e)}") 
        raise HTTPException(status_code=500, detail=f"Internal Server Error {str(e)}")
    finally:
        db.close()


@app.get("/past_prediction/")
def past_prediction(start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
                    end_date: date = Query(..., description="End date (YYYY-MM-DD)")) -> List[dict]:
    try:
        print(f"Received request for past predictions between {start_date} and {end_date}")
        
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="End date must be after start date.")
        
        db = SessionLocal()
        predictions = db.query(FeatureInput).filter(FeatureInput.pred_date.between(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))).all()
        
        if predictions:
            serialized_predictions = []
            for prediction in predictions:
                serialized_prediction = {
                    "id": prediction.id,
                    "Store": prediction.Store,
                    "Dept": prediction.Dept,
                    "Date": prediction.Date,
                    "Temperature": prediction.Temperature,
                    "Fuel_Price": prediction.Fuel_Price,
                    "MarkDown1": prediction.MarkDown1,
                    "MarkDown2": prediction.MarkDown2,
                    "MarkDown3": prediction.MarkDown3,
                    "MarkDown4": prediction.MarkDown4,
                    "MarkDown5": prediction.MarkDown5,
                    "CPI": prediction.CPI,
                    "Unemployment": prediction.Unemployment,
                    "IsHoliday": prediction.IsHoliday,
                    "Type": prediction.Type,
                    "Size": prediction.Size,
                    "Sales": prediction.Sales,
                    "pred_date": str(prediction.pred_date)
                }
                serialized_predictions.append(serialized_prediction)
                
            return serialized_predictions
        else:
            print("No predictions found for the selected date range.")
            raise HTTPException(status_code=404, detail="No predictions found for the selected date range.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error {str(e)}")
    finally:
        db.close()

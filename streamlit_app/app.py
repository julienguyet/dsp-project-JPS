import os
import sys
import json
sys.path.append('../')
from fastapi import FastAPI, HTTPException, Query
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
from datetime import datetime
from typing import List, Optional
from datetime import date



app = FastAPI()

DATABASE_URL = "postgresql://siva:siva@localhost/jsp"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class FeatureInput(Base):
    __tablename__ = "features"
    id = Column(Integer, primary_key=True, index=True)
    Store = Column(Integer)
    Dept = Column(Integer)
    Date = Column(String)
    Weekly_Sales = Column(Float)
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
    Size = Column(Integer)
    Sales = Column(Float)
    pred_date = Column(DateTime, default=datetime.utcnow)


class FeatureInputRequest(BaseModel):
    Store: int
    Dept: int
    Date: str
    Weekly_Sales: float
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
    Size: int


# API endpoint to receive input features, store them in the database, make predictions, and return the result
@app.post("/predictval/")
async def predict_features(features: FeatureInputRequest):
    db = SessionLocal()
    try:
        # Store input features in the database
        feature_input = FeatureInput(**features.dict())
        db.add(feature_input)
        db.commit()
        db.refresh(feature_input)



        input_data = pd.DataFrame(features.dict(), index=[0])
        predictions = make_predictions(input_data)

        feature_input.Sales = predictions['Sales'][0]
        db.add(feature_input)
        db.commit()
        db.refresh(feature_input)

        return JSONResponse(content={"sales": float(predictions['Sales'][0])})
    
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail= f"Internal Server Error {str(e)}")
    finally:
        db.close()


@app.get("/past_prediction/")
def past_prediction(start_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
                    end_date: date = Query(..., description="End date (YYYY-MM-DD)")) -> List[str]:
    try:
        print(f"Received request for past predictions between {start_date} and {end_date}")
        
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="End date must be after start date.")
        
        db = SessionLocal()
        predictions = db.query(FeatureInput).filter(FeatureInput.pred_date.between(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))).all()
        
        if predictions:
            prediction_dates = [str(prediction.pred_date) for prediction in predictions]
            print(f"Predictions found: {prediction_dates}")
            return prediction_dates
        else:
            print("No predictions found for the selected date range.")
            raise HTTPException(status_code=404, detail="No predictions found for the selected date range.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error {str(e)}")
    finally:
        db.close()


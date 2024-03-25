from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import List
from sales_prediction.inference import *
import sys
sys.path.append('../../house_prices/')

# Initialize FastAPI app
app = FastAPI()

# Create SQLAlchemy engine
SQLALCHEMY_DATABASE_URL = "postgresql://postgres:Lufiva@localhost:5432/predictions"
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# Create SQLAlchemy session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Define SQLAlchemy Base
Base = declarative_base()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Define Response Model for Past Predictions
class PastPredictionResponse(BaseModel):
    prediction_value: int
    features_used: List[str]
    prediction_time: datetime

# API endpoint to retrieve past predictions along with used features
@app.get("/past-predictions/", response_model=List[PastPredictionResponse])
async def get_past_predictions(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    past_predictions = db.query(Prediction).order_by(Prediction.timestamp.desc()).offset(skip).limit(limit).all()

    if not past_predictions:
        raise HTTPException(status_code=404, detail="No past predictions found")

    return past_predictions

import streamlit as st
import requests
import pandas as pd
from datetime import datetime

API_BASE_URL = "http://localhost:8000"  

default_values = {
    "Store": 0,
    "Dept": 0,
    "Date": "2024-01-01",
    "Weekly_Sales": 0.0,
    "Temperature": 0.0,
    "Fuel_Price": 0.0,
    "MarkDown1": 0.0,
    "MarkDown2": 0.0,
    "MarkDown3": 0.0,
    "MarkDown4": 0.0,
    "MarkDown5": 0.0,
    "CPI": 0.0,
    "Unemployment": 0.0,
    "IsHoliday": False,
    "Type": "",
    "Size": 0
}

def predict_sales(data):
    endpoint = f"{API_BASE_URL}/predictval/"
    response = requests.post(endpoint, json=data)
    if response.status_code == 200:
        return response.json()["sales"]
    else:
        st.error("Failed to make predictions. Please check your input data.")


def get_past_predictions(start_date, end_date):
    endpoint = f"{API_BASE_URL}/past_prediction/"
    params = {"start_date": start_date, "end_date": end_date}
    response = requests.get(endpoint, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def main():
    st.title("Sales Prediction App")

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Make Prediction", "Past Predictions"])

    if page == "Make Prediction":
        form_values = {}
        for key, value in default_values.items():
            form_values[key] = st.text_input(key, value)

        if st.button("Predict"):
            response = predict_sales(form_values)
            if response is not None:
                st.success(f"The predicted sales is: ${response:.2f}")
            else:
                st.error("Failed to get prediction. Please try again.")

    elif page == "Past Predictions":
        st.subheader("Past Predictions !")
        start_date = st.date_input('Start Date')
        end_date = st.date_input('End Date')
        submit_button = st.button("Get Data")

        if submit_button:
            if start_date and end_date:
                if start_date <= end_date:
                    start_date_str = start_date.strftime('%Y-%m-%d')
                    end_date_str = end_date.strftime('%Y-%m-%d')              
                    data = get_past_predictions(start_date_str, end_date_str)
                    if data:
                        st.write('Predictions between selected dates:')
                        st.write(data)
                    else:
                        st.write('No predictions found for the selected date range.')
                else:
                    st.error('Error: End date must fall after start date.')


if __name__ == "__main__":
    main()

import io
import streamlit as st
import requests
import pandas as pd
from datetime import datetime

API_BASE_URL = "http://localhost:8000"  

default_values = {
    "Store": 0,
    "Dept": 0,
    "Date": "2024-01-01",
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
def fill_empty_cells(data):
    for key, value in default_values.items():
        if pd.isnull(data[key]):
            data[key] = value
    return data

def predict_sales(data):
    endpoint = f"{API_BASE_URL}/predictval/"
    if isinstance(data, pd.DataFrame): 

        csv_string = data.to_csv(index=False)
        csv_bytes = csv_string.encode()
        csv_file = io.BytesIO(csv_bytes)
        files = {'file': ("data.csv", csv_file)}
        response = requests.post(endpoint, files=files)
        
    if response.status_code == 200:
        try:
            return response.json()["sales"]
        except Exception as e:
            print(f"Error parsing response JSON: {e}")
            return None
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

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
    nav_option = st.sidebar.radio("Go to", ["Make Prediction", "Past Predictions"])

    if nav_option == "Make Prediction":
        make_prediction()

    elif nav_option == "Past Predictions":
        past_predictions()

def make_prediction():
    st.subheader("Input Data for Prediction")
    input_option = st.radio("Select Input Method:", ("Fill the form", "Upload CSV"))

    if input_option == "Fill the form":
        form_values = {}
        for key, value in default_values.items():
            form_values[key] = st.text_input(key, value)

        if st.button("Predict"):
            with st.spinner('Predicting...'):
                df = pd.DataFrame([form_values])
                response = predict_sales(df)
                if response is not None:
                    for idx, pred in enumerate(response):
                        st.success(f"The predicted sales is: {pred:.2f}")
                else:
                    st.error("Failed to get prediction. Please try again.")

    elif input_option == "Upload CSV":
        st.subheader("Upload CSV for Multiple Predictions")
        uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

        if uploaded_file is not None:
            st.write("File Uploaded Successfully!")
            uploaded_df = pd.read_csv(uploaded_file)
            if st.button("Predict"):
                with st.spinner('Predicting...'):
                    uploaded_df = uploaded_df.apply(fill_empty_cells, axis=1) 
                    predictions = predict_sales(uploaded_df)
                    if predictions:
                        uploaded_df['Predicted_Sales'] = predictions
                        st.write(uploaded_df)
                    else:
                        st.error("Failed to get predictions. Please try again.")

def past_predictions():
    st.subheader("Past Predictions")
    start_date = st.date_input('Start Date')
    end_date = st.date_input('End Date')
    submit_button = st.button("Get Data")

    if submit_button:
        with st.spinner('Getting past predictions...'):
            if start_date and end_date:
                if start_date <= end_date:
                    start_date_str = start_date.strftime('%Y-%m-%d')
                    end_date_str = end_date.strftime('%Y-%m-%d')              
                    data = get_past_predictions(start_date_str, end_date_str)
                    if data:
                        st.write('Predictions between selected dates:')
                        df = pd.DataFrame(data)
                        st.write(df)
                    else:
                        st.write('No predictions found for the selected date range.')
                else:
                    st.error('Error: End date must fall after start date.')

if __name__ == "__main__":
    main()

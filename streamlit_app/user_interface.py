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



# Define API base URL
API_BASE_URL = "http://localhost:8000"  # Replace with your API URL

# Function to make API request to predict sales
def predict_sales(data):
    endpoint = f"{API_BASE_URL}/predict/"
    response = requests.post(endpoint, json=data)
    if response.status_code == 200:
        return response.json()["prediction"]
    else:
        st.error("Failed to make predictions. Please check your input data.")

# Function to get past predictions
def get_past_predictions():
    endpoint = f"{API_BASE_URL}/past-predictions/"
    response = requests.get(endpoint)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to retrieve past predictions.")

# Main Streamlit app
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
        st.subheader("Past Predictions")
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

        st.subheader("Make Prediction")
        with st.form("prediction_form"):
            department = st.number_input('Enter Department', min_value=1)
            date = st.date_input('Enter Date')
            weekly_sales = st.number_input('Enter Weekly Sales', value=0.0)
            is_holiday = st.checkbox("Is Holiday? (Check only if it's a yes)")

            predict_button = st.form_submit_button(label='Predict')

        # Predict sales when predict button is clicked
        if predict_button:
            input_data = {
                "Dept": department,
                "Date": date.strftime("%Y-%m-%d"),
                "Weekly_Sales": weekly_sales,
                "IsHoliday": is_holiday
            }
            prediction = predict_sales(input_data)

            # Display prediction result
            st.subheader("Prediction Result")
            if prediction is not None:
                st.success(f"Prediction: {prediction}")
            else:
                st.error("Failed to retrieve prediction. Please try again.")

    elif page == "Past Predictions":
        st.subheader("Past Predictions")
        past_predictions = get_past_predictions()
        if past_predictions:
            df = pd.DataFrame(past_predictions)
            st.dataframe(df)
        else:
            st.info("No past predictions found.")

if __name__ == "__main__":
    main()

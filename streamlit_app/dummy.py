import streamlit as st
import requests
import pandas as pd

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

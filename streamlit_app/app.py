import streamlit as st
import requests

# Define the API endpoint
API_URL = "http://localhost:8000/predictval/"

# Define the default values for the form
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

def predict_sale_price(data):
    response = requests.post(API_URL, json=data)
    if response.status_code == 200:
        result = response.json()
        return result.get("sales")
    else:
        return None

def main():
    st.title("Walmart sales prediction")

    st.write("Fill in the details below to predict the sale price of a house.")

    # Create a form for user input
    form_values = {}
    for key, value in default_values.items():
            form_values[key] = st.text_input(key, value)

    if st.button("Predict"):
        response = predict_sale_price(form_values)
        if response is not None:
            st.success(f"The predicted sales is: ${response:.2f}")
        else:
            st.error("Failed to get prediction. Please try again.")

if __name__ == "__main__":
    main()

import numpy as np
import joblib
import streamlit as st

@st.cache
def make_prediction(open_price, high, low):
 
    # load the model and make prediction
    model = joblib.load("stock_model.pkl")
    open_price = float(open_price)
    high = float(high)
    low = float(low)
    x_test = [open_price, high, low]
    # make prection
    result = model.predict([x_test])
 
    return result

# Set the app title
st.title("Stock Market Prediction App")
st.write(
    "A simple machine laerning app to predict the stock market closing price"
)

# Declare a form to receive a movie's review
form = st.form(key="my_form")
open_price = form.text_input(label="Enter the open price")
high_price = form.text_input(label="Enter the high price")
low_price = form.text_input(label="Enter the low price")
submit = form.form_submit_button(label="Make Prediction")

if submit:
    # make prediction from the input text
    result = make_prediction(open_price, high_price, low_price)
 
    # Display results of the NLP task
    st.header("Results")

    st.write("The Prediction for market close is ", result)
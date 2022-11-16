# pip install streamlit
import streamlit as st

val = st.slider("val")
st.write(val, "squared is", val * val)
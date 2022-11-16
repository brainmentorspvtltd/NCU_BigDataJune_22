import streamlit as st
import bs4
import urllib.request as url

@st.cache(suppress_st_warning=True)
def search_product(product_name):
    product_name = product_name.replace(" ", "+")
    path = f"https://www.flipkart.com/search?q={product_name}"
    response = url.urlopen(path)
    print("Response...",response)
    html = bs4.BeautifulSoup(response, "lxml")

    titles = html.find_all('div', {'class' : '_4rR01T'})
    priceList = html.find_all("div", {'class' : '_30jeq3 _1_WHN1'})
    st.header("Results")
    for i in range(len(titles)):
        st.write("Title : " + str(titles[i].text))
        st.write("Price : " + str(priceList[i].text))
        # print("Title :",item.text)
    return "success"

# Set the app title
st.title("Product Search App...")
st.write(
    "A simple web scraping application that search products"
)

# Declare a form to receive a movie's review
form = st.form(key="my_form")
product_name = form.text_input(label="Enter product name : ")
submit = form.form_submit_button(label="Start Searching")

if submit:
    search_product(product_name)

    # st.write("The output is ", result)
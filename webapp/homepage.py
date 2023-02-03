import streamlit as st
from PIL import Image

# image = Image.open('digi.png')


#
class Homepage:
    def __init__(self):
        self.module = 'Home'

    def home_page(self):
        home_cols = st.columns([2, 1])
        # home_cols[0].image(image)
        home_cols[0].markdown("## 🎶 Welcome to Optiml 🎶")


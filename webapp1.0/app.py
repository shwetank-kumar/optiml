import sys, pathlib

dir_path = str(pathlib.Path.cwd().parent)
sys.path.append(dir_path)
import streamlit as st
from streamlit_option_menu import option_menu
from config import *
from utils import load_data, setup_snowflake, set_params
from homepage import Homepage
from dashboard import show_dashboard, query_dashboard
import streamlit_authenticator as sauth
st.set_page_config(
    page_title="OptiML",
    page_icon="〰️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

authenticator = sauth.Authenticate(names, usernames, passwords, "optiml", "optimlabccd", cookie_expiry_days=30)
if 'connection' not in st.session_state:
    setup_snowflake()
    print(f"Snowflake connection done")

with st.sidebar:
    name, authentication_status, username = authenticator.login("Login", "sidebar")
    print(name, authentication_status, username)

    # st.write(f"logged in schema {st.session_state.Schema}")
    if authentication_status == False:
        st.error("Username/password is incorrect")
        selected = 'Home'
    #
    if authentication_status == None:
        st.warning("Please enter your username and password")
        selected = 'Home'

    if authentication_status:
        # ---- Actions after user authentication ----
        print(f"User {username} Authenticated")
        st.session_state['username'] = username

        print(f"User {username} Authenticated")
        set_params(param=user_details[username], update=True)

        st.title(f"Hey {name.title()}\nChoose options below to navigate.")
        # st.title("Choose options below to navigate.")
        selected = option_menu(
            menu_title=None,
            options=['Home', 'Resource Usage', 'Query Profile', 'WH Profile', 'Storage Profile', 'User Profile',
                     'About Us'],
            icons=['house', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill',
                   'file-bar-graph-fill', 'droplet-fill', 'gear']
            , menu_icon="cast")

        authenticator.logout("Logout", "sidebar")

if authentication_status:
    load_data(username=username)

if selected == 'Home':
    myhome = Homepage()
    myhome.home_page()
elif selected == 'Resource Usage':
    print("Resource Usage Selected")
    show_dashboard(**st.session_state)
elif selected == "Query Profile":
    query_dashboard(**st.session_state)

import pathlib, sys

dir_path = str(pathlib.Path.cwd().parent)
sys.path.append(dir_path)
from config import set_params, authenticate_user, change_state

set_params()
import pandas as pd
from homepage import Homepage
from dashboard import show_dashboard
import streamlit as st
import os
from streamlit_option_menu import option_menu
from optiml.backend.cost_profile import CostProfile, get_previous_dates
from optiml.connection import SnowflakeConnConfig

connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp', warehousename="XSMALL_WH").create_connection()
cqlib = CostProfile(connection, st.session_state.Schema)

st.set_page_config(
    page_title="Snowflake",
    page_icon="〰️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

session_argument = {}

if 'total_cost_df' not in st.session_state:
    print("dates:  ", st.session_state.sdate, st.session_state.edate)
    st.session_state.total_cost_df = 0
    st.session_state.total_cost_df = cqlib.total_cost_breakdown_ts(st.session_state.sdate, st.session_state.edate)

if 'cost_by_user_df' not in st.session_state:
    st.session_state.cost_by_user_df = 0
    st.session_state.cost_by_user_df = cqlib.cost_by_user_ts(st.session_state.sdate, st.session_state.edate)

if 'cost_by_wh_df' not in st.session_state:
    st.session_state.cost_by_wh_df = 0
    st.session_state.cost_by_wh_df = cqlib.cost_by_wh_ts(st.session_state.sdate, st.session_state.edate)

if 'cost_by_partner_tools_df' not in st.session_state:
    st.session_state.cost_by_partner_tools_df = 0
    st.session_state.cost_by_partner_tools_df = cqlib.cost_by_partner_tool_ts(st.session_state.sdate,
                                                                              st.session_state.edate)
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False


def signin_form():
    with st.form(key='login'):
        st.markdown("### Login with your details ")
        user_name = st.text_input(label="Enter Email", placeholder="Enter your user name")
        password = st.text_input(label="Enter password", placeholder="Enter password", type="password")
        if st.form_submit_button("Sign In 🔑"):
            if authenticate_user(user_name, password):
                st.session_state['logged_in'] = True
            print(user_name, password, st.session_state['logged_in'])


# with st.sidebar:
#     st.header("Hello Admin. Welcome to OPTIM")
#     signin_form()
#     if 'logged_in' in st.session_state and st.session_state.logged_in == True:
#         selected = option_menu(
#             menu_title=None,
#             options=['Home', 'Resource Usage', 'Query Profile', 'WH Profile', 'Storage Profile', 'User Profile',
#                      'About Us'],
#             icons=['house', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill',
#                    'file-bar-graph-fill', 'droplet-fill', 'gear']
#             , menu_icon="cast")
#     else:
#         selected = "Home"


with st.sidebar:
    if not st.session_state['logged_in']:
        st.header("Welcome to OPTIM. \nLogin to continue")
        signin_form()
        selected = "Home"
    elif st.session_state['logged_in']:
        st.title("Hey Admin\nChoose options below to navigate.")
        # st.title("Choose options below to navigate.")
        selected = option_menu(
            menu_title=None,
            options=['Home', 'Resource Usage', 'Query Profile', 'WH Profile', 'Storage Profile', 'User Profile',
                     'About Us'],
            icons=['house', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill',
                   'file-bar-graph-fill', 'droplet-fill', 'gear']
            , menu_icon="cast")
        st.button(label="Logout", on_click=change_state, args=('logged_in', False))

if selected == 'Home':
    myhome = Homepage()
    myhome.home_page()
elif (selected == 'Resource Usage') and (st.session_state.logged_in == True):
    print("Resource Usage Selected")
    show_dashboard(**st.session_state)

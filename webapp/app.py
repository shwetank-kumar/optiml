import pathlib, sys

dir_path = str(pathlib.Path.cwd().parent)
sys.path.append(dir_path)
from config import set_params, authenticate_user, change_state, authenticator

set_params()
import pandas as pd
from homepage import Homepage
from dashboard import show_dashboard, query_dashboard
import streamlit as st
import os
from streamlit_option_menu import option_menu
from optiml.backend.query_profile import QueryProfile
from optiml.backend.cost_profile import CostProfile, get_previous_dates
from optiml.connection import SnowflakeConnConfig

connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp', warehousename="XSMALL_WH").create_connection()
cqlib = CostProfile(connection, st.session_state.Schema)
qqlib = QueryProfile(connection, st.session_state.Schema)
# df.head()
st.set_page_config(
    page_title="Snowflake",
    page_icon="〰️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

if 'total_cost_df' not in st.session_state:
    print("dates:  ", st.session_state.sdate, st.session_state.edate)
    st.session_state.total_cost_df = 0
    st.session_state.total_cost_df = cqlib.total_cost_breakdown_ts(st.session_state.sdate, st.session_state.edate)

if 'cost_by_user_df' not in st.session_state:
    st.session_state.cost_by_user_df = 0
    st.session_state.cost_by_user_df = cqlib.cost_by_user_ts(st.session_state.sdate,
                                                             st.session_state.edate)

if 'cost_by_wh_df' not in st.session_state:
    st.session_state.cost_by_wh_df = 0
    st.session_state.cost_by_wh_df = cqlib.cost_by_wh_ts(st.session_state.sdate,
                                                         st.session_state.edate)

if 'cost_by_partner_tools_df' not in st.session_state:
    st.session_state.cost_by_partner_tools_df = 0
    st.session_state.cost_by_partner_tools_df = cqlib.cost_by_partner_tool_ts(st.session_state.sdate,
                                                                              st.session_state.edate)
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

if "query_execution_status" not in st.session_state:
    st.session_state.query_execution_status = qqlib.queries_stats_by_execution_status(st.session_state.sdate,
                                                                                      st.session_state.edate)
if "full_table_scans" not in st.session_state:
    st.session_state.full_table_scans = qqlib.queries_full_table_scan(st.session_state.sdate,
                                                                      st.session_state.edate)
if "unique_query_by_type" not in st.session_state:
    st.session_state.unique_query_by_type = qqlib.unique_queries_by_type(st.session_state.sdate,
                                                                         st.session_state.edate)
# df.head()

# --- USER AUTHENTICATION ---


with st.sidebar:
    name, authentication_status, username = authenticator.login("Login", "sidebar")
    print(name, authentication_status, username)

    if authentication_status == False:
        st.error("Username/password is incorrect")
        selected = 'Home'
    #
    if authentication_status == None:
        st.warning("Please enter your username and password")
        selected = 'Home'

    if authentication_status:
        # ---- READ EXCEL ----
        print("Authenticated-----------------")
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

if selected == 'Home':
    myhome = Homepage()
    myhome.home_page()
elif selected == 'Resource Usage':
    print("Resource Usage Selected")
    show_dashboard(**st.session_state)
elif selected == "Query Profile":
    query_dashboard(**st.session_state)

from homepage import Homepage
from dashboard import show_dashboard
import streamlit as st
import os
from streamlit_option_menu import option_menu

from backend.cost_profile import CostProfile, get_previous_dates
from connection import SnowflakeConnConfig

connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp', warehousename="XSMALL_WH").create_connection()
cache_dir = os.path.expanduser('~/data/knot')
cqlib = CostProfile(connection, 'KNT', cache_dir, "enterprise")

print("Hello")
sdate = '2022-10-11'
edate = '2022-10-21'

st.set_page_config(
    page_title="Snowflake",
    page_icon="〰️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

df = cqlib.total_cost_breakdown_ts(sdate, edate)
# session_argument = st.session_state
session_argument = {}

if 'total_cost_df' not in st.session_state:
    st.session_state.total_cost_df = 0
    st.session_state.total_cost_df = cqlib.total_cost_breakdown_ts(sdate, edate)

if 'cost_by_user_df' not in st.session_state:
    st.session_state.cost_by_user_df = 0
    st.session_state.cost_by_user_df = cqlib.cost_by_user_ts(sdate, edate)

with st.sidebar:
    st.header("Welcome to OPTIM")
    selected = option_menu(
        menu_title=None,
        options=['Home', 'Resource Usage', 'Query Profile', 'WH Profile', 'Storage Profile', 'User Profile',
                 'About Us'],
        icons=['house', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill', 'file-bar-graph-fill',
               'file-bar-graph-fill', 'droplet-fill', 'gear']
        , menu_icon="cast")

if selected == 'Home':
    myhome = Homepage()
    myhome.home_page()
elif selected == 'Resource Usage':
    print("Resource Usage Selected")
    show_dashboard(**st.session_state)

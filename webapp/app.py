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

sdate = '2022-10-11'
edate = '2022-10-21'


if 'raw_df' not in st.session_state:
    st.session_state['raw_df']  = cqlib.total_cost_breakdown_ts(sdate, edate)


with st.sidebar:
    st.header("Welcome to OPTIM")
    selected = option_menu(
        menu_title=None,
        options=['Home', 'Dashboard', 'About Us'],
        icons=['house', 'file-bar-graph-fill', 'droplet-fill', 'gear']
        , menu_icon="cast")


if selected == 'Home':
    myhome = Homepage()
    myhome.home_page()
elif selected == 'Dashboard':
    show_dashboard("")
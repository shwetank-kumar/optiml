import streamlit as st
import datetime
from datetime import timedelta
from optiml.backend.cost_profile import CostProfile, get_previous_dates
from optiml.backend.query_profile import QueryProfile
from optiml.connection import SnowflakeConnConfig


# @st.cache
def load_data():
    if 'total_cost_df' not in st.session_state:
        print("dates:  ", st.session_state.sdate, st.session_state.edate)
        st.session_state.total_cost_df = 0
        st.session_state.total_cost_df = st.session_state.cqlib.total_cost_breakdown_ts(st.session_state.sdate,
                                                                                        st.session_state.edate)

    if 'cost_by_user_df' not in st.session_state:
        st.session_state.cost_by_user_df = 0
        st.session_state.cost_by_user_df = st.session_state.cqlib.cost_by_user_ts(st.session_state.sdate,
                                                                                  st.session_state.edate)

    if 'cost_by_wh_df' not in st.session_state:
        st.session_state.cost_by_wh_df = 0
        st.session_state.cost_by_wh_df = st.session_state.cqlib.cost_by_wh_ts(st.session_state.sdate,
                                                                              st.session_state.edate)

    if 'cost_by_partner_tools_df' not in st.session_state:
        st.session_state.cost_by_partner_tools_df = 0
        st.session_state.cost_by_partner_tools_df = st.session_state.cqlib.cost_by_partner_tool_ts(
            st.session_state.sdate,
            st.session_state.edate)
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False

    if "query_execution_status" not in st.session_state:
        st.session_state.query_execution_status = st.session_state.qqlib.queries_stats_by_execution_status(
            st.session_state.sdate,
            st.session_state.edate)
    if "full_table_scans" not in st.session_state:
        st.session_state.full_table_scans = st.session_state.qqlib.queries_full_table_scan(st.session_state.sdate,
                                                                                           st.session_state.edate)
    if "unique_query_by_type" not in st.session_state:
        st.session_state.unique_query_by_type = st.session_state.qqlib.unique_queries_by_type(st.session_state.sdate,
                                                                                              st.session_state.edate)


params = dict(
    Schema="KIV.ACCOUNT_USAGE",
    sdate=datetime.datetime.strptime('2022-10-05', "%Y-%m-%d").date(),
    edate=datetime.datetime.strptime('2022-10-12', "%Y-%m-%d").date(),
    TRAINING_LENGTH=30,
)


# @st.cache
def set_params(update=False, *args, **kwargs):
    if update and kwargs:
        params.update(kwargs)
    for k, v in params.items():
        print(k, v)
        st.session_state[k] = v
    st.session_state.p1_sdate, st.session_state.p1_edate = get_previous_dates(st.session_state.sdate,
                                                                              st.session_state.edate, 1)
    st.session_state['training_start'] = st.session_state["sdate"] - timedelta(st.session_state['TRAINING_LENGTH'] + 1)
    st.session_state['training_end'] = st.session_state["sdate"] - timedelta(1)
    st.session_state['Schema'] = "KNT.ACCOUNT_USAGE" if st.session_state['username'] in [
        "admin2"] else "KIV.ACCOUNT_USAGE"
    st.session_state['cqlib'] = CostProfile(st.session_state.connection, st.session_state.Schema)
    st.session_state['qqlib'] = QueryProfile(st.session_state.connection, st.session_state.Schema)


def setup_snowflake(*args, **kwargs):
    connection = SnowflakeConnConfig(accountname='jg84276.us-central1.gcp',
                                     warehousename="XSMALL_WH").create_connection()
    st.session_state['connection'] = connection

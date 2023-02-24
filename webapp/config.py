import streamlit as st
from optiml.backend.cost_profile import get_previous_dates

params = dict(
    Schema="KIV.ACCOUNT_USAGE",
    sdate='2022-10-05',
    edate='2022-10-12'
)


def set_params():
    for k, v in params.items():
        print(k, v)
        st.session_state[k] = v
    st.session_state.p1_sdate, st.session_state.p1_edate = get_previous_dates(st.session_state.sdate,
                                                                              st.session_state.edate, 1)
